package runtime

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/context"

	pcolh "github.com/willtrking/go-proto-collections/helpers"
	pgcol "github.com/willtrking/go-proto-collections/protocollections"
)

type linkingWaitLock struct {
	firstUnlockOnce  sync.Once
	secondUnlockOnce sync.Once
	firstWait        sync.Mutex
	secondWait       sync.Mutex
	hadErrors        bool
}

// Link collections together by validating that the parentKey and the collectionKey are either equal,
// or can be set to be equal. Short circuit on first problem
// Has side effects on elemData.
func linkCollections(elemData []interface{}, details *pgcol.CollectionDetails, colKey string, parentMessage pcolh.CollectionMessage) ([]string, map[string][]string) {
	linkingErrors := make(map[string][]string)
	var keysSet []string

	for idx, dat := range elemData {
		if !parentMessage.ProtoBelongsToCollection(dat, colKey) {
			//This key doesn't belong to the top level
			if parentMessage.CollectionParentKeyIsDefault(colKey) {
				//Parent message has default for this
				var errorKey string
				if details.Listable {
					errorKey = colKey + "[" + strconv.Itoa(idx) + "]"
				} else {
					errorKey = colKey
				}

				linkingErrors[errorKey] = []string{
					fmt.Sprintf("Mismatch linking key '%s' value with parent linking key '%s' value", details.CollectionKey, details.ParentKey),
				}
				//Short circuit
				return nil, linkingErrors

			} else {
				//Parent doesn't have default
				//Check if proto has default key

				if parentMessage.CollectionKeyIsDefault(dat, colKey) {
					//Key is default, parent isnt
					//Set it to the parent key val
					//Returns the key that was set
					keySet := parentMessage.SetCollectionKeyDataFromParent(dat, colKey)
					keysSet = append(keysSet, keySet)

				} else {
					//It doesn't
					//Means theres a mismatch in the keys, so error out
					var errorKey string
					if details.Listable {
						errorKey = colKey + "[" + strconv.Itoa(idx) + "]"
					} else {
						errorKey = colKey
					}

					linkingErrors[errorKey] = []string{
						fmt.Sprintf("Mismatch linking key '%s' value with parent linking key '%s' value", details.CollectionKey, details.ParentKey),
					}
					//Short circuit
					return nil, linkingErrors
				}
			}
		}
	}

	return keysSet, nil
}

func writerErrorsToMap(err []WriterError, listable bool) map[string][]string {
	errMap := make(map[string][]string)

	for _, e := range err {
		errorKey := e.Attr
		if listable && e.Index != "" {
			errorKey = errorKey + "[" + e.Index + "]"
		}
		errMap[errorKey] = append(errMap[errorKey], e.Desc)
	}

	return errMap
}

func dataProcessor(ctx context.Context, writer CollectionWriter, parentWriter CollectionWriter,
	elem pcolh.CollectionElem, parentMessage pcolh.CollectionMessage,
	path string, parent string, colKey string,
	errChan chan map[string][]string,
	pathLinkingWait *linkingWaitLock, parentLinkingWait *linkingWaitLock,
	childLinkingLocks []*linkingWaitLock,
	wg sync.WaitGroup) {

	defer wg.Done()
	defer close(errChan)

	firstUnlocker := func() {
		//Unlock our current path, but only do it once
		pathLinkingWait.firstWait.Unlock()
	}

	secondUnlocker := func() {
		//Unlock our current path, but only do it once
		pathLinkingWait.secondWait.Unlock()
	}

	//Grab parent message (if it isn't top level)
	//Grab the key for this collection from the parent
	//Check if all the elements of this collection are OK with that key, meaning
	// - Is it a default value? If so assume it will be set on write
	// - If it isnt, make sure it lines up with the key for this collection

	details := elem.DefaultDetails()
	elemData := elem.InterfaceSlice()

	//Link collection data with parent, short circuit on first error.
	//See linkCollections definition for more details

	if parent != COL_DELIM {
		//If the parent ISN'T the top level, we need to wait for it to link
		parentLinkingWait.firstWait.Lock()
		parentLinkingWait.firstWait.Unlock()
	}

	if writer == nil {
		err := make(map[string][]string)
		err[colKey] = []string{
			"Missing writer",
		}

		errChan <- err
		pathLinkingWait.hadErrors = true
		pathLinkingWait.secondUnlockOnce.Do(secondUnlocker)
		pathLinkingWait.firstUnlockOnce.Do(firstUnlocker)

		return
	}

	if parent != COL_DELIM && parentWriter == nil {
		err := make(map[string][]string)
		err[colKey] = []string{
			"Missing parent writer",
		}

		errChan <- err
		pathLinkingWait.hadErrors = true
		pathLinkingWait.secondUnlockOnce.Do(secondUnlocker)
		pathLinkingWait.firstUnlockOnce.Do(firstUnlocker)

		return
	}

	linkedKeys, linkingErrors := linkCollections(elemData, details, colKey, parentMessage)
	if linkingErrors != nil {
		errChan <- linkingErrors
		pathLinkingWait.hadErrors = true
		pathLinkingWait.secondUnlockOnce.Do(secondUnlocker)
		pathLinkingWait.firstUnlockOnce.Do(firstUnlocker)

		return
	}

	writer.Read(elem.ProtoSlice(), parentMessage)

	linkedAllKeys := false
	for _, l := range linkedKeys {
		if l == details.CollectionKey {
			linkedAllKeys = true
			break
		}
	}

	pathLinkingWait.firstUnlockOnce.Do(firstUnlocker)

	if len(childLinkingLocks) > 0 {
		//Wait for children to link as well

		for _, l := range childLinkingLocks {
			//Wait for child to wait for it's children to link
			l.secondWait.Lock()
			l.secondWait.Unlock()
			if l.hadErrors {
				pathLinkingWait.hadErrors = true
				pathLinkingWait.secondUnlockOnce.Do(secondUnlocker)
				return
			}
		}

	}

	pathLinkingWait.hadErrors = false
	pathLinkingWait.secondUnlockOnce.Do(secondUnlocker)

	if parent != COL_DELIM {
		parentWriter.WaitValidate()

		if parentWriter.ValidateHadErrors() {
			writer.SetValidateHadErrors(true)
			writer.ValidateUnlockWait()
			return
		}
	}

	validateCtx, validateErrs := writer.Validate(ctx)

	if len(validateErrs) > 0 {
		//Defer this here just in case
		//Internally uses sync.Once to prevent double calls without another Validate call, so it's safe.
		defer writer.ValidateUnlockWait()

		if parent != COL_DELIM && !linkedAllKeys {
			//Check if any errs could be solved by linking a parent after write/preconditon
			//If it can, delay the rest until the parent write/precondition, then relink etc
			//This will cause all children to wait as well
			relink := false
			for _, v := range validateErrs {
				if v.Attr == details.CollectionKey {
					//Got an error on the key we could link
					relink = true
				}
			}

			if relink {

				//Wait for parent precondition
				parentWriter.WaitPrecondition()

				//If there were errors, just leave anyway
				if parentWriter.PreconditionHadErrors() {
					//We had errors, so this will propogate down
					return
				}

				//Relink

				linkedKeys, linkingErrors = linkCollections(elemData, details, colKey, parentMessage)
				if linkingErrors != nil {
					errChan <- linkingErrors
					return
				}

				//See if the linked all keys this time
				linkedAllKeys = false
				for _, l := range linkedKeys {
					if l == details.CollectionKey {
						linkedAllKeys = true
						break
					}
				}

				if linkedAllKeys {
					//If we did, the precondition validation allowed this
					//Try to revalidate here
					validateCtx, validateErrs = writer.Validate(ctx)

					if len(validateErrs) > 0 {
						//Still got a problem
						errChan <- writerErrorsToMap(validateErrs, details.Listable)
						return
					}

				} else {
					//We didn't so lets wait for write

					//Wait for parent write
					parentWriter.WaitWrite()

					//If there were errors, just leave anyway
					if parentWriter.WriteHadErrors() {
						//We had errors, so this will propogate down
						return
					}

					//Relink

					linkedKeys, linkingErrors = linkCollections(elemData, details, colKey, parentMessage)
					if linkingErrors != nil {
						errChan <- linkingErrors
						return
					}

					//See if the linked all keys this time
					linkedAllKeys = false
					for _, l := range linkedKeys {
						if l == details.CollectionKey {
							linkedAllKeys = true
							break
						}
					}

					if linkedAllKeys {
						//If we did, the write validation allowed this
						//Try to revalidate here

						validateCtx, validateErrs = writer.Validate(ctx)

						if len(validateErrs) > 0 {
							//Still got a problem
							errChan <- writerErrorsToMap(validateErrs, details.Listable)
							return
						}

					} else {
						//Otherwise send our validate errors out
						errChan <- writerErrorsToMap(validateErrs, details.Listable)
						return
					}

				}

			} else {
				//Can't relink, just send our validate errors out
				errChan <- writerErrorsToMap(validateErrs, details.Listable)
				return
			}
		} else {
			//Parent top level, or linked all keys
			errChan <- writerErrorsToMap(validateErrs, details.Listable)
			return
		}
	}

	//Good to go!
	writer.ValidateUnlockWait()

	if parent != COL_DELIM {
		parentWriter.WaitPrecondition()

		if parentWriter.PreconditionHadErrors() {
			writer.SetPreconditionHadErrors(true)
			writer.PreconditionUnlockWait()
			return
		}
	}

	preconCtx, preconErrs := writer.CheckPrecondition(validateCtx)

	if len(preconErrs) > 0 {
		//Defer this here just in case
		//Internally uses sync.Once to prevent double calls without another CheckPrecondition call, so it's safe.
		defer writer.PreconditionUnlockWait()

		if parent != COL_DELIM && !linkedAllKeys {
			//Check if any errs could be solved by linking a parent after write
			//If it can, delay the rest until the parent write, then relink etc
			//This will cause all children to wait as well
			relink := false
			for _, v := range preconErrs {
				if v.Attr == details.CollectionKey {
					//Got an error on the key we could link
					relink = true
				}
			}

			if relink {

				//We didn't so lets wait for write

				//Wait for parent write
				parentWriter.WaitWrite()

				//If there were errors, just leave anyway
				if parentWriter.WriteHadErrors() {
					//We had errors, so this will propogate down
					return
				}

				//Relink

				linkedKeys, linkingErrors = linkCollections(elemData, details, colKey, parentMessage)
				if linkingErrors != nil {
					errChan <- linkingErrors
					return
				}

				//See if the linked all keys this time
				linkedAllKeys = false
				for _, l := range linkedKeys {
					if l == details.CollectionKey {
						linkedAllKeys = true
						break
					}
				}

				if linkedAllKeys {
					//If we did, the write validation allowed this
					//Try to check preconditions here

					preconCtx, preconErrs = writer.CheckPrecondition(ctx)

					if len(preconErrs) > 0 {
						//Still got a problem
						errChan <- writerErrorsToMap(preconErrs, details.Listable)
						return
					}

				} else {
					//Otherwise send our validate errors out
					errChan <- writerErrorsToMap(preconErrs, details.Listable)
					return
				}

			} else {
				//Can't relink, just send our precon errors out
				errChan <- writerErrorsToMap(preconErrs, details.Listable)
				return
			}
		} else {
			//Can't relink, just send our precon errors out
			errChan <- writerErrorsToMap(preconErrs, details.Listable)
			return
		}
	}

	//Good to go!
	writer.PreconditionUnlockWait()

	if parent != COL_DELIM {
		parentWriter.WaitWrite()

		if parentWriter.WriteHadErrors() {
			writer.SetWriteHadErrors(true)
			writer.WriteUnlockWait()
			return
		}
	}

	defer writer.WriteUnlockWait()

	writeResult, writeErrs := writer.Write(preconCtx)

	if len(writeErrs) > 0 {
		errChan <- writerErrorsToMap(writeErrs, details.Listable)
		return
	}

	elem.LoadData(writeResult)

}

func buildTree(ctx context.Context, registry *CollectionRegistry, level []pcolh.CollectionMessage,
	elem pcolh.CollectionElem, parentMessage pcolh.CollectionMessage,
	path string, parent string, colKey string,
	writerMap map[string]CollectionWriter, errChan map[string]chan map[string][]string,
	linkingWait map[string]*linkingWaitLock, wg sync.WaitGroup) {

	if _, ok := linkingWait[path]; !ok {
		//Don't have lock
		linkingWait[path] = &linkingWaitLock{
			firstUnlockOnce:  sync.Once{},
			secondUnlockOnce: sync.Once{},
			firstWait:        sync.Mutex{},
			secondWait:       sync.Mutex{},
			hadErrors:        true,
		}
		linkingWait[path].firstWait.Lock()
		linkingWait[path].secondWait.Lock()
	}

	if elem != nil {
		if _, ok := writerMap[path]; !ok {
			writer, writeErr := registry.Writer(elem)

			if writer != nil && writeErr == nil {
				//Have a writer

				w, wErr := writer()

				if wErr != nil || w == nil {
					writerMap[path] = nil
				} else {
					writerMap[path] = w
				}

			}
		}
	}

	var childLinkingLocks []*linkingWaitLock

	for _, top := range level {
		keys := top.CollectionKeys()
		for _, key := range keys {
			e := top.CollectionElem(key)
			if e != nil {
				currentPath := path + COL_DELIM + key
				//tree[currentPath] = e
				errChan[currentPath] = make(chan map[string][]string, 1)

				if _, ok := linkingWait[currentPath]; !ok {
					//Don't have lock
					linkingWait[currentPath] = &linkingWaitLock{
						firstUnlockOnce:  sync.Once{},
						secondUnlockOnce: sync.Once{},
						firstWait:        sync.Mutex{},
						secondWait:       sync.Mutex{},
						hadErrors:        true,
					}
					linkingWait[currentPath].firstWait.Lock()
					linkingWait[currentPath].secondWait.Lock()
				}

				childLinkingLocks = append(childLinkingLocks, linkingWait[currentPath])

				if e.DataIsCollectionMessage() {
					buildTree(ctx, registry, e.CollectionMessageSlice(), e, top, currentPath, path, key, writerMap, errChan, linkingWait, wg)
				} else {
					buildLeaf(ctx, registry, e, top, currentPath, path, key, writerMap, errChan, linkingWait, wg)
				}

			}
		}
	}

	if colKey != "" {
		wg.Add(1)
		go dataProcessor(ctx, writerMap[path], writerMap[parent], elem, parentMessage, path, parent, colKey, errChan[path], linkingWait[path], linkingWait[parent], childLinkingLocks, wg)
	}

}

func buildLeaf(ctx context.Context, registry *CollectionRegistry, elem pcolh.CollectionElem,
	parentMessage pcolh.CollectionMessage,
	path string, parent string, colKey string,
	writerMap map[string]CollectionWriter,
	errChan map[string]chan map[string][]string,
	linkingWait map[string]*linkingWaitLock, wg sync.WaitGroup) {

	if _, ok := linkingWait[path]; !ok {
		//Don't have lock
		linkingWait[path] = &linkingWaitLock{
			firstUnlockOnce:  sync.Once{},
			secondUnlockOnce: sync.Once{},
			firstWait:        sync.Mutex{},
			secondWait:       sync.Mutex{},
			hadErrors:        true,
		}
		linkingWait[path].firstWait.Lock()
		linkingWait[path].secondWait.Lock()
	}

	if elem != nil {
		if _, ok := writerMap[path]; !ok {
			writer, writeErr := registry.Writer(elem)

			if writer != nil && writeErr == nil {
				//Have a writer

				w, wErr := writer()

				if wErr != nil || w == nil {
					writerMap[path] = nil
				} else {
					writerMap[path] = w
				}

			}
		}
	}

	wg.Add(1)
	go dataProcessor(ctx, writerMap[path], writerMap[parent], elem, parentMessage, path, parent, colKey, errChan[path], linkingWait[path], linkingWait[parent], []*linkingWaitLock{}, wg)

}

func cleanErrMap(path string, errMap map[string][]string) map[string]map[string][]string {
	path = strings.TrimSpace(path)
	path = strings.Trim(path, COL_DELIM)

	if path == "" {
		path = "."
	} else {
		path = "." + path
	}

	cleaned := make(map[string]map[string][]string)
	cleaned[path] = make(map[string][]string)

	splitPath := strings.Split(path, COL_DELIM)

	if len(splitPath) == 1 {

		for k, v := range errMap {
			nK := strings.TrimSpace(k)
			nK = strings.Trim(nK, COL_DELIM)

			cleaned[path][nK] = v
		}

		return cleaned
	} else {

		half := strings.Join(splitPath[:1], COL_DELIM)

		for k, v := range errMap {
			nK := strings.TrimSpace(k)
			nK = strings.Trim(nK, COL_DELIM)

			if half != "" {
				nK = half + "." + nK
			}

			cleaned[path][nK] = v
		}

		return cleaned
	}
}

func ReadWriteCollections(ctx context.Context, registry *CollectionRegistry, topLevel []pcolh.CollectionMessage) map[string]map[string][]string {

	writerMap := make(map[string]CollectionWriter)
	errChan := make(map[string]chan map[string][]string)
	linkingWait := make(map[string]*linkingWaitLock)
	var wg sync.WaitGroup

	buildTree(ctx, registry, topLevel, nil, nil, COL_DELIM, COL_DELIM, "", writerMap, errChan, linkingWait, wg)

	wg.Wait()

	for path, c := range errChan {
		// At this point all the processors are done,
		// so its safe to just consume from the channels without a select, etc.
		err := <-c
		if err != nil && len(err) > 0 {
			return cleanErrMap(path, err)
		}

	}

	return nil
}
