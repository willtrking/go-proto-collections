package runtime

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
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
func linkCollections(elemData []proto.Message, details *pgcol.CollectionDetails, colKey string, parentMessage pcolh.CollectionMessage) ([]string, map[string][]string) {
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

func linkLevel(levelData []*levelHolder, details *pgcol.CollectionDetails) ([]proto.Message, []proto.Message, bool, map[string][]string) {
	data := []proto.Message{}
	parents := []proto.Message{}
	linkedAllKeys := true

	for _, d := range levelData {

		linkedKeys, linkingErrors := linkCollections(d.levelData, details, d.colKey, d.parent)

		linkedAll := false
		for _, linked := range linkedKeys {
			if linked == details.CollectionKey {
				linkedAll = true
				break
			}
		}

		if !linkedAll {
			linkedAllKeys = false
		}

		if linkingErrors != nil {

			return nil, nil, false, linkingErrors
		}

		for _, l := range d.levelData {
			data = append(data, l)
			parents = append(parents, d.parent)
		}
	}

	return data, parents, linkedAllKeys, nil

}

func newDataProcessor(ctx context.Context, levelData []*levelHolder, details *pgcol.CollectionDetails,
	writer CollectionWriter, parentWriter CollectionWriter, errChan chan map[string][]string,
	pathLinkingWait *linkingWaitLock, parentLinkingWait *linkingWaitLock, wg sync.WaitGroup) {

	defer wg.Done()
	defer close(errChan)

	firstUnlocker := func() {
		//Unlock our current path, but only do it once
		pathLinkingWait.firstWait.Unlock()
	}

	if parentWriter != nil {
		parentLinkingWait.firstWait.Lock()
		parentLinkingWait.firstWait.Unlock()
	}

	colKey := ""

	for _, lH := range levelData {
		if colKey == "" {
			colKey = lH.colKey
		} else {
			if colKey != lH.colKey {
				panic("levelData mismatch colKey")
			}
		}
	}

	data, parents, linkedAllKeys, linkingErrors := linkLevel(levelData, details)

	if linkingErrors != nil {
		errChan <- linkingErrors
		return
	}

	pathLinkingWait.firstUnlockOnce.Do(firstUnlocker)

	writer.Read(data, parents)

	if parentWriter != nil {
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

		if parentWriter != nil && !linkedAllKeys {

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

				data, parents, linkedAllKeys, linkingErrors = linkLevel(levelData, details)

				if linkingErrors != nil {
					errChan <- linkingErrors
					return
				}

				if linkedAllKeys {
					//If we did, the precondition validation allowed this
					//Try to revalidate here

					writer.Read(data, parents)

					validateCtx, validateErrs = writer.Validate(ctx)

					if len(validateErrs) > 0 {
						//Still got a problem
						errChan <- WriterErrorsToMap(validateErrs, details.Listable)
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

					data, parents, linkedAllKeys, linkingErrors = linkLevel(levelData, details)

					if linkingErrors != nil {
						errChan <- linkingErrors
						return
					}

					if linkedAllKeys {
						//If we did, the write validation allowed this
						//Try to revalidate here
						writer.Read(data, parents)

						validateCtx, validateErrs = writer.Validate(ctx)

						if len(validateErrs) > 0 {
							//Still got a problem
							errChan <- WriterErrorsToMap(validateErrs, details.Listable)
							return
						}

					} else {
						//Otherwise send our validate errors out
						errChan <- WriterErrorsToMap(validateErrs, details.Listable)
						return
					}

				}

			} else {
				//Can't relink, just send our validate errors out
				errChan <- WriterErrorsToMap(validateErrs, details.Listable)
				return
			}
		} else {
			//Parent top level, or linked all keys
			errChan <- WriterErrorsToMap(validateErrs, details.Listable)
			return
		}
	}

	writer.ValidateUnlockWait()

	if parentWriter != nil {
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

		if parentWriter != nil && !linkedAllKeys {
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

				data, parents, linkedAllKeys, linkingErrors = linkLevel(levelData, details)

				if linkingErrors != nil {
					errChan <- linkingErrors
					return
				}

				if linkedAllKeys {
					//If we did, the write validation allowed this
					//Try to check preconditions here
					writer.Read(data, parents)

					preconCtx, preconErrs = writer.CheckPrecondition(ctx)

					if len(preconErrs) > 0 {
						//Still got a problem
						errChan <- WriterErrorsToMap(preconErrs, details.Listable)
						return
					}

				} else {
					//Otherwise send our validate errors out
					errChan <- WriterErrorsToMap(preconErrs, details.Listable)
					return
				}

			} else {
				//Can't relink, just send our precon errors out
				errChan <- WriterErrorsToMap(preconErrs, details.Listable)
				return
			}
		} else {
			//Can't relink, just send our precon errors out
			errChan <- WriterErrorsToMap(preconErrs, details.Listable)
			return
		}
	}

	writer.PreconditionUnlockWait()

	if parentWriter != nil {
		parentWriter.WaitWrite()

		if parentWriter.WriteHadErrors() {
			writer.SetWriteHadErrors(true)
			writer.WriteUnlockWait()
			return
		}

	}

	//writer.Read()

	writerErrors := writer.Write(preconCtx)

	if len(writerErrors) > 0 {
		errChan <- WriterErrorsToMap(writerErrors, details.Listable)
		return
	}

	parentDataMap := make(map[pcolh.CollectionMessage][]interface{})

	for idx, parent := range parents {
		colMessage := parent.(pcolh.CollectionMessage)

		if _, ok := parentDataMap[colMessage]; !ok {
			parentDataMap[colMessage] = []interface{}{}
		}

		parentDataMap[colMessage] = append(parentDataMap[colMessage], data[idx])
	}

	for parentMessage, data := range parentDataMap {
		parentMessage.LoadCollection(colKey, data)
	}

	fmt.Println("++++++++++++")
	//fmt.Println(data)
	// /fmt.Println(parents)
	fmt.Println(parentDataMap)
	fmt.Println("++++++++++++")
	/*for _, w := range writerResponse {
		fmt.Println(w.DataMessage())
		fmt.Println(w.ParentMessage())
		fmt.Println("---------")
		fmt.Println("")
	}*/

	writer.WriteUnlockWait()
	fmt.Println(" HEY WERE DONE!")
}

type levelHolder struct {
	level     []pcolh.CollectionMessage
	levelData []proto.Message
	parent    pcolh.CollectionMessage
	colKey    string
}

func buildTree(ctx context.Context, registry *CollectionRegistry, level []*levelHolder,
	path string, parent string, writer CollectionWriter,
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

	var childLinkingLocks []*linkingWaitLock

	nextLevels := make(map[string][]*levelHolder)
	levelWriters := make(map[string]CollectionWriter)
	levelDetails := make(map[string]*pgcol.CollectionDetails)

	for _, topStuff := range level {
		for _, top := range topStuff.level {
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

					if _, ok := levelWriters[currentPath]; !ok {
						writer, writeErr := registry.Writer(e)
						if writer != nil && writeErr == nil {
							//Have a writer

							w, wErr := writer()

							if wErr != nil || w == nil {
								levelWriters[currentPath] = nil
							} else {
								levelWriters[currentPath] = w
							}

						} else {
							panic(writeErr.Error())
						}
					}

					if _, ok := nextLevels[currentPath]; !ok {
						nextLevels[currentPath] = []*levelHolder{}
					}

					if _, ok := levelDetails[currentPath]; !ok {
						levelDetails[currentPath] = e.DefaultDetails()
					}

					nextLevels[currentPath] = append(nextLevels[currentPath], &levelHolder{
						level:     e.CollectionMessageSlice(),
						levelData: e.ProtoSlice(),
						parent:    top,
						colKey:    key,
					})
					if e.DataIsCollectionMessage() {
						//buildTree(ctx, registry, e.CollectionMessageSlice(), e, elem, top, currentPath, path, key, writerMap, errChan, linkingWait, wg)
					} else {
						//buildLeaf(ctx, registry, e, elem, top, currentPath, path, key, writerMap, errChan, linkingWait, wg)
					}

				}
			}
		}
	}

	for levelPath, holder := range nextLevels {
		fmt.Println(levelPath, " ------ ", holder, " //// ", levelWriters[levelPath])
		buildTree(ctx, registry, holder, levelPath, path, levelWriters[levelPath], writerMap, errChan, linkingWait, wg)
		wg.Add(1)
		go newDataProcessor(ctx, holder,
			levelDetails[levelPath], levelWriters[levelPath],
			writer, errChan[levelPath],
			linkingWait[levelPath], linkingWait[path], wg)
	}

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

	if len(topLevel) > 0 {
		writerMap := make(map[string]CollectionWriter)
		errChan := make(map[string]chan map[string][]string)
		linkingWait := make(map[string]*linkingWaitLock)
		var wg sync.WaitGroup

		buildTree(ctx, registry, []*levelHolder{&levelHolder{level: topLevel, parent: nil}}, COL_DELIM, COL_DELIM, nil, writerMap, errChan, linkingWait, wg)
		fmt.Println("AFTER TREE")
		wg.Wait()

		fmt.Println("HERE WE ARE")
		for path, c := range errChan {
			fmt.Println("ERRRRRRRRRRRRR ", path)
			// At this point all the processors are done,
			// so its safe to just consume from the channels without a select, etc.
			err := <-c
			if err != nil && len(err) > 0 {
				return cleanErrMap(path, err)
			}

		}
		fmt.Println("~~~~~AT THE END~~~~~")
	}

	return nil
}
