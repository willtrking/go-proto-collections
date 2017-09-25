package runtime

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/willtrking/go-proto-collections/helpers"

	"context"
)

type ReadWriteContainer struct {
	Parent               helpers.CollectionMessage
	Data                 []proto.Message
	CollectionContainers []proto.Message
	parentCollection     string
}

func (r *ReadWriteContainer) DataProtoSlice() []proto.Message {
	return r.Data
}

func (r *ReadWriteContainer) ParentMessage() helpers.CollectionMessage {
	return r.Parent
}

func (r *ReadWriteContainer) ParentCollection() string {
	return r.parentCollection
}

func shouldClose(closeSignal *uint32, closeLock *sync.Mutex) bool {
	closeLock.Lock()
	defer closeLock.Unlock()

	shouldClose := atomic.LoadUint32(closeSignal)
	if shouldClose > 0 {
		return true
	}

	return false
}

// Returns whether or not this function was the one that set the close signal
// true = we closed
// false = signal was already closed
func setCloseSignal(closeSignal *uint32, closeLock *sync.Mutex) bool {
	closeLock.Lock()
	defer closeLock.Unlock()

	shouldClose := atomic.LoadUint32(closeSignal)
	if shouldClose > 0 {
		return false
	}

	atomic.AddUint32(closeSignal, 1)

	return true

}

// The full chain for a single writer
// Takes care of locking, exiting when necessary, and ending errors back on a channel
// Returns a new context, a list of responses, and whether or not we successfully wrote
func writerChain(ctx context.Context, containers []*ReadWriteContainer, writer CollectionWriter, closeSignal *uint32, closeLock *sync.Mutex) ([]helpers.CollectionWriterResponse, bool, []WriterError) {

	writer.Read(containers)

	if shouldClose(closeSignal, closeLock) {
		return nil, false, nil
	}

	ctx, validateErr := writer.Validate(ctx)

	if shouldClose(closeSignal, closeLock) {
		return nil, false, nil
	}

	if len(validateErr) > 0 {
		return nil, true, validateErr
	}

	if shouldClose(closeSignal, closeLock) {
		return nil, false, nil
	}

	ctx, preconErr := writer.CheckPrecondition(ctx)

	if shouldClose(closeSignal, closeLock) {
		return nil, false, nil
	}

	if len(preconErr) > 0 {
		return nil, true, preconErr
	}

	if shouldClose(closeSignal, closeLock) {
		return nil, false, nil
	}

	response, writeErr := writer.Write(ctx)

	if shouldClose(closeSignal, closeLock) {
		return nil, false, nil
	}

	if len(writeErr) > 0 {
		return nil, true, writeErr
	}

	if shouldClose(closeSignal, closeLock) {
		return nil, false, nil
	}

	return response, true, nil

}

func readWriteLevel(ctx context.Context, registry *CollectionRegistry, level []helpers.CollectionMessage,
	path string, closeSignal *uint32, closeLock *sync.Mutex, asynchronousRW bool) ([]helpers.CollectionMessage, []WriterError, bool) {

	if shouldClose(closeSignal, closeLock) {
		return nil, nil, true
	}

	dataMap := make(map[RegistryKey][]*ReadWriteContainer)

	for _, d := range level {

		for _, key := range d.CollectionKeys() {
			elem := d.CollectionElem(key)
			if elem != nil {

				registryKey := registry.Key(elem)

				details := elem.DefaultDetails()

				collectionData := elem.ProtoSlice()

				linkCollections(collectionData, details, key, d)

				rwContainer := &ReadWriteContainer{
					Parent:               d,
					Data:                 make([]proto.Message, len(collectionData)),
					CollectionContainers: make([]proto.Message, len(collectionData)),
					parentCollection:     key,
				}

				for idx, cD := range collectionData {
					if elem.DataIsCollectionMessage() {
						asserted, assertedOk := cD.(helpers.CollectionMessage)

						if assertedOk {
							rwContainer.CollectionContainers[idx] = asserted.CollectionContainer()
						} else {
							rwContainer.CollectionContainers[idx] = nil
						}

					} else {
						rwContainer.CollectionContainers[idx] = nil
					}

					rwContainer.Data[idx] = cD
				}

				dataMap[registryKey] = append(dataMap[registryKey], rwContainer)

			}
		}
	}

	if shouldClose(closeSignal, closeLock) {
		return nil, nil, true
	}

	spawnedExecutions := 0

	var doneResponses [][]helpers.CollectionWriterResponse
	doneResponseChan := make(chan []helpers.CollectionWriterResponse, 1)
	writerErrChan := make(chan []WriterError, 1)

	for registryKey, containers := range dataMap {
		if len(containers) > 0 {

			writerFunc, writerFuncErr := registry.WriterForKey(registryKey)

			if writerFuncErr != nil {
				fmt.Println("Couldn't find writer for " + path + "." + containers[0].parentCollection)

				for _, c := range containers {
					c.Parent.ClearCollection(c.parentCollection)
				}
				/*n := make([]helpers.CollectionWriterResponse, len(containers))
				for idx, c := range containers {
					n[idx] = c
				}
				doneResponses = append(doneResponses, n)*/

				continue
			}

			if writerFunc == nil {
				fmt.Println("Couldn't find writer for " + path + "." + containers[0].parentCollection)

				for _, c := range containers {
					c.Parent.ClearCollection(c.parentCollection)
				}

				/*n := make([]helpers.CollectionWriterResponse, len(containers))
				for idx, c := range containers {
					n[idx] = c
				}
				doneResponses = append(doneResponses, n)*/

				continue
			}

			writer, writerErr := writerFunc()

			if writerErr != nil {
				fmt.Println("Couldn't find writer for " + path + "." + containers[0].parentCollection)

				for _, c := range containers {
					c.Parent.ClearCollection(c.parentCollection)
				}

				/*n := make([]helpers.CollectionWriterResponse, len(containers))
				for idx, c := range containers {
					n[idx] = c
				}
				doneResponses = append(doneResponses, n)*/

				continue
			}

			if writer == nil {
				fmt.Println("Couldn't find writer for " + path + "." + containers[0].parentCollection)

				for _, c := range containers {
					c.Parent.ClearCollection(c.parentCollection)
				}

				/*n := make([]helpers.CollectionWriterResponse, len(containers))
				for idx, c := range containers {
					n[idx] = c
				}
				doneResponses = append(doneResponses, n)*/

				continue
			}

			if asynchronousRW {

				go func(c []*ReadWriteContainer) {
					responses, didFinish, writerErrors := writerChain(ctx, c, writer, closeSignal, closeLock)

					if didFinish {
						if len(writerErrors) > 0 {
							if !shouldClose(closeSignal, closeLock) {
								if setCloseSignal(closeSignal, closeLock) {
									writerErrChan <- writerErrors
								}
							}
						} else {
							if !shouldClose(closeSignal, closeLock) {
								doneResponseChan <- responses
							}
						}

					} else {
						setCloseSignal(closeSignal, closeLock)
					}

				}(containers)

				spawnedExecutions++

			} else {
				responses, didFinish, writerErrors := writerChain(ctx, containers, writer, closeSignal, closeLock)

				if didFinish {
					if len(writerErrors) > 0 {
						if !shouldClose(closeSignal, closeLock) {
							if setCloseSignal(closeSignal, closeLock) {

								//Return here
								return nil, writerErrors, false
							}
						}
					} else {
						if !shouldClose(closeSignal, closeLock) {
							if len(responses) > 0 {
								doneResponses = append(doneResponses, responses)
							}
						}
					}

				} else {
					setCloseSignal(closeSignal, closeLock)
				}

			}
		}

	}

	if shouldClose(closeSignal, closeLock) {
		return nil, nil, true
	}

	// Only used if asynchronousRW == true
	// Otherwise spawnedExecutions remains at 0
	for i := 0; i < spawnedExecutions; i++ {
		select {
		case resp := <-doneResponseChan:
			if shouldClose(closeSignal, closeLock) {
				break
			}

			if len(resp) > 0 {
				doneResponses = append(doneResponses, resp)
			}

		case err := <-writerErrChan:
			if shouldClose(closeSignal, closeLock) {
				break
			}

			if len(err) > 0 {
				setCloseSignal(closeSignal, closeLock)
				close(doneResponseChan)
				close(writerErrChan)

				//Return here
				return nil, err, false
			}

		default:
			if shouldClose(closeSignal, closeLock) {
				break
			}
			// Didnt get anything, so dont increment our counter
			i--
		}
	}

	close(doneResponseChan)
	close(writerErrChan)

	if shouldClose(closeSignal, closeLock) {
		return nil, nil, true
	}

	//Next level to work with
	var nextLevel []helpers.CollectionMessage

	//Our new level
	newLevel := make(map[helpers.CollectionMessage]bool)
	nextLevelCollectionKeys := make(map[string]bool)

	//nextLevelKeys := make(map[string][]int)
	//Ok now load up our responses
	for _, responses := range doneResponses {
		for _, response := range responses {
			parent := response.ParentMessage()

			if shouldClose(closeSignal, closeLock) {
				break
			}

			for _, newData := range response.DataProtoSlice() {
				assertedMessage, assertedOk := newData.(helpers.CollectionMessage)
				if assertedOk {
					hasColData := false
					for _, key := range assertedMessage.CollectionKeys() {
						elem := assertedMessage.CollectionElem(key)
						if elem != nil {
							nextLevelCollectionKeys[key] = true
							hasColData = true
							break
						}
					}
					if hasColData {
						nextLevel = append(nextLevel, assertedMessage)
					}
				}
			}

			newLevel[parent] = true
		}
	}

	if shouldClose(closeSignal, closeLock) {
		return nil, nil, true
	}

	if len(nextLevel) > 0 {
		newNextLevel, nextLevelErrs, returnFromClose := readWriteLevel(ctx, registry, nextLevel, path, closeSignal, closeLock, asynchronousRW)

		if returnFromClose {
			//If we returned from a close, just end
			return nil, nil, true
		} else {
			// Didn't end from just a close
			// Did we have errs?
			if len(nextLevelErrs) > 0 {
				return nil, nextLevelErrs, false
			}

			//Read our new next level into our nextLevel
			for data, _ := range newLevel {
				for _, key := range data.CollectionKeys() {
					if _, keySent := nextLevelCollectionKeys[key]; keySent {

						var keyData []proto.Message

						for _, newData := range newNextLevel {
							if data.ProtoBelongsToCollection(newData, key) {
								keyData = append(keyData, newData)
							}
						}

						loadErr := data.LoadCollectionFromProto(key, keyData)

						if loadErr != nil {
							return nil, []WriterError{WriterErrorFromError(loadErr)}, false
						}
					}
				}
			}

		}
	}

	finalized := make([]helpers.CollectionMessage, len(newLevel), len(newLevel))
	idx := 0

	for data, _ := range newLevel {
		finalized[idx] = data
		idx++
	}

	return finalized, nil, false

}

func ReadWriteCollections(ctx context.Context, registry *CollectionRegistry, topLevel []helpers.CollectionMessage, asynchronousRW bool) ([]helpers.CollectionMessage, map[string]map[string][]string) {

	if len(topLevel) > 0 {

		var closeSignal uint32 = 0
		closeLock := &sync.Mutex{}
		//var wg sync.WaitGroup

		newLevel, rwErr, returnFromClose := readWriteLevel(ctx, registry, topLevel, COL_DELIM, &closeSignal, closeLock, false)

		//fmt.Println("newLevel ", newLevel)
		//scs := spew.ConfigState{Indent: "\t"}
		//scs.Dump(newLevel[0])

		if !returnFromClose {
			if len(rwErr) > 0 {
				return topLevel, cleanErrMap(COL_DELIM, WriterErrorsToMap(rwErr, true))
			}
			return newLevel, nil
		} else {
			return topLevel, nil
		}
		/*buildTree(ctx, registry, []*levelHolder{&levelHolder{level: topLevel, parent: nil}}, COL_DELIM, COL_DELIM, nil, writerMap, errChan, linkingWait, wg)
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
		fmt.Println("~~~~~AT THE END~~~~~")*/
	}

	return nil, nil
}
