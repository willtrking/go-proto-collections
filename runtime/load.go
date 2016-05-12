package runtime

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"

	pcolh "github.com/willtrking/go-proto-collections/helpers"
)

func LoadCollections(r *CollectionRegistry, topLevel []pcolh.CollectionMessage, paths []string) error {

	if len(topLevel) > 0 && len(paths) > 0 {
		//Setup our map to collection loader singletons
		loaderMap := make(map[string]CollectionLoader)

		//Setup base of our map to our collection elements
		collectionMap := make(map[string]map[string]pcolh.CollectionElem)
		collectionMap[COL_DELIM] = topLevel[0].DefaultCollectionMap()

		totalPaths := len(paths)
		//Make enough ready channels for all of our sections
		//More details below
		readyChannels := make([][]chan string, totalPaths)

		//Parent pointer channel map, and other structures to support it
		//More details below
		parentPointerChannels := make(map[string]chan []interface{})
		parentChildCount := make(map[string]int)

		//Make sure it closes when we return
		defer closeParentPointerChannelList(parentPointerChannels)

		//So we can send our loader paths to our goroutines
		//AFTER this splitPath loop is finished
		//More details after loop block
		loaderPaths := make([][]string, totalPaths)

		//Define a group to wait until everything is loaded
		var loaderGroup sync.WaitGroup

		for pi, path := range paths {

			//Clean our path for simplified parsing
			path = cleanPath(path)

			//Split on our delimiter
			splitPath := strings.Split(path, COL_DELIM)

			//Init our buffers to hold current/parent path info
			var currentParent bytes.Buffer
			var currentPath bytes.Buffer
			//We start our our current path with a delim
			//The parent to the first element in our path will be the same value
			currentPath.WriteString(COL_DELIM)

			//Precalculate our length, necssary to check if we're at the end of the path
			totalSplit := len(splitPath)

			readyChannels[pi] = make([]chan string, totalSplit)

			//Make sure this always closes everything when we return
			defer closeReadyChannelList(readyChannels[pi])

			loaderPaths[pi] = make([]string, totalSplit)

			for si, split := range splitPath {
				//Setup current path key, each element of path is preceeded by .
				currentPath.WriteString(".")
				currentPath.WriteString(split)

				//Setup our current parent key
				currentParent.WriteString(".")
				if si != 0 {
					//If its not the first element in our path, we add the previous element
					currentParent.WriteString(splitPath[si-1])
				}

				//Start a goroutine for each path section
				//Once the loader/collection map generation is done for this section, we can do our loading
				//This is done with a ready channel, which receives he path that is ready when we're done
				//We've defered a close on all of these channels when the slice is created
				//This is critical to prevent these goroutines from becoming zombies
				//This can happen concurrently, since each loader is a state machine it holds
				//if it is currently loading (in which case we can block the single routine)
				//OR if it has loaded, in which case we just get the data back

				//The other set of channels we're making transport the full list of the parent data
				//When our goroutine runs, we load in collection data using data extracted from parents
				//We then need to load that data into a pointer of our collection elem, which is a
				//member of the proper parent protobuf
				//In the case of being at the top level, its simple as our pointers to those parents
				//are passed in on function call
				//Any level after that has no direct reference to the pointers which have been
				//created for the top level, or any level below it
				//Thus we need a channel to pass them to, and we need to wait for them to be passed into this channel to continue
				//These channels are created at the end of our parsing, right before we send the ready signal
				//This is so we can make non blocking buffered channels of the proper size

				loaderGroup.Add(1)
				readyChannels[pi][si] = make(chan string)

				go func(readyChan chan string, parent string, path string, colKey string) {
					defer loaderGroup.Done()

					readyPath := <-readyChan

					if readyPath == "" {
						//fmt.Println(" ---- EXIT ",parent,path)
						//Channel was closed or we got an exit signal
						return
					}

					if !loaderMap[readyPath].Loading() && !loaderMap[readyPath].Loaded() {
						//Grab parent keys to load from
						skipLoad := false
						var parentKeys []interface{}

						//If parent is our delim, this is top level, so we need a special extraction
						if parent == COL_DELIM {
							//fmt.Println("LOADING TOPLEVEL")
							for _, dat := range topLevel {

								//Get the parent key data for this collection
								v := dat.CollectionParentKeyData(colKey)

								if v != nil {
									parentKeys = append(parentKeys, v)
								}

							}
							//fmt.Println(parentKeys)

						} else {
							//Gotta wait for parent to be loaded
							loaderMap[parent].Wait()

							parentData := loaderMap[parent].InterfaceSlice()
							//fmt.Println("    ----LOADED PARENT ",parentData)

							if parentData == nil {
								//If its nil IDK LOL
								//TODO
							} else {

								//Extract necessary info
								for _, dat := range parentData {

									assertedDat := dat.(pcolh.CollectionMessage)

									v := assertedDat.CollectionParentKeyData(colKey)

									if v != nil {
										parentKeys = append(parentKeys, v)
									}

								}

							}
						}

						//fmt.Println(" --- OK",skipLoad)
						//fmt.Printf("    LOADED PARENT",parentKeys)

						//If we're skipping, just get out right now
						if skipLoad {
							return
						}

						//Load from our parent keys, should block
						loaderMap[readyPath].Load(parentKeys)

						//Extract the loaded data
						loaded := loaderMap[readyPath].InterfaceSlice()

						var pointers []interface{}
						//If parent is our delim, this is top level, so we need a special extraction
						if parent == COL_DELIM {

							//Iterate through all our top level protobufs
							for _, topProto := range topLevel {

								//Setup basic helpers
								var found []interface{}

								//Loop over all of our loaded data
								for _, item := range loaded {

									if topProto.ProtoBelongsToCollection(item, colKey) {
										found = append(found, item)
									}

								}

								//Load what we found into our top level protobuf by the collection key we're on
								topProto.LoadCollection(colKey, found)
								pointers = append(pointers, topProto.CollectionInterfaceSlice(colKey)...)
							}

							//Ensure our path is in both the parentPointerChannels AND parentChildCount
							//Otherwise we're not a real parent
							if _, ok := parentPointerChannels[path]; ok {
								if _, ok := parentChildCount[path]; ok {
									for i := 0; i < parentChildCount[path]; i++ {
										//fmt.Println("---- POINTERS", pointers)
										parentPointerChannels[path] <- pointers
									}
								}
							}

						} else {
							//Ensure our parent is parentPointerChannels
							//Otherwise we can't do anything
							if _, ok := parentPointerChannels[parent]; ok {
								//fmt.Println(" --- CHILD LOADED ",colKey,loaded)
								parentPointers := <-parentPointerChannels[parent]
								//fmt.Println("---- PARENT POINTERS", colKey, fmt.Sprintf("%p",parentPointers))

								for _, topProto := range parentPointers {
									//fmt.Println(topProto)

									asserted := topProto.(pcolh.CollectionMessage)

									//Setup basic helpers
									var found []interface{}

									//Loop over all of our loaded data
									for _, item := range loaded {

										if asserted.ProtoBelongsToCollection(item, colKey) {
											found = append(found, item)
										}

									}

									//Load what we found into our top level protobuf by the collection key we're on
									//fmt.Println(" ---- ASSERTED",asserted)
									//fmt.Println(found)
									asserted.LoadCollection(colKey, found)

									pointers = append(pointers, asserted.CollectionInterfaceSlice(colKey)...)

								}
								//fmt.Println(" +++++ CLEANED ",colKey,parentPointers)
								//Ensure our path is in both the parentPointerChannels AND parentChildCount
								//Otherwise we're not a real parent
								if _, ok := parentPointerChannels[path]; ok {
									if _, ok := parentChildCount[path]; ok {
										for i := 0; i < parentChildCount[path]; i++ {
											//fmt.Println("---- POINTERS", pointers)
											parentPointerChannels[path] <- pointers
										}
									}
								}

							}

						}

					}

				}(readyChannels[pi][si],
					currentParent.String(),
					currentPath.String(),
					split)

				//Grab the parent's DefaultCollectionMap
				//Ensure it has one in the first place
				parentCollectionMap, pcOK := collectionMap[currentParent.String()]
				if !pcOK {
					if si != 0 {
						return errors.New(fmt.Sprintf("%s has no collections!", currentParent.String()))
					} else {
						return errors.New(fmt.Sprintf("Top level has no collections!"))
					}
				}

				//Grab our current path from the parent DefaultCollectionMap
				//Ensure it exists
				collectionElem, coOK := parentCollectionMap[split]
				if !coOK {
					if si != 0 {
						return errors.New(fmt.Sprintf("%s is not a collection for %s", currentPath.String(), currentParent.String()))
					} else {
						return errors.New(fmt.Sprintf("%s is not a collection for top level", currentPath.String()))
					}
				}

				//Check to see if we've already setup a loader for our current path
				if _, ok := loaderMap[currentPath.String()]; !ok {
					//We haven't set it up

					//Grab the loader from our registry, if we have it
					loaderFunc, fErr := r.Loader(collectionElem)
					if fErr != nil {

						return errors.New(fmt.Sprintf("Loader for %s not registered", currentPath.String()))
					}

					details := collectionElem.DefaultDetails()

					loader, lErr := loaderFunc(details.CollectionKey)

					if lErr != nil {
						return errors.New(fmt.Sprintf("Loader for %s can't load data with key %s", currentPath.String(), details.CollectionKey))
					}

					//Add it to our loader map
					loaderMap[currentPath.String()] = loader

				}
				//Append to our slice
				//fmt.Println("ADDING",currentPath.String())
				//loaderPaths = append(loaderPaths,currentPath.String())
				loaderPaths[pi][si] = currentPath.String()
				//loaderPaths = append(loaderPaths,currentPath.String())

				//Check to see if we're at the end of our path,
				//IF we're not, the current path is about to become a parent
				//So, our current path protobuf has collections, add it into our collectionMap
				//The next iteration will retrieve it by parent
				//We also need to add ourselves into the parentChildCount map if we aren't already
				//And add one to that value (supports parentPointerChannels)
				if si < totalSplit-1 {
					//Is this path already in here? If so, don't bother
					if _, ok := collectionMap[currentPath.String()]; !ok {
						//Add it in
						//In order to do this, we have to see if the type
						//which the current collections fetches itself a CollectionMessage
						//Note that this is the TYPE THE COLLECTION FETCHES and NOT THE COLLECTION ITSELF
						collectionType := collectionElem.DataProto()
						convertedMessage, convOK := collectionType.(pcolh.CollectionMessage)

						if !convOK {
							//It's not, so throw an error because we're about to try to fetch through it!
							return errors.New(fmt.Sprintf("%s has no collections!", currentPath.String()))
						}

						//We're all set! Map it up
						collectionMap[currentPath.String()] = convertedMessage.DefaultCollectionMap()

					}

					if _, ok := parentChildCount[currentPath.String()]; !ok {
						parentChildCount[currentPath.String()] = 0
					}

					parentChildCount[currentPath.String()]++

				}

			}

		}

		//Setup our parent pointer channels before signaling we're ready
		for parent, count := range parentChildCount {
			//We make them buffered to prevent blocks
			parentPointerChannels[parent] = make(chan []interface{}, count)
		}

		//fmt.Println(parentChildCount)
		//We want to send off each loader in the split path ONCE WE'RE DONE WITH THE PARSE LOOP
		//Theres no hurt to performance by doing this, BUT if the path ends up being invalid
		//we avoid keeping un-necessary resources open
		for li, lps := range loaderPaths {
			for lii, lp := range lps {
				readyChannels[li][lii] <- lp
			}
		}

		loaderGroup.Wait()
		//fmt.Println(fmt.Sprintf("%#v",loaderMap))
		//fmt.Println(fmt.Sprintf("%#v",collectionMap))
	}
	return nil
}
