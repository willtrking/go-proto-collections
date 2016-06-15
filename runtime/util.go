package runtime

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/golang/protobuf/proto"
	"github.com/willtrking/go-proto-collections/helpers"
	pgcol "github.com/willtrking/go-proto-collections/protocollections"
)

const COL_DELIM = "."

var dedupDelim = regexp.MustCompile(fmt.Sprintf("\\%s{2,}", COL_DELIM))

func upperFirst(s string) string {
	if s == "" {
		return ""
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToUpper(r)) + s[n:]
}

func cleanPath(path string) string {
	//Replace all occurences of 2+ delim's with 1 delim
	//Trim leading/trailing delim
	//Makes parsing a little easier
	path = strings.Trim(path, COL_DELIM)
	path = dedupDelim.ReplaceAllString(path, COL_DELIM)
	return path
}

func closeReadyChannelList(chans []chan string) {
	for _, c := range chans {
		if c != nil {
			close(c)
		}
	}
}

func closeParentPointerChannelList(chans map[string]chan []interface{}) {
	for _, c := range chans {
		if c != nil {
			close(c)
		}
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

// Link collections together by validating that the parentKey and the collectionKey are either equal,
// or can be set to be equal. Short circuit on first problem
// Has side effects on elemData.
func linkCollections(elemData []proto.Message, details *pgcol.CollectionDetails, colKey string, parentMessage helpers.CollectionMessage) ([]string, map[string][]string) {
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
