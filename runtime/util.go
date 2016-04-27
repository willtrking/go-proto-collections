package runtime

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
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
