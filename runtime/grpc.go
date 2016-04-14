package runtime

import (
	"strings"

	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
)

//Extract paths for LoadCollections from GRPC metadata
func PathsFromMetadata(m metadata.MD) ([]string, error) {
	var final []string
	if frm, ok := m["collections"]; ok {
		for _, c := range frm {
			c = strings.TrimSpace(c)

			if len(c) > 0 {
				split := strings.Split(c, ",")
				for _, s := range split {
					s = strings.TrimSpace(s)
					if len(s) > 0 {
						final = append(final, s)
					}
				}
			}
		}

	}
	return final, nil
}

//Extract paths for LoadCollections from GRPC context
func PathsFromContext(c context.Context) ([]string, error) {
	if md, ok := metadata.FromContext(c); ok {
		return PathsFromMetadata(md)
	}
	return nil, nil
}
