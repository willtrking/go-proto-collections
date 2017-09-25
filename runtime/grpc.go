package runtime

import (
	"strings"

	"context"

	"google.golang.org/grpc/metadata"
)

//Extract paths for LoadCollections from GRPC metadata
func PathsFromMetadata(m metadata.MD) []string {
	var final []string
	if m != nil {
		if frm, ok := m["collections"]; ok {
			for _, c := range frm {
				c = strings.TrimSpace(c)

				if c != "" {
					split := strings.Split(c, ",")
					for _, s := range split {
						s = strings.TrimSpace(s)
						if s != "" {
							final = append(final, s)
						}
					}
				}
			}

		}
	}
	return final
}

//Extract paths for LoadCollections from GRPC context
func PathsFromContext(c context.Context) []string {
	if c != nil {
		if md, ok := metadata.FromIncomingContext(c); ok {
			return PathsFromMetadata(md)
		}
	}
	return nil
}
