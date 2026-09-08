package core

import (
	"fmt"
	"strings"
	"unicode"
)

const (
	maxStreamIDLength   = 128
	maxStreamNameLength = 256
)

// ValidateStreamID validates an identifier that may be used in URLs and data paths.
func ValidateStreamID(id string) error {
	if id == "" || len(id) > maxStreamIDLength || id[0] < '0' || id[0] > 'z' {
		return ErrInvalidStreamID
	}
	if (id[0] < 'A' || id[0] > 'Z') && (id[0] < 'a' || id[0] > 'z') && (id[0] < '0' || id[0] > '9') {
		return ErrInvalidStreamID
	}
	for _, r := range id {
		if (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' && r != '_' && r != '.' {
			return ErrInvalidStreamID
		}
	}
	return nil
}

func validateStreamName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > maxStreamNameLength {
		return fmt.Errorf("stream name must contain 1-%d characters", maxStreamNameLength)
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return fmt.Errorf("stream name contains a control character")
		}
	}
	return nil
}
