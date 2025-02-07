// utils/utils.go
package utils

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

func Encode(obj interface{}) string {
	b, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func Decode(in string, obj interface{}) error {
	b, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		return fmt.Errorf("failed to decode base64: %w", err)
	}

	return json.Unmarshal(b, obj)
}
