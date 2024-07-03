package metadata

import (
	"encoding/json"
	"io"
)

func ReadBodyToBlob(r io.Reader) ([]byte, error) {
	return io.ReadAll(r)
}

func LoadBlobToJson(blob []byte, dest interface{}) error {
	return json.Unmarshal(blob, dest)
}
