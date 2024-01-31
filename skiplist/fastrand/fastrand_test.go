package fastrand

import (
	"bytes"
	"fmt"
	"testing"
)

func TestFastrand(t *testing.T) {
	fmt.Println(bytes.Compare([]byte("b"), []byte("a")))
}
