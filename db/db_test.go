package db

import (
	"fmt"
	"testing"

	"github.com/go-faker/faker/v4"
)

func TestDb(t *testing.T) {

	tr, err := Open(Folder)
	if err != nil {
		panic(err)
	}

	var seedNumRecords = 1000
	var firstKey []byte

	for i := 0; i < seedNumRecords; i++ {
		k := []byte(faker.Word() + faker.Word())
		v := []byte(faker.Word() + faker.Word())
		if i == 0 {
			firstKey = k
		}

		tr.Set(k, v)
	}
	fmt.Println(string(firstKey))
	v, err := tr.Get(firstKey)
	if err != nil {
		t.Error(err)
	}

	fmt.Println(string(v))
}
