package skiplist

import (
	"fmt"
	"testing"
)

func TestSkipList(t *testing.T) {
	sl := NewSkipList()

	sl.Insert([]byte("a"), []byte("a"))
	sl.Insert([]byte("c"), []byte("c"))
	sl.Insert([]byte("b"), []byte("b"))
	// sl.Insert([]byte("d"), []byte("d"))
	// sl.Insert([]byte("e"), []byte("e"))
	// sl.Insert([]byte("f"), []byte("f"))
	// sl.Insert([]byte("k"), []byte("k"))
	// sl.Insert([]byte("g"), []byte("g"))

	//fmt.Println()
	//fmt.Println(sl.String())

	//sl.Delete([]byte("b"))

	fmt.Println()
	fmt.Println(sl.String())
}
