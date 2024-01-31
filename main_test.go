package main

import (
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/wubba-com/lsm-tree/db"
)

var keys = []string{
	"absaepe",
	"abet",
	"accusantiumest",
}

func init() {
	log.SetOutput(io.Discard)
}

func BenchmarkSSTSearch(b *testing.B) {
	d, err := db.Open("demo")
	if err != nil {
		log.Fatal(err)
	}

	for _, k := range keys {
		b.Run(fmt.Sprintln(k), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err = d.Get([]byte(k))
				if err != nil {
					b.Fatal(err.Error())
				}
			}
		})
	}
}
