package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/go-faker/faker/v4"
	"github.com/wubba-com/lsm-tree/cli"
	"github.com/wubba-com/lsm-tree/db"
)

const dataFolder = "demo"

var shouldReset, shouldSeed *bool
var seedNumRecords *int

func main() {
	setupFlags()

	if *shouldReset {
		eraseDataFolder()
	}

	d, err := db.Open(dataFolder)
	if err != nil {
		log.Fatal(err)
	}

	if *shouldSeed {
		seedDatabaseWithTestRecords(d)
	}

	scanner := bufio.NewScanner(os.Stdin)
	demo := cli.NewCLI(scanner, d)
	demo.Start()
}

func setupFlags() {
	shouldReset = flag.Bool("reset", false, "Reset the database by erasing its folder before startup.")
	shouldSeed = flag.Bool("seed", false, "Seed the database using records created with go-faker.")
	seedNumRecords = flag.Int("records", 1000, "Amount of records to seed the database with upon startup.")
	flag.Usage = func() {
		fmt.Println("\nDB CLI\n\nArguments:")
		flag.PrintDefaults()
	}
	flag.Parse()
}

func eraseDataFolder() {
	err := os.RemoveAll("demo")
	if err != nil {
		panic(err)
	}
}

func seedDatabaseWithTestRecords(d *db.DB) {
	for i := 0; i < *seedNumRecords; i++ {
		k := []byte(faker.Word() + faker.Word())
		v := []byte(faker.Word() + faker.Word())
		d.Set(k, v)
	}
}
