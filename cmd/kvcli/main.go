package main

import (
	"fmt"
	"os"
	"kvstore/internal/db"
)

func printHelp() {
	fmt.Println("Usage:")
	fmt.Println("  kvstore put <key> <value>     Insert or update a key")
	fmt.Println("  kvstore get <key>             Get a value")
	fmt.Println("  kvstore del <key>             Delete a key")
	fmt.Println("  kvstore --help                Show this help message")
}

func main() {

	if len(os.Args) < 2 {
		fmt.Println("ERROR: No command provided")
		printHelp()
		return
	}

	cmd := os.Args[1]

	if cmd == "--help" || cmd == "-h" {
		printHelp()
		return
	}

	// Initialize database
	store, err := db.Open("data.log")
	if err != nil {
		fmt.Println("ERROR: cannot open database:", err)
		return
	}

	switch cmd {

	case "put":
		if len(os.Args) < 4 {
			fmt.Println("ERROR: put requires <key> <value>")
			return
		}

		key := os.Args[2]
		value := os.Args[3]

		err := store.Put(key, []byte(value))
		if err != nil {
			fmt.Println("ERROR:", err)
			return
		}
		fmt.Println("OK")

	case "get":
		if len(os.Args) < 3 {
			fmt.Println("ERROR: get requires <key>")
			return
		}

		key := os.Args[2]

		val, err := store.Get(key)
		if err != nil || val == nil {
			fmt.Println("NOT FOUND")
			return
		}

		fmt.Println(string(val))

	case "del":
		if len(os.Args) < 3 {
			fmt.Println("ERROR: del requires <key>")
			return
		}

		key := os.Args[2]

		err := store.Delete(key)
		if err != nil {
			fmt.Println("ERROR:", err)
			return
		}

		fmt.Println("OK")

	default:
		fmt.Println("ERROR: Unknown command:", cmd)
		printHelp()
	}
}
