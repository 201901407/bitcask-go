package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Welcome to Go Implementation of a simple Bitcask KV Store")
	fmt.Println("Commands: set <key> <value> | get <key> | delete <key> | stop")
	fmt.Println("-------------------------------------------------------------")

	for {
		fmt.Print(">> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		args := strings.Fields(input)
		cmd := strings.ToLower(args[0])

		switch cmd {
		case "set":
			if len(args) != 3 {
				fmt.Println("Incorrect usage of set command, correct usage: set <key> <value>")
				continue
			}
			//set function
			fmt.Printf("Set key '%s' to '%s'\n", args[1], args[2])

		case "get":
			if len(args) != 2 {
				fmt.Println("Incorrect usage of get command, correct usage: get <key>")
				continue
			}
			//get function
			if ok {
				fmt.Printf("Value: %s\n", value)
			} else {
				fmt.Println("Key not found")
			}

		case "delete":
			if len(args) != 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			kv.Delete(args[1])
			fmt.Printf("Deleted key '%s'\n", args[1])

		case "stop":
			fmt.Println("Exiting Bitcask KV store. Goodbye!")
			return

		default:
			fmt.Println("Unknown command. Available commands: set, get, delete, stop")
		}
	}
}
