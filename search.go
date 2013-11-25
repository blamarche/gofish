package main

import (
	"fmt"
	"flag"

	/*
	"net"	
	"strconv"
	"time"
	"strings"
	*/
)

func main() {
	flag.Parse()
	
	args := flag.Args()
	fmt.Println(args);
}
