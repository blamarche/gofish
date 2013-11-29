package main

import (
	"fmt"
	"flag"
	"strings"
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

	//u, _:=url.Parse("http://www.dracsoft.com/tech")
	//u2, _:=url.Parse("http://www.dracsoft.com/tech/")

	theurl := "http://www.dracsoft.com/tech/"
	if strings.LastIndex(theurl, "/")==len(theurl)-1 {
		theurl = strings.TrimRight(theurl, "/")
	}
	fmt.Println(theurl)
	//todo: multidb search
}
