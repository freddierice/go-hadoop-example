package main

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/freddierice/go-hadoop.v2"
	hhdfs "gopkg.in/freddierice/go-hproto.v1/hdfs"
)

const (
	PROTOCOL_CONTEXT = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage:\n\t%v <user> <host> <dir>\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "description:\n\tThis is an example of how to "+
		"use go-hadoop to interface with a function of HDFS. This example "+
		"performs ls on a specified directory.\n")
	fmt.Fprintf(os.Stderr, "example:\n")
	fmt.Fprintf(os.Stderr, "\t%v user namenode:8020 /\n", os.Args[0])
	os.Exit(1)
}

func main() {
	if len(os.Args) != 4 {
		usage()
	}

	username := os.Args[1]
	host := os.Args[2]
	src := os.Args[3]
	conn, err := hadoop.Dial(username, host, PROTOCOL_CONTEXT, "hdfs", hadoop.AuthSasl)
	if err != nil {
		log.Fatal(err)
	}

	req := &hhdfs.GetFileInfoRequestProto{
		Src: &src,
	}
	res := &hhdfs.GetFileInfoResponseProto{}
	if err := conn.Call("getFileInfo", req, res); err != nil {
		log.Fatal(err)
	}
	if res.Fs.GetFileType() != hhdfs.HdfsFileStatusProto_IS_DIR {
		log.Fatal("src must be a directory")
	}

	b := true
	startAfter := []byte{0}
	reqList := &hhdfs.GetListingRequestProto{
		Src:          &src,
		StartAfter:   startAfter,
		NeedLocation: &b,
	}
	resList := &hhdfs.GetListingResponseProto{}
	if err := conn.Call("getListing", reqList, resList); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("listing for %v:\n", src)
	for _, d := range resList.GetDirList().GetPartialListing() {
		fmt.Printf("%v:%v\t\t%v", d.GetOwner(), d.GetGroup(), string(d.GetPath()))
		if d.GetFileType() == hhdfs.HdfsFileStatusProto_IS_DIR {
			fmt.Printf("/")
		}
		fmt.Println("")
	}
}
