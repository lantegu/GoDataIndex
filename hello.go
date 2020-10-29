package main

import (
	"bufio"
	"fmt"
	"graph"
	"io"
	"os"
	"strings"
)

func main(){
	test := graph.NewFloatVectors()
	vectors := make([][]float64, 1024)
	test.vectors = vectors
	inputFile, inputError := os.Open("0.txt")
	if inputError != nil {
		fmt.Print("文件似乎不存在")
		return
	}
	defer inputFile.Close()
	inputReader := bufio.NewReader(inputFile)
	for {
		inputString, readerError := inputReader.ReadString('\n')
		stringSplit := strings.Split(inputString, ",")
		fmt.Print(len(stringSplit))
        if readerError == io.EOF {
            return
        }
	}
}

