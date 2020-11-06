package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)



func dirSort(listDirs []string) []string{
	intListDirs := make([]int ,len(listDirs))
	result := make([]string, len(listDirs))
	for i, listDir := range(listDirs){
		listDir = listDir[:strings.Index(listDir, ".")]
		intListDirs[i], _ = strconv.Atoi(listDir)
	} 
	sort.Ints(intListDirs)
	for i, intListDir := range(intListDirs){
		result[i] = strconv.Itoa(intListDir) + ".txt"
	}

	return result
}

// stringToFloats表示将字符串转换为浮点数组
func stringToFloats(data string, length int) ([] float64, error){
	vector := make([]float64, length)
	stringSplit := strings.Split(data, ",")
	stringSplit = stringSplit[:len(stringSplit)-1]
	for i, element := range stringSplit {
		if i >= length {
			return nil, errors.New("vectors' dim error")
		}
		vectorElement, _ := strconv.ParseFloat(element, 64)
		vector[i] = vectorElement
	}
	return vector, nil
}

// loadBucket 载入桶 path表示桶路径 length表示桶
func loadBucket(path string, length int) (indexs []int, vectors [][]float64,err error){
	indexs = make([]int, 0)
	inputFile, inputError := os.Open(path)
	if inputError != nil {
		fmt.Print("文件似乎不存在")
		return nil, nil, errors.New("Load file error")
	}
	defer inputFile.Close()
	vectors = make([][]float64, 0)
	inputReader := bufio.NewReader(inputFile)
	for {
		vector := make([]float64, length)
		inputString, readerError := inputReader.ReadString('\n')
		if readerError == io.EOF {
			break
		}
		index := inputString[:strings.Index(inputString, ":")]
		inputString = inputString[strings.Index(inputString, ":")+1:]
		indexInt, _ := strconv.Atoi(index)
		indexs = append(indexs, indexInt)
		vector, _ = stringToFloats(inputString, length)
		vectors = append(vectors, vector)
	}
	return indexs, vectors, nil
}

// path为向量路径， len为向量产生长度
func loadData(path string, length int) ([][]float64, error) {
	inputFile, inputError := os.Open(path)
	if inputError != nil {
		fmt.Print("文件似乎不存在")
		return nil, errors.New("Load file error")
	}
	defer inputFile.Close()
	vectors := make([][]float64, 0)
	inputReader := bufio.NewReader(inputFile)
	for {
		vector := make([]float64, length)
		inputString, readerError := inputReader.ReadString('\n')
		if readerError == io.EOF {
			break
		}
		inputString = inputString[strings.Index(inputString, ":")+1:len(inputString)-1]
		vector, _ = stringToFloats(inputString, length)
		vectors = append(vectors, vector)
	}
	return vectors, nil
}
// 聚类算法不一定要依赖Kmeans流程，其他也要用 num表示聚类点数 length表示向量维度 vectors 表示采样点
// center表示采样结果
func searchCenter(num int, length int,vectors *floatVectors,codeNum int) *floatVectors {
	center := NewFloatVectors()
	// 随机选取num个聚簇点作为初始聚簇中心
	randArray := make([]int, num)
	rand.Seed(time.Now().Unix())
	copy(randArray, rand.Perm(vectors.len)[:num])
	for _, index := range randArray {
		vector := NewFloatVector(length)
		vector.SetVector(vectors.vectors[index].vector)
		center.Append(*vector)
	}
	for i := 0; i < 500; i++ {
		neighbor := make([]int, vectors.len)
		var wg sync.WaitGroup
		for index, vector := range vectors.vectors {
			wg.Add(1)
			go func(index int, vector floatVector) {
				defer wg.Done()
				maxIndex, maxDistance := 0, -100000.0
				for centerIndex, centerPoint := range center.vectors {
					distance, err := vector.distance(centerPoint)
					if err != nil {
						fmt.Print("计算出错")
					}
					if distance > maxDistance {
						maxDistance = distance
						maxIndex = centerIndex
					}
				}
				neighbor[index] = maxIndex
			}(index, vector)
		}
		wg.Wait()
		// 重新计算每个簇的中心
		//count用来存储每个聚簇中心点的个数
		// 聚簇中心数据清零
		for j := 0; j < center.len; j++ {
			center.vectors[j].resetVector()
		}
		count := make([]int, num)
		// 此处两个函数添加sem并行
		for j, neigh := range neighbor {
			count[neigh]++
			center.vectors[neigh].addVector(vectors.vectors[j])
		}
		for j := 0; j < center.len; j++ {
			center.vectors[j].divVector(count[j])
		}
		if i%100 == 0{
			fmt.Printf("聚心%d运行%d次", codeNum, i)
		}
	}
	return center
}

func loadCenter(path string) *floatVectors{
	inputFile, inputError := os.Open(path)
	if inputError != nil {
		fmt.Printf("An error occurred on opening the inputfile\n" +
			"Does the file exist?\n" +
			"Have you got acces to it?\n")
	}
	defer inputFile.Close()
	center := NewFloatVectors()
	inputReader := bufio.NewReader(inputFile)
	for {
		inputString, readerError := inputReader.ReadString('\n')
		if readerError == io.EOF {
			break
		}
		inputString = inputString[strings.Index(inputString, ":")+1:]
		inputFloatArray := make([]float64, 0)
		tempString := strings.Split(inputString, ",")
		tempString = tempString[:len(tempString)-1]
		for _, element := range tempString {
			inputFloat, _ := strconv.ParseFloat(element, 64)
			inputFloatArray = append(inputFloatArray, inputFloat)
		}
		vector := NewFloatVector(1024)
		vector.SetVector(inputFloatArray)
		center.Append(*vector)
	}
	return center
}

func loadPqcenter(path string, M int) [](*floatVectors){
	pqCenter := make([]*floatVectors, M)
	inputFile, inputError := os.Open(path)
	if inputError != nil {
		fmt.Printf("An error occurred on opening the inputfile\n" +
			"Does the file exist?\n" +
			"Have you got acces to it?\n")
	}
	defer inputFile.Close()
	center := NewFloatVectors()
	row := 0
	inputReader := bufio.NewReader(inputFile)
	for {
		inputString, readerError := inputReader.ReadString('\n')
		if readerError == io.EOF {
			break
		}
		if inputString == "||\n"{
			pqCenter[row] = center
			center = NewFloatVectors()
			row++
		}else{
			inputFloatArray := make([]float64, 0)
			tempString := strings.Split(inputString, ",")
			tempString = tempString[:len(tempString)-1]
			for _, element := range tempString {
				inputFloat, _ := strconv.ParseFloat(element, 64)
				inputFloatArray = append(inputFloatArray, inputFloat)
			}
			vector := NewFloatVector(128)
			vector.SetVector(inputFloatArray)
			center.Append(*vector)
		}
	}
	return pqCenter
}