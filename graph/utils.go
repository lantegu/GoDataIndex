package main

import (
	"bufio"
	"encoding/csv"
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

// Index 是索引接口，展示索引所需要的功能
type Index interface {
	createIndex(path string) string
	storeIndex(path string) bool
	searchVector(vector floatVector) (int, floatVector)
}

func dirSort(listDirs []string) []string {
	intListDirs := make([]int, len(listDirs))
	result := make([]string, len(listDirs))
	for i, listDir := range listDirs {
		listDir = listDir[:strings.Index(listDir, ".")]
		intListDirs[i], _ = strconv.Atoi(listDir)
	}
	sort.Ints(intListDirs)
	for i, intListDir := range intListDirs {
		result[i] = strconv.Itoa(intListDir) + ".txt"
	}

	return result
}

// 获取最近向量 vector表示待比较向量 vectors表示向量组
func getNeighVector(vector floatVector, pointerVectors *floatVectors, ch chan int) {
	maxIndex, maxDistance := 0, -100000.0
	for centerIndex, centerPoint := range pointerVectors.vectors {
		distance, err := vector.distance(centerPoint)
		if err != nil {
			fmt.Print("计算出错")
		}
		if distance > maxDistance {
			maxDistance = distance
			maxIndex = centerIndex
		}
	}
	ch <- maxIndex
}

// stringToFloats表示将字符串转换为浮点数组
func stringToFloats(data []string, length int, splitString string) ([]float64, error) {
	vector := make([]float64, length)
	// 按某个字符分割
	if len(data) > length {
		return nil, errors.New("vectors' dim error")
	}
	for i, element := range data {
		vectorElement, _ := strconv.ParseFloat(element, 64)
		vector[i] = vectorElement
	}
	return vector, nil
}

// loadBucket 载入桶 indexs表示Bucket所有数编号， vectors表示buvket所有数的向量组
func loadBucket(path string, length int) (indexs []int, vectors [][]float64, err error) {
	indexs = make([]int, 0)
	csvFile, err := os.Open(path)
	if err != nil {
		fmt.Print("文件似乎不存在")
		return nil, nil, errors.New("Load file error")
	}
	defer csvFile.Close()
	vectors = make([][]float64, 0)
	csvReader := csv.NewReader(csvFile)
	for {
		vector := make([]float64, length)
		inputString, readerError := csvReader.Read()
		if readerError == io.EOF {
			break
		}
		index := inputString[0]
		inputString = inputString[1:]
		indexInt, _ := strconv.Atoi(index)
		indexs = append(indexs, indexInt)
		vector, _ = stringToFloats(inputString, length, ",")
		vectors = append(vectors, vector)
	}
	return indexs, vectors, nil
}

// path为向量路径， len为向量产生长度
func loadData(path string, length int) ([][]float64, error) {
	csvfile, err := os.Open(path)
	if err != nil {
		fmt.Print("文件似乎不存在")
		return nil, errors.New("Load file error")
	}
	defer csvfile.Close()
	vectors := make([][]float64, 0)
	csvReader := csv.NewReader(csvfile)
	for {
		vector := make([]float64, length)
		inputString, readerError := csvReader.Read()
		if readerError == io.EOF {
			break
		}
		vector, _ = stringToFloats(inputString, length, ",")
		vectors = append(vectors, vector)
	}
	return vectors, nil
}

// 寻找聚类中心 num表示聚类点数 length表示向量维度 vectors 表示采样点，codenum为编号，仅用于辅助打印
// center表示采样结果
func searchCenter(num int, length int, vectors *floatVectors, codeNum int) *floatVectors {
	center := NewFloatVectors()
	// 随机选取num个聚簇点作为初始聚簇中心
	randArray := make([]int, num)
	rand.Seed(time.Now().Unix())
	copy(randArray, rand.Perm(vectors.length)[:num])
	for _, index := range randArray {
		vector := NewFloatVector(length)
		vector.SetVector(vectors.vectors[index].vector)
		center.Append(*vector)
	}
	for i := 0; i < 5; i++ {
		neighbor := make([]int, vectors.length)
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
		for j := 0; j < center.length; j++ {
			center.vectors[j].resetVector()
		}
		count := make([]int, num)
		for j, neigh := range neighbor {
			count[neigh]++
			wg.Add(1)
			go func(j int, neigh int) {
				defer wg.Done()
				center.vectors[neigh].addVector(vectors.vectors[j])
			}(j, neigh)
		}
		wg.Wait()
		for j := 0; j < center.length; j++ {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				center.vectors[j].divNum(count[j])
			}(j)
		}
		wg.Wait()
		if i%100 == 0 {
			fmt.Printf("聚心%d运行%d次", codeNum, i)
		}
	}
	return center
}

// 载入聚类中心
func loadCenter(path string) *floatVectors {
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

// 载入pq量化中心
func loadPqcenter(path string, M int) [](*floatVectors) {
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
		if inputString == "||\n" {
			pqCenter[row] = center
			center = NewFloatVectors()
			row++
		} else {
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
