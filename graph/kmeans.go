// Kmeans 聚簇索引，展示聚簇的功能，具有根目录（储存路径）
// 中心点文件路径名与桶路径名（表示分成的桶个数）, vectors表示生产而成的向量组，用于各类操作
// 一般vectors为少数采样点
package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

type Empty interface{}
type semaphore chan Empty

// acquire n resources
func (s semaphore) P(n int) {
	e := new(Empty)
	for i := 0; i < n; i++ {
		s <- e
	}
}

// release n resources
func (s semaphore) V(n int) {
	for i := 0; i < n; i++ {
		<-s
	}
}

// Kmeans Kmeans索引
type Kmeans struct {
	root    string
	vectors *floatVectors
	center  *floatVectors
}

// NewKmeans 向外生产一个Kmeans
func NewKmeans() *Kmeans {
	return &Kmeans{}
}

// 建立索引并返回建立索引后的索引位置 len表示向量维度长度,num 表示 聚簇点个数
func (pointer *Kmeans) createIndex(dataPath string, length int, num int) (string, error) {
	rd, err := ioutil.ReadDir(dataPath)
	if err != nil {
		fmt.Print("出错")
	}
	sampling := num * 256 / len(rd)
	pointer.vectors = NewFloatVectors()
	var mu sync.Mutex
	sem := make(semaphore, 2)
	for _, fi := range rd {
		sem.P(1)
		fmt.Print("start\n")
		go func(path string) {
			defer sem.V(1)
			result := make([][]float64, 0)
			result, err = loadData(dataPath+"/"+path, length)
			if err != nil {
				fmt.Print("load data error")
			}
			if sampling >= len(result) {
				fmt.Print("数据量过少,请减少聚簇点数")
			}
			randArray := make([]int, sampling)
			rand.Seed(time.Now().Unix())
			copy(randArray, rand.Perm(len(result))[:sampling])

			for _, index := range randArray {
				vector := NewFloatVector(length)
				vector.SetVector(result[index])
				mu.Lock()
				pointer.vectors.Append(*vector)
				mu.Unlock()
			}
			fmt.Print("finish\n")
		}(fi.Name())
	}
	for {
		if len(sem) == 0 {
			fmt.Print("资源消耗完毕")
			break
		}
	}
	pointer.searchCenter(num, length)
	return "", nil
}

// num 表示聚簇点中心个数
func (pointer *Kmeans) searchCenter(num int, length int) error {
	if pointer.vectors == nil {
		return errors.New("数据尚在加载无法生成")
	}
	if pointer.center != nil {
		return errors.New("中心数据已产生，无需搜索")
	}
	vectors := pointer.vectors
	pointer.center = searchCenter(num, length, vectors, 0)

	return nil
}

// 储存索引并返回成功标志
func (pointer *Kmeans) storeIndex(dataPath string, length int, bucketPath string, num int) (bool, error) {
	if pointer.center == nil {
		return false, errors.New("聚类算法尚未运行")
	}
	// bucket 为桶，将每个向量储存到对应的桶中，
	// bucketIdentifier是存储编号的桶，因为每个向量有自己的编号，这样才能对应进行搜索。
	rd, err := ioutil.ReadDir(dataPath)
	if err != nil {
		fmt.Print("出错")
	}
	err = os.Mkdir(bucketPath, os.ModePerm)
	if err != nil {
		fmt.Print("bucket 已经加载")
	}
	// 对csv文件进行排序，默认是按字符串顺序排序，我们按照数值排序
	listDirs := make([]string, 0)
	for _, fi := range rd {
		listDirs = append(listDirs, fi.Name())
	}
	listDirs = dirSort(listDirs)
	// 记录总数 因为是多个文件
	count := 0
	for _, listDir := range listDirs {
		bucket := make([]floatVectors, num)
		bucketIdentifier := make([][]int, num)
		for i := range bucketIdentifier {
			bucketIdentifier[i] = make([]int, 0)
		}
		data, _ := loadData(dataPath+"/"+listDir, length)
		var wg sync.WaitGroup
		var mu sync.Mutex
		for i, floatData := range data {
			wg.Add(1)
			go func(floatData []float64, i int) {
				defer wg.Done()
				maxIndex, maxDistance := 0, -100000.0
				vector := NewFloatVector(length)
				vector.SetVector(floatData)
				for centerIndex, centerPoint := range pointer.center.vectors {
					distance, err := vector.distance(centerPoint)
					if err != nil {
						fmt.Print(err)
					}
					if distance > maxDistance {
						maxDistance = distance
						maxIndex = centerIndex
					}
				}
				mu.Lock()
				bucket[maxIndex].Append(*vector)
				bucketIdentifier[maxIndex] = append(bucketIdentifier[maxIndex], i)
				mu.Unlock()

			}(floatData, i+count)
		}
		count += len(data)
		wg.Wait()
		for i, bucketVector := range bucket {
			wg.Add(1)
			go func(i int, bucketVector floatVectors) {
				defer wg.Done()
				outputFile, outputError := os.OpenFile("./"+bucketPath+"/"+strconv.Itoa(i)+".csv",
					os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
				if outputError != nil {
					fmt.Printf("An error occurred with file opening or creation\n")
				}
				defer outputFile.Close()
				outputWriter := csv.NewWriter(outputFile)
				for j := 0; j < bucketVector.length; j++ {
					outputstrings := bucketVector.vectorString(j)
					outputstrings = append([]string {strconv.Itoa(bucketIdentifier[i][j])}, outputstrings...)
					mu.Lock()
					outputWriter.Write(outputstrings)
					mu.Unlock()
				}
				outputWriter.Flush()
			}(i, bucketVector)
		}
		wg.Wait()
	}

	// 存储中心点
	outputFile, outputError := os.OpenFile("./"+bucketPath+"/center.csv",
		os.O_WRONLY|os.O_CREATE, 0666)
	if outputError != nil {
		fmt.Printf("An error occurred with file opening or creation\n")
		return false, nil
	}
	defer outputFile.Close()
	outputWriter := csv.NewWriter(outputFile)
	for j := 0; j < pointer.center.length; j++ {
		outputStrings := pointer.center.vectorString(j)
		outputStrings = append([]string {strconv.Itoa(j)}, outputStrings...)
		outputWriter.Write(outputStrings)
	}
	outputWriter.Flush()
	return true, nil
}

// 调用查询函数查询与特征最接近的向量 inputvect为输入的待搜索向量， root 为文件路径 length为向量维度
func (pointer *Kmeans) searchVector(inputVector floatVector, root string, length int) (int, floatVector, float64) {
	if _, err := os.Stat(root); os.IsNotExist(err) {
		fmt.Print("文件不存在")
	}
	pointer.root = root
	// 如果还没有聚簇点，那么加载聚簇点
	if pointer.center == nil {
		inputFile, inputError := os.Open(root + "/center.csv")
		if inputError != nil {
			fmt.Printf("An error occurred on opening the inputfile\n" +
				"Does the file exist?\n" +
				"Have you got acces to it?\n")
		}
		defer inputFile.Close()
		pointer.center = NewFloatVectors()
		inputReader := csv.NewReader(inputFile)
		for {
			inputString, readerError := inputReader.Read()
			if readerError == io.EOF {
				break
			}
			inputString = inputString[1:]
			inputFloatArray := make([]float64, length)
			for i, element := range inputString {
				inputFloat, _ := strconv.ParseFloat(element, 64)
				inputFloatArray[i] = inputFloat
			}
			vector := NewFloatVector(length)
			vector.SetVector(inputFloatArray)
			pointer.center.Append(*vector)
		}
	}
	// maxIndex 为获取的桶编号, 先将输入向量特征与聚簇点匹配，找到相对应的桶
	maxIndex, maxDistance := 0, -100000.0
	for index, vector := range pointer.center.vectors {
		distance, err := vector.distance(inputVector)
		if err != nil {
			fmt.Print("计算出错")
		}
		if distance > maxDistance {
			maxDistance = distance
			maxIndex = index
		}
	}
	// 加载相应的桶
	inputFile, inputError := os.Open(root + "/" + strconv.Itoa(maxIndex) + ".csv")
	if inputError != nil {
		fmt.Printf("An error occurred on opening the inputfile\n" +
			"Does the file exist?\n" +
			"Have you got acces to it?\n")
	}
	defer inputFile.Close()
	inputReader := csv.NewReader(inputFile)
	var wg sync.WaitGroup
	var mu sync.Mutex
	maxIndex, maxDistance = 0, -100000.0
	maxVector := NewFloatVector(1024)
	// 加载桶内每个向量与目标向量做匹配
	for {
		inputString, readerError := inputReader.Read()
		if readerError == io.EOF {
			break
		}
		indexString := inputString[0]
		index, _ := strconv.Atoi(indexString)
		inputString = inputString[1:]
		inputFloatArray := make([]float64, length)
		for i, element := range inputString {
			inputFloat, _ := strconv.ParseFloat(element, 64)
			inputFloatArray[i] = inputFloat
		}
		vector := NewFloatVector(length)
		vector.SetVector(inputFloatArray)
		wg.Add(1)
		go func(index int, vector floatVector) {
			defer wg.Done()
			distance, err := vector.distance(inputVector)
			if err != nil {
				fmt.Print("计算出错")
			}
			mu.Lock()
			if distance > maxDistance {
				maxDistance = distance
				maxIndex = index
				maxVector = &vector
			}
			mu.Unlock()
		}(index, *vector)
	}
	wg.Wait()
	return maxIndex, *maxVector, maxDistance
}
