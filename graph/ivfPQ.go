package main

import (
	"encoding/csv"
	//"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"

	//"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

// IvfPQ 量化索引，用于生成量化表
type IvfPQ struct {
	M          int             // M 为量化区段
	pqRoot     string          // 量化表的储存位置
	bucketRoot string          // bucketRoot 为桶位置
	center     *floatVectors   // center为第一次聚类的聚心
	pqCenter   []*floatVectors // pqCenter 为用于编码的聚类聚心共有M*pqNum个floatVector
}

// NewIvfPQ 生成一个量化结构体
func NewIvfPQ(M int) *IvfPQ {
	return &IvfPQ{M: M}
}

// 为一个向量的每一块生成编号
func (pointer *IvfPQ) getCode(clusterPoint *floatVectors, vector *floatVector, ch chan int) {
	maxIndex, maxDistance := 0, -100000.0
	for i, point := range clusterPoint.vectors {
		distance, err := vector.distance(point)
		if err != nil {
			fmt.Print(err)
		}
		if distance > maxDistance {
			maxDistance = distance
			maxIndex = i
		}
	}
	ch <- maxIndex
}

// dataPath 为数据来源地址（即第一次聚类地址），length为向量维度， num 为桶个数（即第一次聚簇点数），
// M 为量化分段个数 pqNum为量化的聚簇点, bucketExist 为判断桶是否存在，以及存在则无需建立
func (pointer *IvfPQ) createIndex(dataPath string, length int, num int, pqNum int, bucketExist bool) {
	if bucketExist == false {
		kmeans := NewKmeans()
		kmeans.createIndex(dataPath, length, num)
		kmeans.storeIndex(dataPath, length, "bucket", num)
	} else {
		centerFloat, _ := loadData("bucket/center.csv", 1024)
		vectors := NewFloatVectors()
		for _, vectorFloat := range centerFloat {
			vector := NewFloatVector(length)
			vector.SetVector(vectorFloat)
			vectors.Append(*vector)
		}
		pointer.center = vectors
	}
	rd, err := ioutil.ReadDir("bucket")
	if err != nil {
		fmt.Print("出错")
	}
	// 每个量化区块维度
	dim := length / pointer.M
	// 为bucket下的目录排序，按数值，略过center文件
	listDirs := make([]string, 0)
	for _, fi := range rd {
		if fi.Name() == "center.csv" {
			continue
		}
		listDirs = append(listDirs, fi.Name())
	}
	listDirs = dirSort(listDirs)
	pointer.pqCenter = make([]*floatVectors, pointer.M)
	// 遍历目录 对每个桶做均匀采样
	sampling := 512
	sampleData := NewFloatVectors()
	var mu sync.Mutex
	sem := make(semaphore, 4)

	for _, listDir := range listDirs {
		// 获取[][]floats格式数据, 采样大小默认为聚簇点*256, sampleData 为采样结果
		sem.P(1)
		fmt.Print("start reading bucket\n")
		go func(listDir string) {
			defer sem.V(1)
			_, data, _ := loadBucket("bucket"+"/"+listDir, length)
			if sampling >= len(data) {
				fmt.Print("数据量过少,请减少聚簇点数")
			}
			randArray := make([]int, sampling)
			rand.Seed(time.Now().Unix())
			copy(randArray, rand.Perm(len(data))[:sampling])
			for _, index := range randArray {
				vector := NewFloatVector(length)
				vector.SetVector(data[index])
				mu.Lock()
				sampleData.Append(*vector)
				mu.Unlock()
			}
		}(listDir)
	}
	for {
		if len(sem) == 0 {
			fmt.Print("完成聚类采样")
			break
		}
	}
	//每个采样区划分为八块
	sem = make(semaphore, 3)
	for i := 0; i < pointer.M; i++ {
		sem.P(1)
		go func(i int) {
			defer sem.V(1)
			cuttedSampleData, _ := sampleData.cutVectors(dim, i*dim, (i+1)*dim)
			pointer.pqCenter[i] = searchCenter(num, dim, cuttedSampleData, i)
		}(i)
		// sampleData得分割在这里 11月4日 停在此处 若聚类没问题  储存完就完事了
	}
	for {
		if len(sem) == 0 {
			fmt.Print("pq聚心完成")
			break
		}
	}
}

// path为根路径， length为向量维度
func (pointer *IvfPQ) storeIndex(dataPath string, length int, pqnum int) {
	rd, err := ioutil.ReadDir("bucket")
	if err != nil {
		fmt.Print("出错")
	}
	dim := length / pointer.M
	listDirs := make([]string, 0)
	for _, fi := range rd {
		if fi.Name() == "center.csv" {
			continue
		}
		listDirs = append(listDirs, fi.Name())
	}
	listDirs = dirSort(listDirs)
	err = os.Mkdir(dataPath+"/"+"pqCode", os.ModePerm)
	if err != nil {
		fmt.Print("编码桶已存在")
	}
	var mu sync.RWMutex
	sem := make(semaphore, 4)
	for i, listDir := range listDirs {
		//为每个桶单独创建文件夹并且编码
		sem.P(1)
		fmt.Printf("start encoding bucket:%d\n", i)
		go func(i int, listDir string) {
			defer sem.V(1)
			outputFile, outputError := os.OpenFile(dataPath+"/pqCode/"+strconv.Itoa(i)+".csv",
				os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
			if outputError != nil {
				fmt.Printf("An error occurred with file opening or creation\n")
			}
			defer outputFile.Close()
			outputWriter := csv.NewWriter(outputFile)
			indexs, data, _ := loadBucket(dataPath+"/bucket/"+listDir, length)
			for j, floatData := range data {
				vector := NewFloatVector(length)
				vector.SetVector(floatData)
				code := make([]string, pointer.M)
				var wg sync.WaitGroup
				for k := 0; k < pointer.M; k++ {
					wg.Add(1)
					// 为某桶内某一向量
					go func(k int) {
						defer wg.Done()
						tempvector, _ := vector.cutVector(dim, k*dim, (k+1)*dim)
						ch := make(chan int)
						mu.RLock()
						go pointer.getCode(pointer.pqCenter[k], tempvector, ch)
						mu.RUnlock()
						tempcode := <-ch
						code[k] = strconv.Itoa(tempcode)
					}(k)
				}
				wg.Wait()
				outputStrings := make([]string, 0)
				outputStrings = append([]string{strconv.Itoa(indexs[j])},code...)
				outputWriter.Write(outputStrings)
				if j%4000 == 0 {
					fmt.Printf("第:%d个桶第%d个编码完成\n", i, j)
				}
			}
			outputWriter.Flush()
			fmt.Printf("finish encoding :%d\n", i)
		}(i, listDir)
	}
	for {
		if len(sem) == 0 {
			fmt.Print("资源消耗完毕")
			break
		}
	}
	// 保存量化聚簇中心的txt
	centerFile, centerError := os.OpenFile(dataPath+"/pqCode/"+"center.csv",
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if centerError != nil {
		fmt.Printf("An error occurred with file opening or creation\n")
	}
	if err != nil {
		fmt.Print("文件创建出错")
	}
	defer centerFile.Close()
	outputWriter := csv.NewWriter(centerFile)
	for i := 0; i < pointer.M; i++ {
		for j := 0; j < pointer.pqCenter[i].length; j++ {
			outputWriter.Write(pointer.pqCenter[i].vectorString(j))
		}
		// 记录M的分割位置
		outputWriter.Write([]string{"||"})
	}
	outputWriter.Flush()
}

// 查找最匹配的向量
func (pointer *IvfPQ) searchVector(inputVector floatVector, length int, root string) (int, float64) {
	if _, err := os.Stat(root); os.IsNotExist(err) {
		fmt.Print("文件不存在")
	}
	if pointer.center == nil {
		pointer.center = loadCenter(root + "/bucket/center.csv")
	}
	if pointer.pqCenter == nil {
		pointer.pqCenter = make([]*floatVectors, pointer.M)
		pointer.pqCenter = loadPqcenter(root+"/pqCode/center.csv", pointer.M)
	}
	dim := length / pointer.M
	pqList := make([][]float64, pointer.M)
	for i := range pqList {
		pqList[i] = make([]float64, 0)
	}
	// 记录输入向量与pa聚心的距离
	for i := 0; i < pointer.M; i++ {
		tempvector, _ := inputVector.cutVector(dim, i*dim, (i+1)*dim)
		for _, vector := range pointer.pqCenter[i].vectors {
			distance, _ := vector.distance(*tempvector)
			pqList[i] = append(pqList[i], distance)
		}
	}
	// 找到最近粗聚点
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
	inputFile, inputError := os.Open(root + "/pqCode/" + strconv.Itoa(maxIndex) + ".csv")
	if inputError != nil {
		fmt.Printf("An error occurred on opening the inputfile\n" +
			"Does the file exist?\n" +
			"Have you got acces to it?\n")
	}
	defer inputFile.Close()
	inputReader := csv.NewReader(inputFile)
	maxIndex, maxDistance = 0, -100000.0
	for {
		inputString, readerError := inputReader.Read()
		if readerError == io.EOF {
			break
		}
		indexString := inputString[0]
		index, _ := strconv.Atoi(indexString)
		inputString = inputString[1:]
		distance := 0.0
		for i, element := range inputString {
			code, _ := strconv.Atoi(element)
			distance += pqList[i][code]
		}
		if distance > maxDistance {
			maxDistance = distance
			maxIndex = index
		}
	}
	return maxIndex, maxDistance
}

func main() {
	// kmeans 方法建立索引， 储存索引
	// kmeans := NewKmeans()
	// start := time.Now()
	// kmeans.createIndex("../csv_data", 1024, 20)
	// kmeans.storeIndex("../csv_data", 1024, "bucket", 20)
	// delta1 := time.Now().Sub(start)

	// 用于测试Kmeans
	// lijun, _ := loadData("./0_18.csv", 1024)
	// vector := NewFloatVector(1024)
	// for _, floatvector := range(lijun){
	// 	vector.SetVector(floatvector)
	// 	index, _, distance  := kmeans.searchVector(*vector, "bucket", 1024)
	// 	fmt.Print(index, distance,"\n")
	// }
	// IvfPQ 索引
	kivfPq := NewIvfPQ(8)
	// start = time.Now()
	kivfPq.createIndex("./bucket", 1024, 20, 100, true)
	kivfPq.storeIndex("./", 1024, 100)
	// delta2 := time.Now().Sub(start)
	// fmt.Printf("kmeans took this amount of time: %s\n", delta1)
	// fmt.Printf("ivfpq took this amount of time: %s\n", delta2)
	// for _, floatvector := range(lijun){
	// 	vector.SetVector(floatvector)
	// 	index, distance  := kivfPq.searchVector(*vector, 1024, ".")
	// 	fmt.Printf("索引号：%d, 距离：%f\n", index, distance)
	// }

}
