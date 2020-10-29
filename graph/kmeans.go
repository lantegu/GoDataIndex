// Kmeans 聚簇索引，展示聚簇的功能，具有根目录（储存路径）
// 中心点文件路径名与桶路径名（表示分成的桶个数）, vectors表示生产而成的向量组，用于各类操作
// 一般vectors为少数采样点
package main
import (
	"bufio"
	"sync"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)
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
func (pointer *Kmeans) createIndex(path string, length int, num int) (string, error) {
	result := make([][]float64, 0)
	var err error
	result, err = loadData(path, length)
	if err != nil {
		return "", errors.New("load data error")
	}
	if num*256 >= len(result) {
		return "", errors.New("数据量过少,请减少聚簇点数")
	}
	randArray := make([]int, num*256)
	rand.Seed(time.Now().Unix())
	copy(randArray, rand.Perm(len(result))[:num*256])
	pointer.vectors = NewFloatVectors()
	for _, index := range randArray {
		vector := NewFloatVector(length)
		vector.SetVector(result[index])
		pointer.vectors.Append(*vector)
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
	// 这个等会移到最后
	pointer.center = NewFloatVectors()
	// 生成随机数
	randArray := make([]int, num)
	rand.Seed(time.Now().Unix())
	copy(randArray, rand.Perm(vectors.len)[:num])
	// 随机选取num个聚簇点作为初始聚簇中心
	for _, index := range randArray {
		vector := NewFloatVector(length)
		vector.SetVector(vectors.vectors[index].vector)
		pointer.center.Append(*vector)
	}
	// 计算每个样本点最近的簇心点,迭代500次
	for i := 0; i < 500; i++ {
		// 遍历每个样本点，求离样本点最近的中心
		neighbor := make([]int, vectors.len)
		// 这里可以设计协程，用于计算每个路径的最近邻居
		var wg sync.WaitGroup
		for index, vector := range vectors.vectors {
			wg.Add(1)
			go func(index int, vector floatVector) {
				defer wg.Done()
				maxIndex, maxDistance := 0, -100000.0
				for centerIndex, centerPoint := range pointer.center.vectors {
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
		for j := 0; j < pointer.center.len; j++ {
			pointer.center.vectors[j].resetVector()
		}
		count := make([]int, num)
		// 此处两个函数添加sem并行
		for j, neigh := range neighbor {
			count[neigh]++
			pointer.center.vectors[neigh].addVector(vectors.vectors[j])
		}
		for j := 0; j < pointer.center.len; j++ {
			pointer.center.vectors[j].divVector(count[j])
		}
		fmt.Printf("聚类次数:%d", i)
	}

	return nil
}

// 储存索引并返回成功标志
func (pointer *Kmeans) storeIndex(dataPath string, length int, bucketPath string, num int) (bool, error) {
	if pointer.center == nil {
		return false, errors.New("聚类算法尚未运行")
	}
	// bucket 为桶，将每个向量储存到对应的桶中，
	// bucketIdentifier是存储编号的桶，因为每个向量有自己的编号，这样才能对应进行搜索。
	bucket := make([]floatVectors, num)
	bucketIdentifier := make([][]int, num)
	for i := range bucketIdentifier {
		bucketIdentifier[i] = make([]int, 0)
	}
	data, _ := loadData(dataPath, length)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i, floatData := range data {
		vector := NewFloatVector(length)
		vector.SetVector(floatData)
		maxIndex, maxDistance := 0, -100000.0
		// 这里可以增加并行操作
		wg.Add(1)
		go func(floatData []float64, i int) {
			defer wg.Done()
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
			defer mu.Unlock()
			if i%1000 == 0 {
				fmt.Printf("编号:%d运行完毕", i)
			}
		}(floatData, i)
	}
	wg.Wait()
	// 将每个聚簇点分桶存储
	os.Mkdir(bucketPath, os.ModePerm)
	for i, bucketVector := range bucket {
		fmt.Print("./" + bucketPath + "/" + strconv.Itoa(i) + ".txt")
		outputFile, outputError := os.OpenFile("./"+bucketPath+"/"+strconv.Itoa(i)+".txt",
			os.O_WRONLY|os.O_CREATE, 0666)
		if outputError != nil {
			fmt.Printf("An error occurred with file opening or creation\n")
			return false, nil
		}
		defer outputFile.Close()
		outputWriter := bufio.NewWriter(outputFile)
		for j := 0; j < bucketVector.len; j++ {
			outputWriter.WriteString(strconv.Itoa(bucketIdentifier[i][j]) + ":")
			outputWriter.WriteString(bucketVector.vectorString(j))
		}
		outputWriter.Flush()
	}
	// 存储中心点
	outputFile, outputError := os.OpenFile("./"+bucketPath+"/center.txt",
		os.O_WRONLY|os.O_CREATE, 0666)
	if outputError != nil {
		fmt.Printf("An error occurred with file opening or creation\n")
		return false, nil
	}
	defer outputFile.Close()
	outputWriter := bufio.NewWriter(outputFile)
	for j := 0; j < pointer.center.len; j++ {
		outputWriter.WriteString(strconv.Itoa(j) + ":")
		outputWriter.WriteString(pointer.center.vectorString(j))
	}
	outputWriter.Flush()
	return true, nil
}



// 调用查询函数查询与特征最接近的向量
func (pointer *Kmeans) searchVector(inputVector floatVector, root string) (int, floatVector, float64) {
	if _, err := os.Stat(root); os.IsNotExist(err) {
		fmt.Print("文件不存在")
	}
	pointer.root = root
	// 如果还没有聚簇点，那么加载聚簇点
	if pointer.center == nil {
		inputFile, inputError := os.Open(root + "/center.txt")
		if inputError != nil {
			fmt.Printf("An error occurred on opening the inputfile\n" +
				"Does the file exist?\n" +
				"Have you got acces to it?\n")
		}
		defer inputFile.Close()
		pointer.center = NewFloatVectors()
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
	inputFile, inputError := os.Open(root + "/" + strconv.Itoa(maxIndex) + ".txt")
	if inputError != nil {
		fmt.Printf("An error occurred on opening the inputfile\n" +
			"Does the file exist?\n" +
			"Have you got acces to it?\n")
	}
	defer inputFile.Close()
	inputReader := bufio.NewReader(inputFile)
	var wg sync.WaitGroup
	var mu sync.Mutex
	maxIndex, maxDistance = 0, -100000.0
	maxVector := NewFloatVector(1024)
	// 加载桶内每个向量与目标向量做匹配
	for {
		inputString, readerError := inputReader.ReadString('\n')
		if readerError == io.EOF {
			break
		}
		indexString := inputString[:strings.Index(inputString, ":")]
		index, _ := strconv.Atoi(indexString)
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