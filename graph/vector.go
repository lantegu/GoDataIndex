package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

// 浮点向量
type floatVector struct {
	vector []float64
	len    int
}

// 浮点向量求模
func (pointer *floatVector) GetModule() float64 {
	sum := 0.0
	for _, value := range pointer.vector {
		sum += math.Pow(value, 2)
	}
	return math.Sqrt(sum)
}

// 向量特征相加
func (pointer *floatVector) addVector(inputVector floatVector) {
	for i := 0; i < inputVector.len; i++ {
		pointer.vector[i] += inputVector.vector[i]
	}
}

// 向量特征除以某个数
func (pointer *floatVector) divVector(divisor int) error {
	if divisor == 0 {
		return errors.New("除数为零")
	}
	for i := 0; i < pointer.len; i++ {
		pointer.vector[i] /= float64(divisor)
	}
	return nil
}

// 向量特征重置
func (pointer *floatVector) resetVector() {
	for i := 0; i < pointer.len; i++ {
		pointer.vector[i] = 0
	}
}

// 浮点向量设置参数
func (pointer *floatVector) SetVector(inputVector []float64) error {
	if len(inputVector) != pointer.len {
		return errors.New("输入特征维度与初始化维度不匹配")
	}
	copy(pointer.vector, inputVector)
	return nil
}

// 求一个向量特征与另一个向量特征的距离
func (pointer *floatVector) distance(point floatVector) (float64, error) {
	if pointer.GetModule() == 0 || point.GetModule() == 0 {
		return 0, errors.New("存在模为0的向量")
	}
	if pointer.len != point.len {
		return 0, errors.New("向量特征维度不同")
	}
	var sum float64
	for i := 0; i < point.len; i++ {
		sum += pointer.vector[i] * point.vector[i]
	}
	return sum / (point.GetModule() * pointer.GetModule()), nil
}

// 将特征向量转化为String类型
func (pointer *floatVector) toString() string {
	var buffer bytes.Buffer
	for i := 0; i < pointer.len; i++ {
		buffer.WriteString(strconv.FormatFloat(pointer.vector[i], 'f', -1, 64))
		buffer.WriteString(",")
	}
	buffer.WriteString("\n")
	return buffer.String()
}

// NewFloatVector 用于向外生产一个向量
func NewFloatVector(dim int) *floatVector {
	vector := make([]float64, dim, dim)
	return &floatVector{vector: vector, len: dim}
}

// floatVectors 储存向量组
type floatVectors struct {
	vectors []floatVector
	len     int
}

// Append 往向量组内增加向量
func (pointer *floatVectors) Append(input floatVector) {
	pointer.vectors = append(pointer.vectors, input)
	pointer.len++
}

// 返回某个向量的string
func (pointer *floatVectors) vectorString(index int) string {
	vector := pointer.vectors[index]
	return vector.toString()
}

// NewFloatVectors 向外生产一个向量组
func NewFloatVectors() *floatVectors {
	vectors := make([]floatVector, 0)
	return &floatVectors{vectors: vectors}
}

// Index 是索引接口，展示索引所需要的功能
type Index interface {
	createIndex(path string) string
	storeIndex(path string) bool
	searchVector(vector floatVector) (int, floatVector)
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
		var vectorElement float64
		vector := make([]float64, length)
		inputString, readerError := inputReader.ReadString('\n')
		if readerError == io.EOF {
			break
		}
		inputString = inputString[:len(inputString)-1]
		stringSplit := strings.Split(inputString, ",")
		for i, element := range stringSplit {
			if i >= length {
				return nil, errors.New("vectors' dim error")
			}
			vectorElement, _ = strconv.ParseFloat(element, 64)
			vector[i] = vectorElement
		}
		vectors = append(vectors, vector)

	}
	return vectors, nil
}


