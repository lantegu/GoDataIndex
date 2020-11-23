package main

import (
	"errors"
	"math"
	"strconv"
)

// 浮点向量
type floatVector struct {
	vector []float64
	length int
}

// 浮点向量求模
func (pointer *floatVector) GetModule() float64 {
	sum := 0.0
	for _, value := range pointer.vector {
		sum += math.Pow(value, 2)
	}
	return math.Sqrt(sum)
}

// 对向量进行切片
func (pointer *floatVector) cutVector(length int, start int, end int) (result *floatVector, err error) {
	if length > pointer.length {
		return nil, errors.New("切片长度大于向量长度")
	}
	result = NewFloatVector(length)
	result.SetVector(pointer.vector[start:end])
	return result, nil
}

// 向量特征相加
func (pointer *floatVector) addVector(inputVector floatVector) {
	for i := 0; i < inputVector.length; i++ {
		pointer.vector[i] += inputVector.vector[i]
	}
}

// 向量特征相减
func (pointer *floatVector) subVector(inputVector floatVector) {
	for i := 0; i < inputVector.length; i++ {
		pointer.vector[i] -= inputVector.vector[i]
	}
}

// 向量特征除以某个数
func (pointer *floatVector) divNum(divisor int) error {
	if divisor == 0 {
		return errors.New("除数为零")
	}
	floatDivisor := float64(divisor)
	for i := 0; i < pointer.length; i++ {
		pointer.vector[i] /= floatDivisor
	}
	return nil
}

// 向量特征重置
func (pointer *floatVector) resetVector() {
	for i := 0; i < pointer.length; i++ {
		pointer.vector[i] = 0
	}
}

// 浮点向量设置参数
func (pointer *floatVector) SetVector(inputVector []float64) error {
	if len(inputVector) != pointer.length {
		return errors.New("输入特征维度与初始化维度不匹配")
	}
	copy(pointer.vector, inputVector)
	return nil
}

// 求一个向量特征与另一个向量特征的距离
func (pointer *floatVector) distance(pointInputVector floatVector) (float64, error) {
	if pointer.GetModule() == 0 || pointInputVector.GetModule() == 0 {
		return 0, errors.New("存在模为0的向量")
	}
	if pointer.length != pointInputVector.length {
		return 0, errors.New("向量特征维度不同")
	}
	var sum float64
	for i := 0; i < pointInputVector.length; i++ {
		sum += pointer.vector[i] * pointInputVector.vector[i]
	}
	return sum, nil
}

// 将特征向量转化为String类型
func (pointer *floatVector) toStrings() []string {
	strings := make([]string, pointer.length)
	for i := 0; i < pointer.length; i++ {
		strings[i] = strconv.FormatFloat(pointer.vector[i], 'f', -1, 64)
	}
	return strings
}

// NewFloatVector 用于向外生产一个向量
func NewFloatVector(dim int) *floatVector {
	vector := make([]float64, dim, dim)
	return &floatVector{vector: vector, length: dim}
}

// floatVectors 储存向量组
type floatVectors struct {
	vectors []floatVector
	length     int
}

// Append 往向量组内增加向量
func (pointer *floatVectors) Append(input floatVector) {
	pointer.vectors = append(pointer.vectors, input)
	pointer.length++
}

// subVector 对向量组每个向量进行减法
func (pointer *floatVectors) subVector(input floatVector) {
	for i := 0; i < pointer.length; i++ {
		pointer.vectors[i].subVector(input)
	}
}

// 返回某个向量的string
func (pointer *floatVectors) vectorString(index int) []string {
	vector := pointer.vectors[index]
	return vector.toStrings()
}

// 对整个floatVectors进行切片
func (pointer *floatVectors) cutVectors(length int, start int, end int) (result *floatVectors, err error) {
	result = NewFloatVectors()
	for _, vector := range pointer.vectors {
		temp, err := vector.cutVector(length, start, end)
		if err != nil {
			return nil, err
		}
		result.Append(*temp)
	}
	return result, nil
}

// NewFloatVectors 向外生产一个向量组
func NewFloatVectors() *floatVectors {
	vectors := make([]floatVector, 0)
	return &floatVectors{vectors: vectors}
}


