package main

import (
	"fmt"
	"math"
)

// hnswVectors hnsw算法的向量组，layer表示所在最高层数, index 表示表头编号
type hnswVector struct{
	layer int
	index int
	floatVector
}
// NewHnswVector 生产一个hnswvector
func NewHnswVector(layer int, index int, vector floatVector) *hnswVector{
	return &hnswVector{layer: layer, index: index, floatVector: vector}
}

type hnswVectors struct{
	vectors []hnswVector
	length     int
}

//Hnsw 算法, M为结点的度, ef 为动态表大小, ml为归一化因子,data表示存储这些结构的数据,graph是图的邻接表，
//第一维表示每个点，第二维表示某一层，第三维表示某一层的某一个邻接点
// 单元素都直接传向量本身，多元素就传索引数组[]int
type Hnsw struct{
	M int
	ef int
	L int
	ml float64
	ep hnswVector
	graph [][][]int
	data hnswVectors
}

func(pointer *Hnsw) createIndex(path string, length int) {
	floatData, err := loadData(path, length)
	if err != nil{
		fmt.Print(err)
	}
	for i, data := range(floatData){
		// 表示该数据层级
		// flag true表示 L大于pointer.L
		flag := false
		layer := int(math.Floor(-math.Log(getRandFloat64())*pointer.ml))
		if layer > pointer.L{
			pointer.L = layer
			flag = true
		}
		vector := NewFloatVector(length)
		vector.SetVector(data)
		q := NewHnswVector(layer, i, *vector)
		ep := pointer.ep
		for i := pointer.L; i >layer; i--{
			W := pointer.searchLayer(*q, ep, i)
			ep = pointer.selectNeigh(*q, W, pointer.M).vectors[0]
		}
		var neighbors hnswVectors
		for ;i >= 0; i--{
			W := pointer.searchLayer(*q, ep, i)
			neighbors = pointer.selectNeigh(*q, W, pointer.M)
			for _, e := range(neighbors.vectors){
				pointer.link(e, *q, i)
				pointer.prune(e, i)
			}
			ep = neighbors.vectors[0]
		}
		if flag == true{
			flag = false
			pointer.ep = *q
		}
	}
}

// 在某一层连接两个点
func(pointer *Hnsw) link(e hnswVector, q hnswVector, i int){

}

// 修剪某一层的点
func(pointer *Hnsw) prune(e hnswVector, i int){

}

// 在指定层查询K个最近邻节点。q表示待插入向量，ep表示该层起始节点,lc表示所在层级 
func(pointer *Hnsw) searchLayer(q hnswVector, ep hnswVector, lc int) (W []int){
	// v表示已访问点集, c 表示候选点集, w表示最近邻点集
	v := make([]int, 0)
	C := make([]int, 0)
	W = make([]int ,0)
	v, C , W = append(v, ep.index), append(C, ep.index), append(W, ep.index)
	
	return
}

// 选取出节点q在候选集C中的M个最近邻居
func(pointer *Hnsw) selectNeigh(q hnswVector, C [] int, M int) (W hnswVectors){
	return
}


// 存储索引
func(pointer *Hnsw) storeIndex() {

}

// 加载索引并进行查找
func(pointer *Hnsw) searchVector(){

}

