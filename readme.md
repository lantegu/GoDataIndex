1.1版本包括kmeans与ivfpq两个算法。
策略是将一个文件夹先进行分桶，利用csvdata储存每个桶，再进行并行查找。ivfpq在此基础上添加乘积量化极大压缩了索引的大小
但一定程度影响了距离衡量，目前仅能使用向量距离.（200万数据集下的索引速度约为0.03s)


