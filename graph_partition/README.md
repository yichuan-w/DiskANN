# Graph Partition Module

一个独立的图分割模块，用于对DiskANN索引进行图分割和重新布局。

## 功能特性

- ✅ **无TBB依赖** - 使用标准库替代，更轻量
- ✅ **独立构建** - 不依赖外部项目
- ✅ **简单配置** - 只需设置索引文件路径
- ✅ **快速编译** - 最小化依赖

## 快速开始

### 1. 配置索引文件路径

编辑 `build.sh` 中的 `INDEX_PREFIX_PATH`：

```bash
INDEX_PREFIX_PATH="/path/to/your/diskann/index"
```

### 2. 构建和运行

```bash
# 构建并运行图分割
./build.sh release split_graph
```

### 3. 输出文件

- `data/starling/_M_R_L_B/GRAPH/_disk_graph.index` - 重新布局的图索引
- `data/starling/_M_R_L_B/GRAPH/_partition.bin` - 分割信息文件

## 参数配置

在 `build.sh` 中可以调整以下参数：

```bash
GP_TIMES=10        # 图分割迭代次数
GP_LOCK_NUMS=10    # 锁定节点数量
GP_CUT=100         # 分割阈值
GP_SCALE_F=1       # 缩放因子
DATA_TYPE=float    # 数据类型
GP_T=10           # 线程数
```

## 依赖

- **OpenMP** - 并行计算
- **Boost::program_options** - 命令行参数解析
- **标准C++库** - 线程安全队列等

## 文件结构

```
graph_partition/
├── build.sh              # 独立构建脚本
├── CMakeLists.txt        # CMake配置
├── include/
│   ├── partitioner.h     # 主要头文件
│   └── freq_relayout.h   # 频率重新布局
├── src/
│   └── partitioner.cpp   # 主要实现
└── index_relayout.cpp    # 重新布局工具
```

## 集成到其他项目

1. 复制整个 `graph_partition` 目录
2. 修改 `build.sh` 中的 `INDEX_PREFIX_PATH`
3. 运行 `./build.sh release split_graph`

## 性能

- **编译时间**: ~1.4秒
- **图分割**: ~0.26秒
- **重新布局**: ~0.11秒
- **输出文件**: 303KB (图索引) + 18KB (分割信息)
