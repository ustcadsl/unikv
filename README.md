## UniKV: Toward High-Performance and Scalable KV Storage in Mixed Workloads via Unified Indexing

### Introduction
  Persistentkey-value(KV)storesaremainlydesigned basedontheLog-StructuredMerge-tree(LSM-tree),whichsuffer from large read and write ampliﬁcations, especially when KV stores grow in size. Existing design optimizations for LSMtree-based KV stores often make certain trade-offs and fail to simultaneously improve both the read and write performance on large KV stores without sacriﬁcing scan performance. We design UniKV, which uniﬁes the key design ideas of hash indexing and the LSM-tree in a single system. Speciﬁcally, UniKV leverages data locality to differentiate the indexing management of KV pairs. It also develops multiple techniques to tackle the issues caused by unifying the indexing techniques, so as to simultaneously improve the performance in reads, writes, and scans. Experiments show that UniKV signiﬁcantly outperforms several state-of-the-art KV stores (e.g., LevelDB, RocksDB, HyperLevelDB, and PebblesDB) in overall throughput under read-write mixed workloads.

### Publications
* Qiang Zhang, Yongkun Li, Patrick P. C. Lee, Yinlong Xu, Qiu Cui, Liu Tang. UniKV: Toward High-Performance and Scalable KV Storage in Mixed Workloads via Uniﬁed Indexing. IEEE ICDE 2020.

### Overview
The prototype is written in C++ based on [LevelDB](https://github.com/google/leveldb)
#### Minimal Requirement
Minimal setup to test the prototype:
* Ubuntu 14.04 LTS
* 4GB RAM

#### Test tool
generate workloads by YCSB-C, the C++ version of YCSB
* [YCSB-C](https://github.com/basicthinker/YCSB-C)

### Installation
On Ubuntu 14.04 LTS or above, install
* C++ compiler: g++ (version 4.8.4 or above)
* Snappy: libsnappy-dev

`$ sudo apt-get update`  
`$ sudo apt-get install g++ libsnappy-dev`

Getting the source code of UniKV  
`$ git clone https://github.com/ustcadsl/unikv.git`

Compile UniKV  
`$ cd UniKV`  
`$ make clean & make`

Install UniKV  
`$ ./buildLink.sh` 

### Testing the Prototype  
Getting the source code of YCSB-C modified by Qiang Zhang @USTC  
`$ git clone https://github.com/zhangqiangUSTC/YCSB-C.git`  

Build YCSB-C on Ubuntu  
`$ sudo apt-get install libtbb-dev`   
`$ cd YCSB-C`  
`$ make clean & make`  

As the driver for Redis is linked by default, change the runtime library path to include the hiredis library by:  
`$ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib`   

Run test script  
`$ ./run_KVTest.sh`  

** This project is released by the ADSLab in USTC.
