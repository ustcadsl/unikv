// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)
/*用来缓存的是sstable的索引数据，也可以理解为mysql中索引在内存中得缓存， 及通
                            常所说的元数据的缓存；index_block, bloom_fileter就放在table_cache,来快
                            速定位一个key是否在该table中；*/
//table_cache 通过leveldb.max_open_files 来设置，最大1000个table及1000个sstable文件；
#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include <string>
#include <stdint.h>
#include "db/dbformat.h"
#include "unikv/cache.h"
#include "unikv/table.h"
#include "port/port.h"
#include <fcntl.h>

//#define READ_SSTABLE_AHEAD

namespace leveldb {

class Env;

class TableCache {
 public:
  TableCache(const std::string& dbname, const Options* options, int entries);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-NULL, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or NULL if no Table object underlies
  // the returned iterator.  The returned "*tableptr" object is owned by
  // the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
/*  Iterator* NewIterator(const ReadOptions& options,
                        uint64_t file_number,
                        uint64_t file_size, IndexEntry* myHashIndex,long int* myReduceIOInComp,
                        Table** tableptr = NULL);*/
  
    Iterator* NewIterator(const ReadOptions& options,
                        uint64_t file_number,
                        uint64_t file_size,
			                  bool withFilter,bool isPrefetch,
                        Table** tableptr = NULL);
  
  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**,bool withFilter,bool isPrefetch);
  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**,unsigned long int* readDataSizeActual,uint64_t* tableCacheNum,bool isPrefetch);
  Status FindTableInL0(uint64_t file_number, uint64_t file_size,Cache::Handle** handle,unsigned long int* readDataSizeActual,uint64_t* tableCacheNum,bool isPrefetch);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options,
             uint64_t file_number,
             uint64_t file_size,
             const Slice& k,
             void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  Status Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,int level,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&),unsigned long int* readDataSizeActual, uint64_t*  tableCacheNum,uint64_t*  blockCacheNum,int* BFfilter) ;
  
  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

 private:
  Env* const env_;
  const std::string dbname_;
  const Options* options_;
  Cache* cache_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
