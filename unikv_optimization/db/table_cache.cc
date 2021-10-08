// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "unikv/env.h"
#include "unikv/table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& /*key*/, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

TableCache::~TableCache() {
  delete cache_;
}


Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle, bool withFilter, bool isPrefetch) {
  Status s;
  RandomAccessFile* file = NULL;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);//

  if(isPrefetch){
     #ifdef READ_SSTABLE_AHEAD
     std::string fname = TableFileName(dbname_, file_number);
      int fileID;
      //PosixRandomAccessFile* file = NULL;
      Table* table = NULL;
      s = env_->NewRandomAccessFile(fname, &file,&fileID);//。                            
      //int ret=readahead(fileID,0,file_size); 
      int ret=posix_fadvise(fileID,0L,file_size, POSIX_FADV_WILLNEED);//POSIX_FADV_RANDOM | POSIX_FADV_WILLNEED
   //   printf("ret=%d,file_size=%lu\n", ret,file_size);
      if(ret == -1){
        printf("posix_fadvise failed!\n");
      }
      #endif
  }

  if (*handle == NULL) {//
    std::string fname = TableFileName(dbname_, file_number);
    int fileID;
    //RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file, &fileID);//                                          
    if (!s.ok()) {
      std::string old_fname = LDBTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      long unsigned int num;
      if(withFilter){
	      s = Table::Open(*options_, file, file_size, &table,&num);
      }else{
	      s = Table::OpenL0Table(*options_, file, file_size, &table,&num);//
      }
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {//
      TableAndFile* tf = new TableAndFile;//change to a Block
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key,tf, 1, &DeleteEntry);//handle point to the inserted handle
    }
  }
  return s;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle,unsigned long int* readDataSizeActual,uint64_t* tableCacheNum, bool isPrefetch) {
  Status s;
  RandomAccessFile* file = NULL;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);//
  
  if(isPrefetch){
     #ifdef READ_SSTABLE_AHEAD
     std::string fname = TableFileName(dbname_, file_number);
      int fileID;
      //PosixRandomAccessFile* file = NULL;
      Table* table = NULL;
      s = env_->NewRandomAccessFile(fname, &file,&fileID);//                            
      //#ifdef READ_AHEAD
      //int ret=readahead(fileID,0,file_size); 
      int ret=posix_fadvise(fileID,0L,file_size, POSIX_FADV_WILLNEED);//POSIX_FADV_RANDOM | POSIX_FADV_WILLNEED
      //printf("ret=%d,file_size=%lu\n", ret,file_size);
      if(ret == -1){
        printf("posix_fadvise failed!\n");
      }
      #endif
  }

  if (*handle == NULL) {//
    printf("L2 not in page cache,prefetch:%d！\n",isPrefetch);
    std::string fname = TableFileName(dbname_, file_number);
    //RandomAccessFile* file = NULL;
    int fileID;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file, &fileID);//                                            
    if (!s.ok()) {
      std::string old_fname = LDBTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table,readDataSizeActual);//
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {//
      TableAndFile* tf = new TableAndFile;//change to a Block
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key,tf, 1, &DeleteEntry);//handle point to the inserted handle
    }
  }else{
     *tableCacheNum=*tableCacheNum+1;
  }
  return s;
}


Status TableCache::FindTableInL0(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle,unsigned long int* readDataSizeActual,uint64_t* tableCacheNum, bool isPrefetch) {
  Status s;
  RandomAccessFile* file = NULL;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);//

  if(isPrefetch){
     #ifdef READ_SSTABLE_AHEAD
     std::string fname = TableFileName(dbname_, file_number);
      int fileID;
      //PosixRandomAccessFile* file = NULL;
      Table* table = NULL;
      s = env_->NewRandomAccessFile(fname, &file,&fileID);//                           
      //int ret=readahead(fileID,0,file_size); 
      int ret=posix_fadvise(fileID,0L,file_size, POSIX_FADV_WILLNEED);//POSIX_FADV_RANDOM | POSIX_FADV_WILLNEED
   //   printf("ret1=%d,file_size=%lu\n", ret,file_size);
      if(ret == -1){
        printf("posix_fadvise failed!\n");
      }
      #endif
  }

  if (*handle == NULL) {//
    //fprintf(stderr, "table %d in L0 doesn't cache Index_Part\n", (int)file_number);
    std::string fname = TableFileName(dbname_, file_number);
    //RandomAccessFile* file = NULL;
    int fileID;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file, &fileID);//                                          
    if (!s.ok()) {
      std::string old_fname = LDBTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::OpenL0Table(*options_, file, file_size, &table,readDataSizeActual);//
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {//
      TableAndFile* tf = new TableAndFile;///change to a Block
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);//handle point to the inserted handle
    }
  }else{
     *tableCacheNum=*tableCacheNum+1;
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,bool withFilter,bool isPrefetch,
                                  Table** tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }
  Cache::Handle* handle = NULL;
  Status s;
  if(isPrefetch){
      s = FindTable(file_number, file_size, &handle,withFilter,true);
  }else{
      s = FindTable(file_number, file_size, &handle,withFilter,false);
  }
  if (!s.ok()) {
    //fprintf(stderr,"before NewErrorIterator\n");
    return NewErrorIterator(s);
  }
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL){
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle,false, false);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle);
  }
  return s;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,int level,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&),unsigned long int* readDataSizeActual, uint64_t* tableCacheNum,uint64_t* blockCacheNum,int* BFfilter) {
  Cache::Handle* handle = NULL;
  Status s ;
  if(level==0){
    s= FindTableInL0(file_number, file_size, &handle,readDataSizeActual,tableCacheNum, false);//查找table，没有则新建table结构并插入table_cache
    if (s.ok()) {
        Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
        s = t->InternalGetL0Table(options, k, arg, saver,readDataSizeActual,blockCacheNum);//在table中查找/////////////////////////////////////////////
        cache_->Release(handle);
    }
  }
  else{
     s = FindTable(file_number, file_size, &handle,readDataSizeActual,tableCacheNum, false);//查找table，没有则新建table结构并插入table_cache
     if (s.ok()) {
        Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
        s = t->InternalGet(options, k, arg, saver,readDataSizeActual,blockCacheNum,BFfilter);//在table中查找/////////////////////////////////////////////
        cache_->Release(handle);
      }
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
