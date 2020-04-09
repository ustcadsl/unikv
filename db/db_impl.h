// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "unikv/db.h"
#include "unikv/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <fstream>
#include <sstream>
#include "db/version_set.h"
#include "db/b_tree.h"
#include <list>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <fcntl.h>
#include <signal.h>
#include <aio.h>
#include <mutex>
#include <sys/stat.h>
#include <sys/types.h>

using namespace std;

typedef unsigned char byte;

//#define LIST_HASH_INDEX
#define CUCKOO_HASH_INDEX
#define SIZE_BASED_MERGE
#define SPLIT_DURING_COMPACT

//#define READ_AHEAD
#define SEEK_PREFETCH
//#define NORMAL_COMPACT

#define SCAN_THREADS
//#define SCAN_AIO

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

static FILE* globalLogFile[config::kNumPartition*config::logFileNum];
//static FILE* globalLogFile[config::kNumPartition][config::logFileNum];

class DBImpl : public DB {
 public:
 
   struct recordIndexPara{  
    uint64_t number;
    Iterator* memIter;
  #ifdef LIST_HASH_INDEX
    ListIndexEntry* curHashIndex;
  #endif
  #ifdef CUCKOO_HASH_INDEX  
    ListIndexEntry* curHashIndex;
    //CuckooIndexEntry* curHashIndex;
  #endif
  }; 
  struct recordIndexPara recordIndex;
  
  struct upIndexPara{  
    uint64_t file_number;
    std::vector<FileMetaData*> *deleteFiles;
#ifdef LIST_HASH_INDEX   
    ListIndexEntry* curHashIndex;
#endif
#ifdef CUCKOO_HASH_INDEX
    ListIndexEntry* curHashIndex;
    //CuckooIndexEntry* curHashIndex;
#endif
  }; 
  struct upIndexPara updateIndex;

  struct getValuePara{  
      std::string dbname_;
      std::string valueStr;
      int beginLogNum;
  }; 
  
  float ampliFactor;
  int continueSplitCounts;
  FILE* currentLogFile=NULL;
  int LogNum[config::kNumPartition];
  int beginLogNum[config::kNumPartition];
  struct aiocb my_aiocb[config::maxScanLength];
  TreeNode* treeRoot;
  ofstream TreeFile;
  vector<uint64_t> logNumVec;
  vector<uint64_t> logImNumVec;
  HashFunc hashfunc;
  
#ifdef LIST_HASH_INDEX
  struct ListIndexEntry *ListHashIndex[config::kNumPartition];
#endif

#ifdef CUCKOO_HASH_INDEX
  //struct CuckooIndexEntry *CuckooHashIndex[config::kNumPartition];
  struct ListIndexEntry *CuckooHashIndex[config::kNumPartition];
#endif

  int NewestPartition=0;
  int NoPersistentFile=0;
  const FilterPolicy* filter_policy_half=NewBloomFilterPolicy(1);
  const FilterPolicy* filter_policy_add=NewBloomFilterPolicy(20);
 
  bool finishCompaction;
  bool bg_scanCompaction_scheduled_;
  bool doCompact;
  int compactPartition;
  uint64_t continueFlushBytes[config::kNumPartition];
  
  double totalLogTime,totalCacheTime,totalFlushTime,totalCompactTime;
  double totalReadMem,totalReadL0,totalReadLn,totalGetCostLn,totalReadOther;
  
  DBImpl(const Options& options, const std::string& dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Status rebuildHashIndex(const ReadOptions&);
  virtual void AnalysisTableKeys(const ReadOptions& options);
  virtual Iterator* NewIterator(const ReadOptions&);
  
  virtual void NewIterator(const ReadOptions& options, Iterator** iter );
  virtual Iterator* NewIterator(const ReadOptions& options, char* beginKey);

  virtual int NewIterator(const ReadOptions& options, char* beginKey,int scanLength);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end,int partition);
  
  virtual int MapCharKeyToPartition(Slice charKey);
  virtual void AnalysisHashTable(long int* emptyBucket, long int* totalEntry);
 
 uint64_t OutputFileSizeSet(int n,uint64_t valueSize){ 
   int choose=valueSize/900;
   if(choose < 16){
      return 67108864;
   }
   if(choose==16){
      return 33554432;
   }
   if(choose>30 && choose<=60){
      return 16777216;
   }
   if(choose>60){
      return 2097152;
   }
    return 2097152; 
 }

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);
  void persistentHashTable();
  void compactHashIndexTable();
  void compactHashIndexPartitionTable(int partition);
  void MergeUnsortedStoreFilesTogether(int partition);
  void recoveryHashTable();
  int recoveryB_Tree(TreeNode* Root);
  void persistentB_Tree();
  int seekValueParallel(Iterator* iter,int partition,char* beginKey,int scanLength);
  int scanAIOreads(Iterator* iter,int partition,char* beginKey,int scanLength);
  void initHashIndex();
  void initValueLogNumber();
  void initGlobalLogFile();
  static void* recordHashIndex(void *paraData);
  static void* parallelGetValue(void *paraData);
  
  void doBackgroundGC(int curPartition,int LogSegments);
  void doGCWithinPartition(int curPartition,int LogSegments);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles(int partition);
  void DeleteInvalidFiles(std::vector<FileMetaData*> *invalidFiles);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable(int partition) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  static bool containNewTable(uint64_t NewTableNum, std::vector<FileMetaData*> *myfiles);
  //void updateHashTable(std::vector<FileMetaData*> *deleteFiles);
  static void* updateHashTable(void *paraData);

  static void* updateBigHashTable(void *paraData);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base,int partition)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */,int partition)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction(int partition) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  void  BackgroundCompaction(int partition) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState* compact,int partition)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact,int partition,bool doSplit)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState* compact,int partition,bool doSplit,int direction);
  Status OpenCompactionOutputFileForScan(CompactionState* compact,int partition,bool doSplit,int direction);

  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input,int direction);
  Status InstallCompactionResults(CompactionState* compact,int partition,bool doSplit,int* moveToLevel)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status InstallSizeBasedCompactionResults(CompactionState* compact,int partition)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);    

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  
  
  const InternalFilterPolicy internal_filter_policy_half;
  const InternalFilterPolicy internal_filter_policy_add;

  Options options_; 
  bool owns_info_log_;
  bool owns_cache_;
  const std::string dbname_;
  std::string LogPrefixName;

  // table_cache_ provides its own synchronization
  TableCache* table_cache_;

  // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::Mutex splitMutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;          // Signalled when background work finishes
  //MemTable* mem_;//
  //MemTable* imm_;                // Memtable being compacted
  MemTable* pmem_[config::kNumPartition];//
  MemTable* pimm_[config::kNumPartition];     
  port::AtomicPointer has_imm_[config::kNumPartition];  // So bg thread can detect non-NULL imm_
  WritableFile* logfile_[config::kNumPartition];
  uint64_t logfile_number_[config::kNumPartition];
  uint64_t logfileImm_number_[config::kNumPartition];
  log::Writer* log_[config::kNumPartition];
  uint32_t seed_;                // For sampling.
  uint64_t memBaseSize=0;
  // Queue of writers.
  std::deque<Writer*> writers_;
  std::deque<Writer*> blockedWriters_;
  WriteBatch* tmp_batch_;

  SnapshotList snapshots_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_[config::kNumPartition];

  // Has a background compaction been scheduled or is running?
  bool bg_compaction_scheduled_;

  bool bg_gc_scheduled_;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;   // NULL means beginning of key range
    const InternalKey* end;     // NULL means end of key range
    InternalKey tmp_storage;    // Used to keep track of compaction progress
  };
  ManualCompaction* manual_compaction_;
  VersionSet* versions_;
  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;

    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) { }

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }
  };
  CompactionStats stats_[config::kNumPartition][config::kNumLevels+config::kTempLevel];

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db,
                               const InternalKeyComparator* icmp,
                               const InternalFilterPolicy* ipolicy,
                               const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_