// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#define __STDC_LIMIT_MACROS

#include <deque>
#include <list>
#include <set>
#ifdef _LIBCPP_VERSION
#include <memory>
#else
#include <tr1/memory>
#endif
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/replay_iterator.h"
#include "db/snapshot.h"
#include "unikv/db.h"
#include "unikv/env.h"
#include "port/port.h"
#include "db/version_set.h"
#include "port/thread_annotations.h"
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <fstream>
#include <sstream>
#include "db/b_tree.h"
#include <list>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <fcntl.h>
#include <signal.h>
#include <aio.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <queue>
#include <shared_mutex>

using namespace std;

typedef unsigned char byte;

#define SCAN_IMPROVED_MERGE

//#define LIST_HASH_INDEX
#define CUCKOO_HASH_INDEX
#define SIZE_BASED_MERGE
#define SPLIT_DURING_COMPACT

//#define READ_AHEAD
#define SEEK_PREFETCH
//#define NORMAL_COMPACT

//#define SCAN_THREADS
#define SCAN_AIO

#define REQUESTPOOLSIZE 100000


namespace leveldb {
#ifdef _LIBCPP_VERSION
#define SHARED_PTR std::shared_ptr
#else
#define SHARED_PTR std::tr1::shared_ptr
#endif

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
static int finishUpdateHash;

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

  struct GCFilePara{  
    FILE* readLogfile;
    //FILE* writeLogfile;
    long offset;
    long length;
    int partition;
    int index;
    std::string* addressStr;
  }; 

  mutex queueLock;
  //struct GCFilePara gcFilePara;
  float ampliFactor;

  int updateIndexCount=0;
  int continueSplitCounts;

  double totalLogTime[config::kNumPartition],totalMemTableTime[config::kNumPartition],totalFlushTime[config::kNumPartition],totalCompactTime[config::kNumPartition],totalSplitTime,wCostTimeInQueue[config::kNumPartition];
  double totalReadMem[config::kNumPartition],totalReadL0[config::kNumPartition],totalReadLn[config::kNumPartition],totalGetCostLn,totalReadOther[config::kNumPartition],totalReadLockTime[config::kNumPartition];
  
  FILE* currentLogFile=NULL;
  int LogNum[config::kNumPartition];
  int beginLogNum[config::kNumPartition];
  struct aiocb my_aiocb[config::maxScanLength];
  TreeNode* treeRoot;
  ofstream TreeFile;
  vector<uint64_t> logNumVec;
  vector<uint64_t> logImNumVec;
  HashFunc hashfunc;
  mutable std::shared_mutex RWmutex_;
  
#ifdef LIST_HASH_INDEX
  struct ListIndexEntry *ListHashIndex[config::kNumPartition];
#endif

#ifdef CUCKOO_HASH_INDEX
  //struct CuckooIndexEntry *CuckooHashIndex[config::kNumPartition];
  struct ListIndexEntry *CuckooHashIndex[config::kNumPartition];
#endif

  int NewestPartition=0;
  int writeL0Number;
  int  NoPersistentFile=0;
  // Implementations of the DB interface
  
  int partitonUnsortedBuffered[config::kNumPartition];
  const FilterPolicy* filter_policy_half=NewBloomFilterPolicy(1);
  const FilterPolicy* filter_policy_add=NewBloomFilterPolicy(20);//40
 
  bool finishCompaction;
  bool doGC;
  bool duringCompaction;
  bool allCompaction;
  bool bg_scanCompaction_scheduled_;
  bool flushedMemTable[config::kNumPartition]={true};
  bool splitFinishInstall=true;
  ReadOptions Roptions;
  bool duringMergeMem;
  bool doCompact;
  bool bg_gc_scheduled_;
  
  long int totalKeyCount, totalKey;
  int compactPartition;
  uint64_t continueFlushBytes[config::kNumPartition];
  int keyLength;

  DBImpl(const Options& options, const std::string& dbname);
  virtual ~DBImpl();

   
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  //virtual Status InsertRequestQueue(const WriteOptions& options, const Slice& key, const Slice& value);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual Iterator* NewIterator(const ReadOptions& options, char* beginKey);
  virtual void NewIterator(const ReadOptions& options, Iterator** iter );
  virtual int NewIterator(const ReadOptions& options, char* beginKey, int scanLength);
  
  virtual void GetReplayTimestamp(std::string* timestamp);
  virtual void AllowGarbageCollectBeforeTimestamp(const std::string& timestamp);
  virtual bool ValidateTimestamp(const std::string& timestamp);
  virtual int CompareTimestamps(const std::string& lhs, const std::string& rhs);
  virtual Status GetReplayIterator(const std::string& timestamp,
                                   ReplayIterator** iter);
  virtual void ReleaseReplayIterator(ReplayIterator* iter);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end,int partition);
  virtual Status LiveBackup(const Slice& name);

   virtual int MapCharKeyToPartition(Slice charKey);
   virtual void sleepWrites();
   virtual Status rebuildHashIndex(const ReadOptions&);
  
  void AnalysisHashTable(long int* emptyBucket, long int* totalEntry);

  void PerformGet(ReadOptions& options, char *key, int partition);

  void persistentHashTable();
  void compactHashIndexTable();
  void compactHashIndexPartitionTable(int partition);
  void MergeUnsortedStoreFilesTogether(int partition);
  void recoveryHashTable();
  int recoveryB_Tree(TreeNode* Root);
  void persistentB_Tree();
  void AnalysisTableKeys(const ReadOptions& options);
  int seekValueParallel(Iterator* iter,int partition,char* beginKey,int scanLength);
  int scanAIOreads(Iterator* iter,int partition,char* beginKey,int scanLength);
  void initHashIndex();
  void initValueLogNumber();
  void initGlobalLogFile();
  void setUpWritePartitionThreads(int partition);
  
  static void* recordHashIndex(void *paraData);
  static void* updateHashTable(void *paraData);
  static void* updateBigHashTable(void *paraData);
  static void* parallelGetValue(void *paraData);

  static bool containNewTable(int NewTableNum, std::vector<FileMetaData*> *myfiles);

  void doBackgroundGC(int curPartition,int LogSegments);
  void doGCWithinPartition(int curPartition,int LogSegments);

uint64_t OutputFileSizeSet(int n,uint64_t valueSize){ 
   int choose=valueSize/900;
   if(choose < 16){
      return 67108864;//16777216
   }
   if(choose==16){///////
      return 33554432;
   }
   if(choose>30 && choose<=60){///////
      return 16777216;//268435456
   }
   if(choose>60){///////
      return 2097152;//268435456
   }
    return 2097152; 
 }

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(unsigned level, const Slice* begin, const Slice* end);

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

  // Peek at the last sequence;
  // REQURES: mutex_ not held
  SequenceNumber LastSequence();

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  Iterator* NewInternalIterator(const ReadOptions&, uint64_t number,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed, bool external_sync);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
 // void DeleteObsoleteFiles();
    void DeleteObsoleteFiles(int partition);
    void DeleteInvalidFiles(std::vector<FileMetaData*> *invalidFiles);

  // A background thread to compact the in-memory write buffer to disk.
  // Switches to a new log-file/memtable and writes a new descriptor iff
  // successful.
  static void CompactMemTableWrapper(void* db)
  { reinterpret_cast<DBImpl*>(db)->CompactMemTableThread(); }
  
  
  static void WritePartitionsWrapper0(void* db)
  { reinterpret_cast<DBImpl*>(db)->WritePartitionThread(0); }
  static void WritePartitionsWrapper1(void* db)
  { reinterpret_cast<DBImpl*>(db)->WritePartitionThread(1); }
  static void WritePartitionsWrapper2(void* db)
  { reinterpret_cast<DBImpl*>(db)->WritePartitionThread(2); }
  static void WritePartitionsWrapper3(void* db)
  { reinterpret_cast<DBImpl*>(db)->WritePartitionThread(3); }
  static void WritePartitionsWrapper4(void* db)
  { reinterpret_cast<DBImpl*>(db)->WritePartitionThread(4); }
  static void WritePartitionsWrapper5(void* db)
  { reinterpret_cast<DBImpl*>(db)->WritePartitionThread(5); }
  static void WritePartitionsWrapper6(void* db)
  { reinterpret_cast<DBImpl*>(db)->WritePartitionThread(6); }
  static void WritePartitionsWrapper7(void* db)
  { reinterpret_cast<DBImpl*>(db)->WritePartitionThread(7); }

  void WritePartitionThread(int partition);
  
  void CompactMemTableThread();


  Status RecoverLogFile(uint64_t log_number,
                        VersionEdit* edit,
                        SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void CompactMemTable(int partition) EXCLUSIVE_LOCKS_REQUIRED(mutex_);//

      
  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base, uint64_t* number,int partition)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status SequenceWriteBegin(Writer* w, WriteBatch* updates)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void SequenceWriteEnd(Writer* w)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // REQUIRES: writers_mutex_ not held
  void WaitOutWriters();

  static void CompactLevelWrapper(void* db)
  { reinterpret_cast<DBImpl*>(db)->CompactLevelThread(); }

  void CompactLevelThread();
  Status BackgroundCompaction(int partition) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const Status& s);

  void CleanupCompaction(CompactionState* compact,int partition)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact,int partition,bool doSplit)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  //Status OpenCompactionOutputFile(CompactionState* compact);
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

  const Options options_;  // options_.comparator == &internal_comparator_
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
  port::AtomicPointer shutting_down_;
  //port::AtomicPointer has_imm_;  // So bg thread can detect non-NULL imm_
   MemTable* pmem_[config::kNumPartition];///////////////////////////////////////////////////////////////////////////
  MemTable* pimm_[config::kNumPartition];     
  //MemTable* HotBuffermm_;    
  port::AtomicPointer has_imm_[config::kNumPartition];  // So bg thread can detect non-NULL imm_
  
  //WritableFile* logfile_[config::kNumPartition];
  SHARED_PTR<WritableFile> logfile_[config::kNumPartition];
  uint64_t logfile_number_[config::kNumPartition];
  uint64_t logfileImm_number_[config::kNumPartition];
  //log::Writer* log_[config::kNumPartition];
  SHARED_PTR<log::Writer> log_[config::kNumPartition];
  uint32_t seed_;                // For sampling.
  uint64_t memBaseSize=0;
  // Synchronize writers
  port::Mutex writers_mutex_;
  uint64_t writers_upper_;
  Writer* writers_tail_;
  SnapshotList snapshots_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_[config::kNumPartition];

  bool allow_background_activity_;
  bool levels_locked_[leveldb::config::kNumLevels];
  int num_bg_threads_;
  // Tell the foreground that background has done something of note
  port::CondVar bg_fg_cv_;
  // Communicate with compaction background thread
  port::CondVar bg_compaction_cv_;
  // Communicate with memtable->L0 background thread
  port::CondVar bg_memtable_cv_;
  // Mutual exlusion protecting the LogAndApply func
  port::CondVar bg_log_cv_;
  bool bg_log_occupied_;

  // Information for a manual compaction
  struct ManualCompaction {
    ManualCompaction()
      : level(),
        done(),
        begin(),
        end(),
        tmp_storage() {
    }
    unsigned level;
    bool done;
    const InternalKey* begin;   // NULL means beginning of key range
    const InternalKey* end;     // NULL means end of key range
    InternalKey tmp_storage;    // Used to keep track of compaction progress
   private:
    ManualCompaction(const ManualCompaction&);
    ManualCompaction& operator = (const ManualCompaction&);
  };
  ManualCompaction* manual_compaction_;

  // Where have we pinned tombstones?
  SequenceNumber manual_garbage_cutoff_;

  // replay iterators
  std::list<ReplayIteratorImpl*> replay_iters_;

  // how many reads have we done in a row, uninterrupted by writes
  uint64_t straight_reads_;

  VersionSet* versions_;

  // Information for ongoing backup processes
  port::CondVar backup_cv_;
  port::AtomicPointer backup_in_progress_; // non-NULL in progress
  uint64_t backup_waiters_; // how many threads waiting to backup
  bool backup_waiter_has_it_;
  bool backup_deferred_delete_; // DeleteObsoleteFiles delayed by backup; protect with mutex_

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
  CompactionStats stats_[config::kNumPartition][config::kNumLevels];

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
