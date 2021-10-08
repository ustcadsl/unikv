// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#define __STDC_LIMIT_MACROS

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/replay_iterator.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "unikv/db.h"
#include "unikv/env.h"
#include "unikv/replay_iterator.h"
#include "unikv/status.h"
#include "unikv/table.h"
#include "unikv/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include <iostream>
#include <mutex>  // For std::unique_lock
#include <shared_mutex>

using namespace std;

namespace leveldb {

const unsigned kStraightReads = 10;

const int kNumNonTableCacheFiles = 10;

struct requestEntry {
  int RWFlag;
  ReadOptions Roptions;
  WriteOptions Woptions;
  //std::string key;
  char key[50];
  //std::string* value;
  WriteBatch wBatch;
};

WriteBatch batch[config::kNumPartition];
queue<requestEntry*> requestBatchQueue[config::kNumPartition];
//queue<requestEntry*> requestPool;

// Information kept for every waiting writer
struct DBImpl::Writer {
  port::CondVar cv_;
  bool linked_;
  bool has_imm_;
  bool wake_me_when_head_;
  bool block_if_backup_in_progress_;
  int partition;
  Writer* prev_;
  Writer* next_;
  uint64_t micros_;
  uint64_t start_sequence_;
  uint64_t end_sequence_;
  //MemTable* mem_;
  MemTable* mem_[config::kNumPartition];
   
  SHARED_PTR<WritableFile> logfile_[config::kNumPartition];
  SHARED_PTR<log::Writer> log_[config::kNumPartition];

  explicit Writer(port::Mutex* mtx)
    : cv_(mtx),
      linked_(false),
      has_imm_(false),
      wake_me_when_head_(false),
      block_if_backup_in_progress_(true),
      prev_(NULL),
      next_(NULL),
      micros_(0),
      start_sequence_(0),
      end_sequence_(0){
      //mem_(NULL),
      //logfile_(),
      //log_() {
	
	for(int i=0;i<config::kNumPartition;i++){
	      mem_[i]=NULL;
        //logfile_[i].reset();
        //log_[i].reset();
	}
  }
  ~Writer() throw () {
    for(int i=0;i<config::kNumPartition;i++){
    // must do in order: log, logfile
    if (log_[i]) {
      assert(logfile_[i]);
      log_[i].reset();
      logfile_[i].reset();
    }
    }

    // safe because Unref is synchronized internally
    for(int i=0;i<config::kNumPartition;i++){
      if (mem_[i]) {
	      mem_[i]->Unref();
      }
    }
  }
 private:
  Writer(const Writer&);
  Writer& operator = (const Writer&);
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    Output() : number(), file_size(), smallest(), largest() {}
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  //std::vector<Output> outputs;
  std::vector<Output> outputs[2];

  // State kept for output being generated
  //WritableFile* outfile;
  //TableBuilder* builder;
  WritableFile* outfile[2];
  TableBuilder* builder[2];

  uint64_t total_bytes;

  //Output* current_output() { return &outputs[outputs.size()-1]; }
  Output* current_output(int i) { return &outputs[i][outputs[i].size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(),
        outputs(),
        //outfile(NULL),
        //builder(NULL),
        total_bytes(0) {
	  outfile[0]=NULL;
	  outfile[1]=NULL;
	  builder[0]=NULL;
	  builder[1]=NULL;
  }
 private:
  CompactionState(const CompactionState&);
  CompactionState& operator = (const CompactionState&);
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  result.write_buffer_size=67108864;//////////
  //result.max_file_size=2097152;//4194304;

  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
       internal_filter_policy_half(filter_policy_half),
     internal_filter_policy_add(filter_policy_add),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(),
      db_lock_(NULL),
      mutex_(),
      shutting_down_(NULL),
     // mem_(new MemTable(internal_comparator_)),
      //imm_(NULL),
      //has_imm_(),
      //logfile_(),
     // logfile_number_(0),
      //log_(),
      seed_(0),
      writers_mutex_(),
      writers_upper_(0),
      writers_tail_(NULL),
      snapshots_(),
      pending_outputs_(),
      allow_background_activity_(false),
      num_bg_threads_(0),
      bg_fg_cv_(&mutex_),
      bg_compaction_cv_(&mutex_),
      bg_memtable_cv_(&mutex_),
      bg_log_cv_(&mutex_),
      bg_log_occupied_(false),
      manual_compaction_(NULL),
      manual_garbage_cutoff_(raw_options.manual_garbage_collection ?
                             SequenceNumber(0) : kMaxSequenceNumber),
      replay_iters_(),
      straight_reads_(0),
      versions_(),
      backup_cv_(&writers_mutex_),
      backup_in_progress_(),
      backup_waiters_(0),
      backup_waiter_has_it_(false),
      backup_deferred_delete_(),
      bg_error_() {
  mutex_.Lock();
  //mem_->Ref();
  //has_imm_.Release_Store(NULL);
  //finishCompactMemTable=true;
  LogPrefixName=dbname_+"/" + "logfile-";
  printf("LogPrefixName:%s\n",LogPrefixName.c_str());

  backup_in_progress_.Release_Store(NULL);
  env_->StartThread(&DBImpl::CompactMemTableWrapper, this);
  env_->StartThread(&DBImpl::CompactLevelWrapper, this);

  num_bg_threads_ = 2;
  setUpWritePartitionThreads(0);
  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);
  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_);

  for(int i=0;i<config::kNumPartition;i++){
     LogNum[i]=0;
      beginLogNum[i]=0;
      continueFlushBytes[i]=0;
      //logfile_[i]=NULL;
      logfile_number_[i]=0;
      logfileImm_number_[i]=0;
      //log_[i]=NULL;
      pmem_[i]=NULL;
      pimm_[i]=NULL;
      has_imm_[i].Release_Store(NULL);
     partitonUnsortedBuffered[i]=0; 
     totalMemTableTime[i]=0;
     totalLogTime[i]=0,totalFlushTime[i]=0,totalCompactTime[i]=0,wCostTimeInQueue[i]=0;
     totalReadMem[i]=0,totalReadL0[i]=0,totalReadLn[i]=0,totalReadOther[i]=0;
     totalReadLockTime[i]=0;
     //toReadNum[i]=0;
  }

  for(int i=0;i<config::maxScanLength;i++){
    //Zero out the aiocb structure (recommended) 
    bzero( (char *)&my_aiocb[i], sizeof(struct aiocb) );
    //Allocate a data buffer for the aiocb request
    my_aiocb[i].aio_buf = malloc(config::maxValueSize);
    if (!my_aiocb[i].aio_buf) perror("malloc");
  }

  if(0 != access(config::B_TreeDir, 0)){
        // if this folder not exist, create a new one.
        mkdir(config::B_TreeDir, S_IRWXU);
  }
  if(0 != access(config::HashTableDir, 0)){
        // if this folder not exist, create a new one.
        mkdir(config::HashTableDir, S_IRWXU);
  }
  gValueSize=1000;
  gKeySize=24;

  logNumVec.clear();
  logImNumVec.clear();
  treeRoot = new TreeNode();
  treeRoot->setLeafFlag(1);
  compactPartition=0;
  NoPersistentFile=0;
  totalSplitTime=0;
  totalGetCostLn=0;
  readDataSizeActual=0;
  finishCompaction=true;
  ampliFactor = 1.0;
  continueSplitCounts =0;
  doCompact=false;
  bg_scanCompaction_scheduled_=false;

  NewestPartition=0;
  writeL0Number=0;
  keyLength=16;
  //flushInDoCompaction=false;
  allCompaction=true;
  duringMergeMem=false;
  doGC = false;
  duringCompaction = false;
  
  for (unsigned i = 0; i < leveldb::config::kNumLevels+config::kTempLevel; ++i) {
    levels_locked_[i] = false;
  }
  mutex_.Unlock();
  //writers_mutex_.Lock();
  //writers_mutex_.Unlock();
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  //shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  bg_memtable_cv_.SignalAll();
  fprintf(stderr,"after bg_memtable_cv_.SignalAll()\n");
  printf("after bg_memtable_cv_.SignalAll()\n");

  bg_compaction_cv_.SignalAll();
  fprintf(stderr,"after bg_compaction_cv_.SignalAll\n");
   printf("after bg_compaction_cv_.SignalAll\n");
  
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
   fprintf(stderr,"num_bg_threads_1=%d\n",num_bg_threads_);
  while (num_bg_threads_ > 0) {// || finishUpdateHash==0
    bg_fg_cv_.Wait();
  }
   //shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  fprintf(stderr,"num_bg_threads_2=%d\n",num_bg_threads_);
  Status s;
  printf("NewestPartition:%d\n",NewestPartition);
  fprintf(stderr,"NewestPartition:%d\n",NewestPartition);
  mutex_.Unlock();
  for(int i=0;i<=NewestPartition;i++){
      if(pimm_[i]!=NULL ){
	      CompactMemTable(i);
      }
     if(pmem_[i]!=NULL ){
      	pimm_[i] = pmem_[i];
	      has_imm_[i].Release_Store(pimm_[i]);
	      CompactMemTable(i); 
	      //printf("after CompactMemTable!!\n");
      }
  }
  //compactHashIndexTable();
  for(int i=0; i<=NewestPartition; i++){
    printf("P:%d, totalLogTime:%lf,totalFlushTime:%lf,totalMergeTime:%lf, wCostTimeInQueue:%lf, ",i,totalLogTime[i],totalFlushTime[i],totalCompactTime[i],wCostTimeInQueue[i]);
    printf("totalMemTableTime:%lf\n", totalMemTableTime[i]);
  }
  for(int i=0; i<=NewestPartition; i++){
    printf("P:%d,totalReadMem:%lf,totalReadL0:%lf,totalReadLn:%lf,totalReadLockTime:%lf,totalReadOther:%lf\n",i,totalReadMem[i],totalReadL0[i],totalReadLn[i],totalReadLockTime[i],totalReadOther[i]);
  }
  printf("totalCostGetLn:%lf\n",totalGetCostLn);
  printf("Get in Memtable:%d,IMemtable:%d\n",readMem,readImm);
  fprintf(stderr,"before persistentHashTable\n");
  persistentHashTable();/////////////////
  //mutex_.Unlock();
  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }
 // treeRoot->printfTree();
 for(int p=0;p<=NewestPartition;p++){
    for(int i=beginLogNum[p];i<LogNum[p];i++){
      int index=p*config::logFileNum+i;//-beginLogNum[p];
      //fprintf(stderr,"before close p:%d,index:%d\n",p,index);
      if(globalLogFile[index]!=NULL){
        fclose(globalLogFile[index]);
        globalLogFile[index]=NULL;
      }
    }
    beginLogNum[p]=0;
    LogNum[p]=0;
  }

  for(int i=0;i<=NewestPartition;i++){
    #ifdef LIST_HASH_INDEX
    for(int j=0;j<config::bucketNum;j++){
        ListIndexEntry* currIndexEntry=ListHashIndex[i][j].nextEntry;
        ListIndexEntry* PrvIndexEntry=&ListHashIndex[i][j];
        while(currIndexEntry!=NULL){
          PrvIndexEntry->nextEntry=currIndexEntry->nextEntry;
          delete currIndexEntry;
          //currIndexEntry=NULL;
          currIndexEntry=PrvIndexEntry->nextEntry;
        }
    }
    delete [] ListHashIndex[i];
    #endif

    #ifdef CUCKOO_HASH_INDEX
    for(int j=0;j<config::bucketNum;j++){
        ListIndexEntry* currIndexEntry=CuckooHashIndex[i][j].nextEntry;
        ListIndexEntry* PrvIndexEntry=&CuckooHashIndex[i][j];
        while(currIndexEntry!=NULL){
          PrvIndexEntry->nextEntry=currIndexEntry->nextEntry;
          delete currIndexEntry;
          //currIndexEntry=NULL;
          currIndexEntry=PrvIndexEntry->nextEntry;
        }
    }
    delete [] CuckooHashIndex[i];
    #endif   
  }

  persistentB_Tree();
  treeRoot->destroyTree();
  fprintf(stderr,"after persistentHashTable\n");
  delete filter_policy_half;
  delete filter_policy_add;
  delete versions_;
 // if (mem_ != NULL) mem_->Unref();
 // if (imm_ != NULL) imm_->Unref();
//  log_.reset();
//  logfile_.reset();
  for(int i=0;i<=NewestPartition;i++){
    log_[i].reset();
    logfile_[i].reset();

  }
  delete table_cache_;
  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
  
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0,0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  ConcurrentWritableFile* file;
  Status s = env_->NewConcurrentWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles(int partition) {
  // Defer if there's background activity
  mutex_.AssertHeld();
  if (backup_in_progress_.Acquire_Load() != NULL) {
    backup_deferred_delete_ = true;
    return;
  }

  // If you ever release mutex_ in this function, you'll need to do more work in
  // LiveBackup

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_[partition];
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber(partition)) ||
                  (number == versions_->PrevLogNumber(partition)));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
        default:
          keep = true;
          break;
      }

      //if((!keep && doCompact) || duringMergeMem){//&& type != kLogFile bg_compaction_scheduled_ && doCompact  && type != kLogFile
      if(doCompact && type != kLogFile){
         // if(versions_->containThisFile(number,partition)){
             keep=true;
        //  }
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        if(type == kLogFile){
            for(int k=0;k<logNumVec.size();k++){
              if(logNumVec[k]==number){
                keep=true;
                break;      
              }
            }
            for(int k=0;k<logImNumVec.size();k++){
              if(logImNumVec[k]==number){
                keep=true;
                break;	
              }
            }
	      }
        if (!keep) {
          Log(options_.info_log, "Delete type=%d #%lld\n",
              int(type),
              static_cast<unsigned long long>(number));
          env_->DeleteFile(dbname_ + "/" + filenames[i]);
        }
      }
    }
  }
}

void DBImpl::DeleteInvalidFiles(std::vector<FileMetaData*> *invalidFiles){
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }
  // Make a set of all of the live files
  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
     // bool keep = true;
      if(containNewTable(number,invalidFiles)){
        //fprintf(stderr,"--delete file:%d,filenames[i]:%s\n",number,filenames[i].c_str());
          table_cache_->Evict(number);//////////////////
          env_->DeleteFile(dbname_ + "/" + filenames[i]);/////////////////
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover();
  fprintf(stderr,"after versions_->Recover, s.ok:%d",s.ok());
  if (s.ok()) {
    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    uint64_t min_log = versions_->LogNumber(0);
    uint64_t prev_log = versions_->PrevLogNumber(0);

    for(int k=0;k<=NewestPartition;k++){//
      uint64_t logNum = versions_->LogNumber(k);
      if(logNum<min_log){
        min_log =logNum;
      }
      fprintf(stderr,"min_log:%d,partition:%d\n",min_log,k);
      uint64_t preLogNum = versions_->PrevLogNumber(k);
      if(preLogNum<prev_log){
        prev_log =preLogNum;
      }
       fprintf(stderr,"prev_log:%d,partition:%d\n",prev_log,k);
    }
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);
    fprintf(stderr,"filenames size:%d,partition:%d\n",filenames.size(),0);
    if (!s.ok()) {
      return s;
    }
    std::set<uint64_t> expected;
    versions_->AddLiveFiles(&expected);
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        expected.erase(number);
        if (type == kLogFile && ((number >= min_log) || (number == prev_log))){
          fprintf(stderr,"logs number:%u,partition:%d\n",number,0);
          printf("logs number:%u,partition:%d\n",number,0);
          logs.push_back(number);
        }
      }
    }
    if (!expected.empty()) {
      char buf[50];
      snprintf(buf, sizeof(buf), "%d missing files; e.g.",
               static_cast<int>(expected.size()));
      return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
    }

    // Recover in the order in which the logs were generated
    fprintf(stderr,"before RecoverLogFile,log.size:%d\n",logs.size());
    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++) {
      s = RecoverLogFile(logs[i], edit, &max_sequence);

      // The previous incarnation may not have written any MANIFEST
      // records after allocating this log number.  So we manually
      // update the file number allocation counter in VersionSet.
      versions_->MarkFileNumberUsed(logs[i]);
    }
     fprintf(stderr,"after RecoverLogFile\n");

    if (s.ok()) {
      if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
      }
    }
  }

  return s;
}

Status DBImpl::RecoverLogFile(uint64_t log_number,
                              VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    LogReporter()
      : env(),
        info_log(),
        fname(),
        status() {
    }
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
   private:
    LogReporter(const LogReporter&);
    LogReporter& operator = (const LogReporter&);
  };
  mutex_.AssertHeld();
  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }
  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentially make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
   int compactions = 0;
  MemTable* mem[config::kNumPartition]= {NULL};
  for(int i=0;i<=NewestPartition;i++){
    if (mem[i] == NULL) {
      mem[i] = new MemTable(internal_comparator_);
      mem[i]->Ref();
    }
  }
 // fprintf(stderr,"before ReadRecord\n");
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);
    Slice key, value;
    batch.GetKeyValue(&key, &value);
    int partition=MapCharKeyToPartition(key);
  
    if (mem[partition] == NULL) {
      mem[partition] = new MemTable(internal_comparator_);
      mem[partition]->Ref();
    }
    //fprintf(stderr,"key:%s, partition:%d\n", key.ToString().c_str(),partition);
    //status = WriteBatchInternal::InsertInto(&batch, mem[partition]);
    status = WriteBatchInternal::InsertIntoRecovery(&batch, mem, partition);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem[partition]->ApproximateMemoryUsage() > options_.write_buffer_size) {
      //fprintf(stderr,"before WriteLevel0Table\n");
      status = WriteLevel0Table(mem[partition], edit, NULL, NULL, partition);
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
      mem[partition]->Unref();
      mem[partition] = NULL;
    }
  }
  for(int i=0;i<=NewestPartition;i++){
      if (mem[i] != NULL) {
        // mem did not get reused; compact it.
        if (status.ok()) {
          //*save_manifest = true;
          //fprintf(stderr,"bedore WriteLevel0Table,partition:%d,NewestPartition:%d\n",i,NewestPartition);
          status = WriteLevel0Table(mem[i], edit, NULL,NULL,i);
            mem[i]->Unref();
            mem[i] = NULL;
        }
      }
  }
  delete file;
  return status;
}

void* DBImpl::recordHashIndex(void* paraData ){
   struct recordIndexPara *myPara=(struct recordIndexPara*)paraData;
   HashFunc curHashfunc;
#ifdef LIST_HASH_INDEX
    ListIndexEntry* curHashIndex=myPara->curHashIndex;
#endif

#ifdef CUCKOO_HASH_INDEX
    ListIndexEntry* curHashIndex=myPara->curHashIndex;
    //ListIndexEntry* overflowBucket=myPara->overflowBucket;
#endif
  byte* tableNumBytes=new byte[2];
  intTo2Byte(int(myPara->number),tableNumBytes);
  Iterator* iter=myPara->memIter;
  iter->SeekToFirst();
   for (; iter->Valid(); iter->Next()){
      Slice key = iter->key();    
      byte* keyBytes=new byte[4];
      unsigned int intKey;
      unsigned int hashKey;
      if(strlen(key.data())>20){
            intKey=strtoul(key.ToString().substr(4,config::kKeyLength).c_str(),NULL,10);
            hashKey=curHashfunc.RSHash((char*)key.ToString().substr(0,config::kKeyLength).c_str(),config::kKeyLength);
            //printf("myKey.size():%d,myKey.data():%s,intKey:%u\n",strlen(myKey.data()),myKey.ToString().substr(0,keyLength).c_str(),intKey);
      }else{
            intKey=strtoul(key.ToString().c_str(),NULL,10);
            hashKey=curHashfunc.RSHash((char*)key.ToString().c_str(),config::kKeyLength-8);
            //fprintf(stderr,"myKey.size():%d,myKey.data():%s,intKey:%d\n",(int)strlen(key.data()),key.data(),intKey);
      }	
      int keyBucket=intKey%config::bucketNum;
      intTo4Byte(hashKey,keyBytes);///
      //printf( "intKey:%u,hashKey:%u,bucket:%d,%s,keyBytes[2]:%u,keyBytes[3]:%u\n",intKey,hashKey,keyBucket,(char*)myKey.ToString().substr(0,config::kKeyLength).c_str(),(unsigned int )keyBytes[2],(unsigned int )keyBytes[3]);    
      //int mytableNum=bytes3ToInt(lastEntry->TableNum);
#ifdef LIST_HASH_INDEX
      ListIndexEntry *lastEntry=&curHashIndex[keyBucket];
      int mytableNum=bytes2ToInt(lastEntry->TableNum);
      if(mytableNum==0){
        lastEntry->KeyTag[0]=keyBytes[2];
        lastEntry->KeyTag[1]=keyBytes[3];     
        lastEntry->TableNum[0]=tableNumBytes[0];
        lastEntry->TableNum[1]=tableNumBytes[1];
        lastEntry->TableNum[2]=tableNumBytes[2];
      }else{
        ListIndexEntry *beginNextEntry=lastEntry->nextEntry;    
        ListIndexEntry *addEntry=new ListIndexEntry;      
        addEntry->KeyTag[0]=lastEntry->KeyTag[0];
        addEntry->KeyTag[1]=lastEntry->KeyTag[1];
        addEntry->TableNum[0]=lastEntry->TableNum[0];
        addEntry->TableNum[1]=lastEntry->TableNum[1];
        addEntry->TableNum[2]=lastEntry->TableNum[2];

        lastEntry->KeyTag[0]=keyBytes[2];
        lastEntry->KeyTag[1]=keyBytes[3];
        lastEntry->TableNum[0]=tableNumBytes[0];
        lastEntry->TableNum[1]=tableNumBytes[1];
        lastEntry->TableNum[2]=tableNumBytes[2];
        lastEntry->nextEntry=addEntry;
        addEntry->nextEntry=beginNextEntry;
      }
#endif

#ifdef CUCKOO_HASH_INDEX
      int findEmptyBucket=0;
        for(int k=0;k<config::cuckooHashNum;k++){
          keyBucket=curHashfunc.cuckooHash((char*)key.ToString().substr(0,config::kKeyLength).c_str(),k,config::kKeyLength);
         // fprintf(stderr,"keyBucket:%d\n",keyBucket);
          int mytableNum=bytes2ToInt(curHashIndex[keyBucket].TableNum);
          //if(curHashIndex[keyBucket].KeyTag[0]==0 && curHashIndex[keyBucket].KeyTag[1]==0){
          if(mytableNum==0){
            findEmptyBucket=1;
            break;
          }
        }
        if(findEmptyBucket){
          //fprintf(stderr,"add to cuckoo hash !!\n");
          curHashIndex[keyBucket].KeyTag[0]=keyBytes[2];
          curHashIndex[keyBucket].KeyTag[1]=keyBytes[3];     
          curHashIndex[keyBucket].TableNum[0]=tableNumBytes[0];
          curHashIndex[keyBucket].TableNum[1]=tableNumBytes[1];
          curHashIndex[keyBucket].TableNum[2]=tableNumBytes[2];
        }
        else{
          ///add to head
          ListIndexEntry *lastEntry=&curHashIndex[keyBucket];
          ListIndexEntry *beginNextEntry=lastEntry->nextEntry;    
          ListIndexEntry *addEntry=new ListIndexEntry;      
          addEntry->KeyTag[0]=lastEntry->KeyTag[0];
          addEntry->KeyTag[1]=lastEntry->KeyTag[1];
          addEntry->TableNum[0]=lastEntry->TableNum[0];
          addEntry->TableNum[1]=lastEntry->TableNum[1];
          addEntry->TableNum[2]=lastEntry->TableNum[2];

          lastEntry->KeyTag[0]=keyBytes[2];
          lastEntry->KeyTag[1]=keyBytes[3];
          lastEntry->TableNum[0]=tableNumBytes[0];
          lastEntry->TableNum[1]=tableNumBytes[1];
          lastEntry->TableNum[2]=tableNumBytes[2];
          lastEntry->nextEntry=addEntry;
          addEntry->nextEntry=beginNextEntry;
          ///add to end
          /*ListIndexEntry *lastEntry=&curHashIndex[keyBucket];
          while(lastEntry->nextEntry!=NULL){//
            lastEntry=lastEntry->nextEntry;
          }       
          ListIndexEntry *addEntry=new ListIndexEntry;      
          addEntry->KeyTag[0]=keyBytes[2];
          addEntry->KeyTag[1]=keyBytes[3];
          addEntry->TableNum[0]=tableNumBytes[0];
          addEntry->TableNum[1]=tableNumBytes[1];
          addEntry->TableNum[2]=tableNumBytes[2];
          addEntry->nextEntry=NULL;
          lastEntry->nextEntry=addEntry;*/ 
        }
#endif
      delete []keyBytes;   
    }
    delete []tableNumBytes;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base, uint64_t* number,int partition) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  if (number) {
    *number = meta.number;
  }
 // fprintf(stderr,"after  pending_outputs_[%d].insert,number:%d\n",partition,meta.number);
  pending_outputs_[partition].insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

mutex_.Unlock();
//////////////////////////////////////////////////////////
recordIndex.number=meta.number;
recordIndex.memIter=mem->NewIterator();
#ifdef LIST_HASH_INDEX
  recordIndex.curHashIndex=ListHashIndex[partition];
#endif

#ifdef CUCKOO_HASH_INDEX
  recordIndex.curHashIndex=CuckooHashIndex[partition];
#endif
  std::future<void*> ret1= Env::Default()->threadPool->addTask(DBImpl::recordHashIndex,(void *) &recordIndex);
  /////////////////////////////////////////////////////////////////
  Status s;
  {
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    edit->AddFile(level, meta.number, partition,meta.file_size,
                  meta.smallest, meta.largest);
  }
  try{
      ret1.wait();
  } catch(const std::future_error &e){ std::cerr<<"wait error\n"; }
  
  delete iter;
  delete recordIndex.memIter;
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  writeDataSizeActual=writeDataSizeActual+meta.file_size;////add
  writeDataSize+=meta.file_size;
  stats_[partition][level].Add(stats);
  return s;
}

void DBImpl::persistentHashTable(){
  ofstream hashFile;
  hashFile.open(config::HashTableFile);
#ifdef LIST_HASH_INDEX
  for(int p=0;p<=NewestPartition;p++){
    for(int i=0;i<config::bucketNum;i++){
          ListIndexEntry *lastEntry=&ListHashIndex[p][i];
          if(bytes2ToInt(lastEntry->TableNum)!=0){
            while(lastEntry!=NULL){
              hashFile<<p<<","<<i<<",";
                hashFile<<(unsigned int)lastEntry->KeyTag[0]<<","<<(unsigned int)lastEntry->KeyTag[1]<<",";
                hashFile<<bytes2ToInt(lastEntry->TableNum)<<";";	  
                lastEntry=lastEntry->nextEntry;     
            }
            hashFile<<"\n";
          }
          lastEntry=&ListHashIndex[p][i];
          if(bytes2ToInt(lastEntry->TableNum)==0 && lastEntry->nextEntry!=NULL){
            lastEntry=lastEntry->nextEntry;
            while(lastEntry!=NULL){
                hashFile<<p<<","<<i<<",";
                hashFile<<(unsigned int)lastEntry->KeyTag[0]<<","<<(unsigned int)lastEntry->KeyTag[1]<<",";
                hashFile<<bytes2ToInt(lastEntry->TableNum)<<";";
                lastEntry=lastEntry->nextEntry;            
            }
            hashFile<<"\n";
          }
    }
  }
#endif

#ifdef CUCKOO_HASH_INDEX
  for(int p=0;p<=NewestPartition;p++){
    for(int i=0;i<config::bucketNum;i++){
      ListIndexEntry *lastEntry=&CuckooHashIndex[p][i];
          if(bytes2ToInt(lastEntry->TableNum)!=0){
            while(lastEntry!=NULL){
              hashFile<<p<<","<<i<<",";
                hashFile<<(unsigned int)lastEntry->KeyTag[0]<<","<<(unsigned int)lastEntry->KeyTag[1]<<",";
                hashFile<<bytes2ToInt(lastEntry->TableNum)<<";";	  
                lastEntry=lastEntry->nextEntry;
            }
            hashFile<<"\n";
          }
          lastEntry=&CuckooHashIndex[p][i];
          if(bytes2ToInt(lastEntry->TableNum)==0 && lastEntry->nextEntry!=NULL){
            lastEntry=lastEntry->nextEntry;
            while(lastEntry!=NULL){
                hashFile<<p<<","<<i<<",";
                hashFile<<(unsigned int)lastEntry->KeyTag[0]<<","<<(unsigned int)lastEntry->KeyTag[1]<<",";
                hashFile<<bytes2ToInt(lastEntry->TableNum)<<";";
                lastEntry=lastEntry->nextEntry; 
            }
            hashFile<<"\n";
          }
    }
  }
#endif
  hashFile.close();
}

void DBImpl::recoveryHashTable(){
  ifstream hashFile;
  hashFile.open(config::HashTableFile);
  if (!hashFile){ 
      printf("open HashTableFile fail\n");     
      fprintf(stderr,"open HashTableFile fail\n");     
  }
    unsigned int partitionNumber=0,tag0,tag1,fileID,bucketNumber=0;
    char temp;
#ifdef LIST_HASH_INDEX
    while(hashFile>>partitionNumber){
            hashFile>>temp>>bucketNumber >> temp >> tag0 >> temp>>tag1>>temp>>fileID>>temp;   //读取的东西写入给变量
            //fprintf(stderr,"partitionNumber:%d,bucketNumber:%d,tag0:%d,tag1:%d,fileID:%d\n",partitionNumber,bucketNumber,tag0,tag1,fileID);
            byte* tableNumBytes=new byte[2];
            intTo2Byte(fileID,tableNumBytes);	//
            ListIndexEntry *lastEntry=&(ListHashIndex[partitionNumber][bucketNumber]);
            int mytableNum=bytes2ToInt(lastEntry->TableNum);
          if(mytableNum==0){
            //fprintf(stderr,"--partitionNumber:%d,bucketNumber:%d,tag0:%d,tag1:%d,fileID:%d\n",partitionNumber,bucketNumber,tag0,tag1,fileID);
            lastEntry->KeyTag[0]=(byte)tag0;
            lastEntry->KeyTag[1]=(byte)tag1;
            lastEntry->TableNum[0]=tableNumBytes[0];
            lastEntry->TableNum[1]=tableNumBytes[1];
            lastEntry->TableNum[2]=tableNumBytes[2];
            //printf("lastEntry bucket:%u,tag0:%u,tag1:%u,fileID:%u\n",bucketNumber,lastEntry->KeyTag[0],lastEntry->KeyTag[1],bytes3ToInt(lastEntry->TableNum));
          }else{
            ListIndexEntry *beginNextEntry=lastEntry->nextEntry;		
            ListIndexEntry *addEntry=new ListIndexEntry;
            addEntry->KeyTag[0]=(byte)tag0;
            addEntry->KeyTag[1]=(byte)tag1;
            addEntry->TableNum[0]=tableNumBytes[0];
            addEntry->TableNum[1]=tableNumBytes[1];
            addEntry->TableNum[2]=tableNumBytes[2];
            
            lastEntry->nextEntry=addEntry;
            addEntry->nextEntry=beginNextEntry;
            // printf("addEntry bucket:%u,tag0:%u,tag1:%u,fileID:%u\n",bucketNumber,addEntry->KeyTag[0],addEntry->KeyTag[1],bytes3ToInt(addEntry->TableNum));
          }
          delete []tableNumBytes;
        }
#endif

#ifdef CUCKOO_HASH_INDEX
    while(hashFile>>partitionNumber){
        hashFile>>temp>>bucketNumber >> temp >> tag0 >> temp>>tag1>>temp>>fileID>>temp;   //读取的东西写入给变量
            //fprintf(stderr,"partitionNumber:%d,bucketNumber:%d,tag0:%d,tag1:%d,fileID:%d\n",partitionNumber,bucketNumber,tag0,tag1,fileID);
            byte* tableNumBytes=new byte[2];
            intTo2Byte(fileID,tableNumBytes);	////////////////////////
            ListIndexEntry *lastEntry=&(CuckooHashIndex[partitionNumber][bucketNumber]);
            int mytableNum=bytes2ToInt(lastEntry->TableNum);
          if(mytableNum==0){
            //fprintf(stderr,"--partitionNumber:%d,bucketNumber:%d,tag0:%d,tag1:%d,fileID:%d\n",partitionNumber,bucketNumber,tag0,tag1,fileID);
            lastEntry->KeyTag[0]=(byte)tag0;
            lastEntry->KeyTag[1]=(byte)tag1;
            lastEntry->TableNum[0]=tableNumBytes[0];
            lastEntry->TableNum[1]=tableNumBytes[1];
            lastEntry->TableNum[2]=tableNumBytes[2];          
          }else{
            ListIndexEntry *beginNextEntry=lastEntry->nextEntry;		
            ListIndexEntry *addEntry=new ListIndexEntry;
            addEntry->KeyTag[0]=(byte)tag0;
            addEntry->KeyTag[1]=(byte)tag1;
            addEntry->TableNum[0]=tableNumBytes[0];
            addEntry->TableNum[1]=tableNumBytes[1];
            addEntry->TableNum[2]=tableNumBytes[2];
            
            lastEntry->nextEntry=addEntry;
            addEntry->nextEntry=beginNextEntry;
          }
          delete []tableNumBytes;
      } 
#endif
  hashFile.close();
}

void DBImpl::persistentB_Tree(){
  TreeFile.open(config::B_TreeFile);
  if(treeRoot->getLeftChild()!=NULL && treeRoot->getRightChild()!=NULL){
    treeRoot->persistentB_Tree(&TreeFile);
  }
  TreeFile.close();
}

int DBImpl::recoveryB_Tree(TreeNode* Root){//
  ifstream treeFile;
  int totalPartition=0,partition,addPartition=0;
  char k[3]="k:",p[3]="p:";
  string str,indexKey,tagChar,temp;
  treeFile.open(config::B_TreeFile);
  if (!treeFile){ 
      printf("open B_TreeFile fail\n");
  } 
    if(getline(treeFile,str)){
      tagChar=str.substr(0,1);
      indexKey=str.substr(2,str.length()-2);
       Root->setIndexCharKey((char*)indexKey.c_str());
       Root->setLeafFlag(0);   
    }   
     while(getline(treeFile,str)){
       int addFinish=0;      
	  tagChar=str.substr(0,2);
	  if(strcmp(tagChar.c_str(),k)==0){	   
	    indexKey=str.substr(2,str.length()-2);
	    TreeNode *addIndexNode=new TreeNode();	     
	      addIndexNode->setIndexCharKey((char*)indexKey.c_str());
	      addIndexNode->setLeafFlag(0);
	      Root->rebuildTree(addIndexNode,&addFinish);		 
	  }
	  if(strcmp(tagChar.c_str(),p)==0){	     
	      partition=atoi(str.substr(2,str.length()-2).c_str());
	      TreeNode *addLeafNode=new TreeNode();
	      addLeafNode->setPartition(partition);
	      addLeafNode->setLeafFlag(1);
	      Root->rebuildTree(addLeafNode,&addFinish);
	      totalPartition++;
	  }         
     }
     treeFile.close();
     return totalPartition;
}


void DBImpl::CompactMemTable(int partition) {//
  mutex_.AssertHeld();
  assert(pimm_[partition] != NULL);
  // Save the contents of the memtable as a new Table
  double flushBeginTime=Env::Default()->NowMicros();
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  uint64_t number;
  Status s = WriteLevel0Table(pimm_[partition], &edit, base, &number,partition);
  base->Unref(); base = NULL;

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()){
    edit.SetPrevLogNumber(logfileImm_number_[partition],partition);
    edit.SetLogNumber(logfile_number_[partition],partition);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_, &bg_log_cv_, &bg_log_occupied_,partition);
    fprintf(stderr,"after LogAndApply,number:%d\n",number);
  }

  for(int k=0;k<logImNumVec.size();k++){
	    if(logImNumVec[k]==logfileImm_number_[partition]){
        logImNumVec.erase(logImNumVec.begin()+k);
        k--;
	    }
	}

  pending_outputs_[partition].erase(number);
  partitonUnsortedBuffered[partition]++;
  if (s.ok()) {
    pimm_[partition]->Unref();
    pimm_[partition] = NULL;
    has_imm_[partition].Release_Store(NULL);
    //bg_fg_cv_.SignalAll();
    //bg_compaction_cv_.Signal();
    fprintf(stderr,"before DeleteObsoleteFiles,number:%d\n",number);
      //if(!duringMergeMem){
      //   DeleteObsoleteFiles(partition);
      //}
  } else {
    RecordBackgroundError(s);
  }
  double flushEndTime=Env::Default()->NowMicros();
  totalFlushTime[partition]+=flushEndTime-flushBeginTime;
}


void DBImpl::CompactMemTableThread() {
  MutexLock l(&mutex_);
  fprintf(stderr,"begin CompactMemTableThread\n");
  while (!shutting_down_.Acquire_Load() && !allow_background_activity_) {
    bg_memtable_cv_.Wait();
  }
  while (!shutting_down_.Acquire_Load()) {
    bool hasMemCompact=false;
    int partition=0;
    while (!shutting_down_.Acquire_Load()){// && !hasMemCompact) {
      for(int i=0;i<=NewestPartition;i++){
        //fprintf(stderr,"partitonUnsortedBuffered[%d]:%d,versions_->NumLevelFiles(0,i):%d,finishCompaction:%d\n", i, partitonUnsortedBuffered[i], versions_->NumLevelFiles(0,i), finishCompaction);
          if(!(pimm_[i]==NULL)){
            hasMemCompact=true;
            partition=i;
            break;
          }
      }
      if(hasMemCompact){// && !duringMergeMem){   
	        break;
      }
      bg_memtable_cv_.Wait();
    }
    if (shutting_down_.Acquire_Load()) {
      break;
    }
    
    duringMergeMem=true;
    // Save the contents of the memtable as a new Table
    double flushBeginTime=Env::Default()->NowMicros();
    VersionEdit edit;
    Version* base = versions_->current();
    base->Ref();
    uint64_t number;
    Status s = WriteLevel0Table(pimm_[partition], &edit, base, &number,partition);
    //fprintf(stderr,"after WriteLevel0Table,number:%d\n",number);
    base->Unref(); base = NULL;

    if (s.ok() && shutting_down_.Acquire_Load()) {
      s = Status::IOError("Deleting DB during memtable compaction");
    }

    // Replace immutable memtable with the generated Table
    if (s.ok()) {
      edit.SetPrevLogNumber(logfileImm_number_[partition],partition);
      edit.SetLogNumber(logfile_number_[partition],partition);  // Earlier logs no longer needed
      s = versions_->LogAndApply(&edit, &mutex_, &bg_log_cv_, &bg_log_occupied_,partition);
    }

     for(int k=0;k<logImNumVec.size();k++){
	      if(logImNumVec[k]==logfileImm_number_[partition]){
          logImNumVec.erase(logImNumVec.begin()+k);
          k--;
	      }
	  }

    pending_outputs_[partition].erase(number);
    partitonUnsortedBuffered[partition]++;
    if (s.ok()) {
      pimm_[partition]->Unref();
      pimm_[partition] = NULL;
      has_imm_[partition].Release_Store(NULL);
      flushedMemTable[partition] = true;

      bg_fg_cv_.SignalAll();
      bg_memtable_cv_.Signal();
      bg_compaction_cv_.Signal();
      if(!doCompact){
        DeleteObsoleteFiles(partition);
      }
      duringMergeMem=false;
    } else {
      RecordBackgroundError(s);
      continue;
    }
    double flushEndTime=Env::Default()->NowMicros();
    totalFlushTime[partition]+=flushEndTime-flushBeginTime;
    assert(config::kL0_SlowdownWritesTrigger > 0);
  }//////////////////////////////////////////////////////////////////////
  Log(options_.info_log, "cleaning up CompactMemTableThread");
  num_bg_threads_ -= 1;
  bg_fg_cv_.SignalAll();
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end,int partition) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (unsigned level = 1; level < config::kNumLevels+config::kTempLevel; level++) {
      if (base->OverlapInLevel(level, begin, end,partition)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(unsigned level, const Slice* begin,const Slice* end) {
  assert(level + 1 < config::kNumLevels+config::kTempLevel);

  InternalKey begin_storage, end_storage;
  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;
      bg_compaction_cv_.Signal();
      bg_memtable_cv_.Signal();
    } else {  // Running either my compaction or another compaction.
      bg_fg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (pimm_[0] != NULL && bg_error_.ok()) {
      bg_fg_cv_.Wait();
    }
    if (pimm_[0] != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::CompactLevelThread() {
  fprintf(stderr,"### in CompactLevelThread\n");
  MutexLock l(&mutex_);
  while (!shutting_down_.Acquire_Load() && !allow_background_activity_) {
    // fprintf(stderr,"in CompactLevelThread bg_compaction_cv_.Wait()-1\n");
    bg_compaction_cv_.Wait();
  }
  while (!shutting_down_.Acquire_Load()) {
    bool hasPartitionNeedCompact=false;
    int partition=0;
    fprintf(stderr,"in CompactLevelThread,hasPartitionNeedCompact:%d, partition:%d**********\n",hasPartitionNeedCompact,partition);
    while (!shutting_down_.Acquire_Load() &&
           manual_compaction_ == NULL &&
           !hasPartitionNeedCompact){
      for(int k=0;k<=NewestPartition;k++){
          if(versions_->NeedsCompaction(k) && pimm_[partition] == NULL){
            partition=k;
            hasPartitionNeedCompact=true;
            break;
          }
      }
      if(hasPartitionNeedCompact && finishCompaction && !duringMergeMem  && flushedMemTable[partition]){// && !duringMergeMem){//&& allCompaction      
            break;
      }
      bg_compaction_cv_.Wait();
    }
    if (shutting_down_.Acquire_Load()) {
        break;
    }
    assert(manual_compaction_ == NULL || num_bg_threads_ == 2);
    Status s = BackgroundCompaction(partition);
    fprintf(stderr,"after BackgroundCompaction,partition:%d\n",partition);
    bg_fg_cv_.SignalAll(); // before the backoff In case a waiter
                           // can proceed despite the error
    if (s.ok()) {
      // Success
    } else if (shutting_down_.Acquire_Load()) {
      // Error most likely due to shutdown; do not wait
    } else {
      // Wait a little bit before retrying background compaction in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed compactions for the duration of
      // the problem.
      Log(options_.info_log, "Waiting after background compaction error: %s",
          s.ToString().c_str());
      fprintf(stderr,"Waiting after background compaction error\n");
      mutex_.Unlock();
      int seconds_to_sleep = 1;
      env_->SleepForMicroseconds(seconds_to_sleep * 1000000);
      mutex_.Lock();
    }
  }
  Log(options_.info_log, "cleaning up CompactLevelThread");
  num_bg_threads_ -= 1;
  bg_fg_cv_.SignalAll();
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_fg_cv_.SignalAll();
  }
}

Status DBImpl::BackgroundCompaction(int partition) {
  mutex_.AssertHeld();
  Status status;
  bool doSplit=false;
  fprintf(stderr,"in BackgroundCall,partition:%d\n",partition);
  if(versions_->NumLevelFiles(0,partition) < config::kL0_CompactionTrigger || bg_scanCompaction_scheduled_){// && NumCachedLevel(partition)<config::kNumLevels+config::kTempLevel-3){
	    fprintf(stderr,"In !versions_->NeedsCompaction(partition)-2!\n");
	    return status;
  }
  //fprintf(stderr,"--in BackgroundCall-2,partition:%d\n",partition);
  doCompact=true;
  uint64_t curPartitionSize=versions_->TotalPartitionBytes(partition,gKeySize,gValueSize);
  continueFlushBytes[partition]=continueFlushBytes[partition]+versions_->flushL0Bytes(partition);
  //fprintf(stderr,"curPartitionSize:%llu\n",curPartitionSize);
  uint64_t splitLimit=config::kSplitBytes;
  if(gValueSize>16000){// || gValueSize<500
    if(gValueSize>64000){
      splitLimit+=26106127360;
    }else{
    splitLimit+=20106127360;//16106127360
    }
  }
  if(continueSplitCounts>4){//3//5
    splitLimit+=25737418240;//20737418240
    printf("add splitLimit to:%llu\n",splitLimit);
  }
  printf("splitLimit:%llu, curPartitionSize:%llu,continueFlushBytes:%llu, continueSplitCounts:%d\n",splitLimit,curPartitionSize,continueFlushBytes[partition],continueSplitCounts);
  fprintf(stderr, "splitLimit:%llu, curPartitionSize:%llu,continueFlushBytes:%llu, continueSplitCounts:%d\n",splitLimit,curPartitionSize,continueFlushBytes[partition],continueSplitCounts);
 
  if(curPartitionSize>=splitLimit){
      doSplit=true;
      NewestPartition++;
      finishCompaction=false;
      while(requestBatchQueue[partition].size()>0){
          mutex_.Unlock();
          env_->SleepForMicroseconds(1);
          mutex_.Lock();
      }
      bg_memtable_cv_.Signal();
      
      if(pimm_[partition] != NULL){
          bg_memtable_cv_.Signal();
          fprintf(stderr, "----wait for CompactMemTable,partition:%d\n",partition);
          bg_fg_cv_.Wait();
      }
      printf("doSplit=true, pmem_[partition]!=NULL,curPartitionSize:%llu\n",curPartitionSize);
      fprintf(stderr, "doSplit=true, pmem_[partition]!=NULL,curPartitionSize:%llu\n",curPartitionSize);
      
      if(pmem_[partition]!=NULL){
          uint64_t new_log_number = versions_->NewFileNumber();
          ConcurrentWritableFile* lfile = NULL;
          status= env_->NewConcurrentWritableFile(LogFileName(dbname_, new_log_number), &lfile);
          if (!status.ok()) {
            // Avoid chewing through file number space in a tight loop.
            versions_->ReuseFileNumber(new_log_number);        
          }
          for(int k=0;k<logNumVec.size();k++){
              if(logNumVec[k]==logfile_number_[partition]){
                logNumVec.erase(logNumVec.begin()+k);
                k--;
              }
          }
          logImNumVec.push_back(logfile_number_[partition]);
          logfileImm_number_[partition]=logfile_number_[partition];
          printf("split,buid new logfile_number_:%u,partition:%d\n",new_log_number,partition);
          fprintf(stderr, "split,buid new logfile_number_:%u,partition:%d\n",new_log_number,partition);
          logfile_ [partition].reset(lfile);//

          logfile_number_[partition]= new_log_number;	  
          logNumVec.push_back(new_log_number);
          log_[partition].reset(new log::Writer(lfile));//new logfile
          pimm_[partition] = pmem_[partition];// 
          //has_imm_[partition].Release_Store(pimm_[partition]);
          pmem_[partition] = new MemTable(internal_comparator_);///
          pmem_[partition]->Ref();
          bg_memtable_cv_.Signal();
          //CompactMemTable(partition); 
          while(pimm_[partition] != NULL){
            mutex_.Unlock();
            env_->SleepForMicroseconds(100);
            mutex_.Lock();
          }
          printf("doSplit=true, pmem_[partition]!=NULL,curPartitionSize:%llu\n",curPartitionSize);
          fprintf(stderr, "--doSplit=true, pmem_[partition]!=NULL,curPartitionSize:%llu\n",curPartitionSize);
      }
    }
  ////////////////////////////////////////
  Compaction* c = NULL;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end,partition);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {

      c = versions_->PickAllCompaction(partition);
  }
    
  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove() && c->level() > 0) {
    // Move file to next level
    fprintf(stderr,"Move file to next level\n");
    for (size_t i = 0; i < c->num_input_files(0); ++i) {
      FileMetaData* f = c->input(0, i);
      c->edit()->DeleteFile(c->level(), partition,f->number);
      c->edit()->AddFile(c->level() + 1, f->number, f->partition,f->file_size,
                         f->smallest, f->largest);
    }
    status = versions_->LogAndApply(c->edit(), &mutex_, &bg_log_cv_, &bg_log_occupied_,partition);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    for (size_t i = 0; i < c->num_input_files(0); ++i) {
      FileMetaData* f = c->input(0, i);
      Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
          static_cast<unsigned long long>(f->number),
          c->level() + 1,
          static_cast<unsigned long long>(f->file_size),
          status.ToString().c_str(),
          versions_->LevelSummary(&tmp));
    }
  } else {
    duringCompaction = true;
    partitonUnsortedBuffered[partition]=0;
    CompactionState* compact = new CompactionState(c);
    fprintf(stderr, "before DoCompactionWork NumFiles L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),versions_->NumLevelFiles(config::kNumLevels+config::kTempLevel-1,partition));   
    printf("before DoCompactionWork NumFiles L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),versions_->NumLevelFiles(config::kNumLevels+config::kTempLevel-1,partition));
    //doCompact=true;
    status = DoCompactionWork(compact,partition,doSplit);////////////////////////////////////////
    DeleteInvalidFiles(compact->compaction->GetAllInput());
    LogNum[partition]++;
    fprintf(stderr, "***after DoCompactionWork!!!!\n");//
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact,partition);
    c->ReleaseInputs();
    //DeleteObsoleteFiles(partition);
    //doCompact=false;
  }

  if (c) {
    levels_locked_[c->level() + 0] = false;
    levels_locked_[c->level() + 1] = false;
    delete c;
  }
  //trigger GC in partition after split it
    if(continueFlushBytes[partition]>=config::kcontinueWriteBytes && curPartitionSize>=config::kGCBytes && !doSplit && config::kGCBytes!=config::kSplitBytes){
        bg_gc_scheduled_=true;
        finishCompaction=true;
        bg_fg_cv_.SignalAll();
        printf("before doGCWithinPartition NumFiles L0:%d ,L1:%d,beginLogNum:%d,LogNum:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),beginLogNum[partition],LogNum[partition]);
        doGCWithinPartition(partition,LogNum[partition]);
        beginLogNum[partition]=LogNum[partition]-1;
        LogNum[partition]++;
        printf("after doBackgroundGC NumFiles L0:%d ,L1:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition));
        //fprintf(stderr,"after doBackgroundGC NumFiles L0:%d ,L1:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition));
        bg_gc_scheduled_=false;
        continueFlushBytes[partition]=0;
    }
    
    if(doSplit){
        bg_gc_scheduled_=true;
        finishCompaction=true;
        bg_fg_cv_.SignalAll();
        bg_memtable_cv_.Signal();//
        printf("before doBackgroundGC NumFiles L0:%d ,L1:%d,beginLogNum:%d,LogNum:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),beginLogNum[partition],LogNum[partition]);
        fprintf(stderr,"before doBackgroundGC NumFiles L0:%d ,L1:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition));        
        doBackgroundGC(partition,LogNum[partition]);
        beginLogNum[partition]=LogNum[partition]-1;
        LogNum[partition]++;
        LogNum[NewestPartition]++;
        //DeleteObsoleteFiles(partition);
        fprintf(stderr, "after doBackgroundGC NumFiles L0:%d ,L1:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition));
        printf("after doBackgroundGC NumFiles L0:%d ,L1:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition));
        bg_gc_scheduled_=false;
        continueSplitCounts++;
    }
    //bg_fg_cv_.SignalAll();
    doCompact=false;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }
   //allCompaction=true;
  return status;
}

void DBImpl::doBackgroundGC(int curPartition,int LogSegments){
    doGC = true;
    double compactBeginTime=Env::Default()->NowMicros();
    FILE* readLogfile[LogSegments];
    for(int i=0;i<LogSegments;i++){
        readLogfile[i]=NULL;
    }
    Status status;
    std::future<void*> ret[config::maxThreadNum];
    int64_t writeLogValue=0;
    int partition[2];
    partition[0]=curPartition;
    partition[1]=NewestPartition;
    //fprintf(stderr,"curPartition=%d,beginLogNum=%d\n",curPartition,beginLogNum[curPartition]);
    for(int i=beginLogNum[curPartition];i<LogSegments-1 || i<1;i++){
        std::stringstream readLogName;
        readLogName<< LogPrefixName <<curPartition<<"-"<<i;      
        std::string name=readLogName.str();
        //readLogfile[i]= fopen(name.c_str(),"rb+");
        int index=curPartition*config::logFileNum+i;//-beginLogNum[curPartition];
        readLogfile[i]=globalLogFile[index];
        //readLogfile[i]=globalLogFile[curPartition][i];
        if( readLogfile[i]==NULL){
            fprintf(stderr,"curPartition:%d,myLogfile:%s error,index:%d!!\n",curPartition,name.c_str(),index);
            printf("curPartition:%d,myLogfile:%s error!!\n",curPartition,name.c_str());
        }
        printf("curPartition:%d,open Logfile:%s GC!!\n",curPartition,name.c_str());
        #ifdef READ_AHEAD
          int fileID=fileno(readLogfile[i]);
          //int ret=readahead(fileID,0,file_size);  config::file_size
          uint64_t prefetchSize=3221225472; //3GB
          int ret=posix_fadvise(fileID,0L,prefetchSize, POSIX_FADV_WILLNEED);//POSIX_FADV_RANDOM | POSIX_FADV_WILLNEED
          printf("GC log file, ret=%d\n", ret);
          if(ret == -1){
            printf("posix_fadvise调用失败！\n");
          }
        #endif
        //fclose(readLogfile[i]);  
    }
    //fprintf(stderr,"LogNum[%d]=%d,LogNum[%d]=%d,LogSegments:%d\n",curPartition,LogNum[curPartition],NewestPartition,LogNum[NewestPartition],LogSegments);
    printf("LogNum[%d]=%d,LogNum[%d]=%d,LogSegments:%d\n",curPartition,LogNum[curPartition],NewestPartition,LogNum[NewestPartition],LogSegments);
//mutex_.Unlock();
    for(int i=0;i<2;i++){   
        std::stringstream LogName;
        LogName<< LogPrefixName <<partition[i]<<"-"<<LogNum[partition[i]];
        std::string name=LogName.str();
        int index=partition[i]*config::logFileNum+LogNum[partition[i]];//-beginLogNum[partition[i]];
        globalLogFile[index]=fopen(name.c_str(),"ab+");
        //globalLogFile[partition[i]][LogNum[partition[i]]]=fopen(name.c_str(),"ab+");
        if(globalLogFile[index]==NULL){     
          printf("Unable to open file:%s!\n",name.c_str());
          exit(0);
        }
        Compaction* c;
        c = versions_->PickSortedStoreForGC(partition[i]);///
        CompactionState* compact = new CompactionState(c);
        printf("c-input.size:%d\n",compact->compaction->num_input_files(1));
        mutex_.Unlock();
        Iterator* SortedStoreIter = versions_->MakeGCterator(c);
        for(SortedStoreIter->SeekToFirst();SortedStoreIter->Valid();){
            Slice key = SortedStoreIter->key();
            std::string addressStr;
            if(compact->builder[0] == NULL){
                status = OpenCompactionOutputFile(compact,partition[i],false,0);
                if (!status.ok()) {
                  fprintf(stderr,"OpenCompactionOutputFile failed,break!!\n");
                  break;
                }
            }
            if(compact->builder[0]->NumEntries()==0){
                compact->current_output(0)->smallest.DecodeFrom(key);
            }
            compact->current_output(0)->largest.DecodeFrom(key);   
            std::string delimiter = "&";
            size_t pos = 0;
            long length,offset,logPartition,logNumber;           
            std::string addressValue=SortedStoreIter->value().ToString();
            pos=addressValue.find(delimiter);
            logPartition=strtoul(addressValue.substr(0, pos).c_str(),NULL,10);             
            addressValue.erase(0, pos + delimiter.length());
            pos=addressValue.find(delimiter);
            logNumber=strtoul(addressValue.substr(0, pos).c_str(),NULL,10);               
            addressValue.erase(0, pos + delimiter.length());
            pos=addressValue.find(delimiter);
            offset=strtoul(addressValue.substr(0, pos).c_str(),NULL,10);    
            addressValue.erase(0, pos + delimiter.length());   
            length=strtoul(addressValue.c_str(),NULL,10);
            if(logNumber<beginLogNum[logPartition]){
                //fprintf(stderr,"curPartition:%d,logPartition:%d,partition[i]:%d,readLogfile[%d],beginLogNum[%d]:%d error!!\n",curPartition,logPartition,partition[i],logNumber,partition[i],beginLogNum[partition[i]]);
            }
            //fprintf(stderr,"Key:%s,p:%d,n:%d,offset:%d,Length:%d\n",key.ToString().c_str(),logPartition,logNumber,offset,length);
            if(logNumber==LogNum[partition[i]]-1 && logPartition==partition[i]){
              addressStr=SortedStoreIter->value().ToString();
              //printf("Key:%s,p:%d,n:%d,offset:%d,Length:%d\n",key.ToString().c_str(),logPartition,logNumber,offset,length);
            }else{
                std::stringstream address_value;
                if( globalLogFile[index]==NULL){                
                  fprintf(stderr,"curPartition:%d,currentLogFile error!!\n",curPartition);
                  printf("curPartition:%d,currentLogFile error!!\n",curPartition);
                }
                long curOffset = ftell(globalLogFile[index]);
                address_value<<partition[i]<<"&"<<LogNum[partition[i]]<<"&"<<curOffset<<"&"<<length;  
                addressStr=address_value.str();    
                int curindex=curPartition*config::logFileNum+logNumber;//-beginLogNum[curPartition];
                if(globalLogFile[curindex]==NULL){           
                  fprintf(stderr,"curPartition:%d,logPartition:%d,partition[i]:%d,readLogfile[%d] error,curindex:%d!!\n",curPartition,logPartition,partition[i],logNumber,curindex);
                  printf("curPartition:%d,logPartition:%d,partition[i]:%d,readLogfile[%d] error!!\n",curPartition,logPartition,partition[i],logNumber);
                }
                char myBuffer[config::maxValueSize];              
                fseek(globalLogFile[curindex],offset,SEEK_SET);
                //fseek(myLogfile,offset,SEEK_SET);
                int num=fread(myBuffer,sizeof(char),length,globalLogFile[curindex]);
                //int num=fread(buffer,sizeof(char),length,myLogfile);
                //write valid kv pairs to new log segments in partition 
                if(length>config::maxValueSize){
                    fprintf(stderr,"length error!!\n");
                }
                int num1=fwrite(myBuffer, sizeof(char),length,globalLogFile[index]);
                writeLogValue+= length;
                writeDataSizeActual+= length;
                //fprintf(stderr,"Key:%s,p:%d,n:%d,offset:%d,Length:%d\n",key.ToString().c_str(),logPartition,logNumber,offset,length);
            }
            //add to SSTable files in SortedStore
            compact->builder[0]->Add(key, addressStr);
            if(compact->builder[0]->FileSize() >= OutputFileSizeSet(0,length)){    
                status = FinishCompactionOutputFile(compact, SortedStoreIter,0);             
                writeDataSizeActual+=compact->current_output(0)->file_size;
                if (!status.ok()){
                  fprintf(stderr,"--FinishCompactionOutputFile failed,break!!\n");
                  break;
                }           
            }       
            SortedStoreIter->Next(); 
        }
        if (status.ok()) {  
            if (compact->builder[0] != NULL) {
              status = FinishCompactionOutputFile(compact, SortedStoreIter,0);
            }
        }
        delete SortedStoreIter;
        SortedStoreIter = NULL;
        mutex_.Lock();
        if(status.ok()) {
          int moveToLevel=1;
          status = InstallCompactionResults(compact,partition[i],false,&moveToLevel);//
        } 
        printf("after InstallCompactionResults NumFiles L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,partition[i]),versions_->NumLevelFiles(1,partition[i]));
        CompactionStats stats[config::kNumLevels+config::kTempLevel];
        int moveToLevel=1;
        for (int which = 0; which < config::kNumLevels+config::kTempLevel; which++) {
          for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
            stats[which].bytes_read += compact->compaction->input(which, i)->file_size;
          }
        }
        for(int k=0;k<2;k++){
            for (size_t i = 0; i < compact->outputs[k].size(); i++) {
              stats[moveToLevel].bytes_written += compact->outputs[k][i].file_size;
            }
        }
        stats[moveToLevel].bytes_written += writeLogValue;
        for(int k=0;k<config::kNumLevels+config::kTempLevel;k++){
            stats_[partition[i]][k].Add(stats[k]);
        }
        if (!status.ok()) {
          RecordBackgroundError(status);
        }
        DeleteInvalidFiles(compact->compaction->GetAllInput());//
        CleanupCompaction(compact,partition[i]);
        c->ReleaseInputs();
        //DeleteObsoleteFiles(partition[i]);
    }

    for(int i=beginLogNum[curPartition];i<LogSegments-1 || i<1;i++){//
        int curindex=curPartition*config::logFileNum+i;//-beginLogNum[curPartition];
        //printf("before close old logs,i:%d,curindex:%d\n",i,curindex);
        fclose(globalLogFile[curindex]);
        globalLogFile[curindex]=NULL;
        printf("after close old logs,i:%d,curindex:%d\n",i,curindex);
        std::stringstream LogName;  
        LogName<< LogPrefixName <<curPartition<<"-"<<i;
        std::string name=LogName.str();

        globalLogFile[curindex]=fopen(name.c_str(),"w");
        fclose(globalLogFile[curindex]);
        globalLogFile[curindex]=NULL;
        env_->DeleteFile(name.c_str());//////
        if(remove(name.c_str())==0){
            printf("Remove file:%s\n",name.c_str());
        }
    }
    double compactEndTime=Env::Default()->NowMicros();
    //totalCompactTime[curPartition]+=compactEndTime-compactBeginTime;
    doGC = false;
}

void DBImpl::doGCWithinPartition(int curPartition,int LogSegments){
    FILE* readLogfile[LogSegments];
    for(int i=0;i<LogSegments;i++){
        readLogfile[i]=NULL;
    }
    Status status;
    int64_t writeLogValue=0;
    int partition[2];
    //fprintf(stderr,"curPartition=%d,beginLogNum=%d\n",curPartition,beginLogNum[curPartition]);
    for(int i=beginLogNum[curPartition];i<LogSegments-1 || i<1;i++){
        std::stringstream readLogName;
        readLogName<< LogPrefixName <<curPartition<<"-"<<i;      
        std::string name=readLogName.str();
        int index=curPartition*config::logFileNum+i;//-beginLogNum[curPartition];
        readLogfile[i]=globalLogFile[index];
        //readLogfile[i]=globalLogFile[curPartition][i];
        if( readLogfile[i]==NULL){
            fprintf(stderr,"curPartition:%d,myLogfile:%s error,index:%d!!\n",curPartition,name.c_str(),index);
            printf("curPartition:%d,myLogfile:%s error!!\n",curPartition,name.c_str());
        }
        printf("curPartition:%d,open Logfile:%s GC!!\n",curPartition,name.c_str());
        #ifdef READ_AHEAD
          int fileID=fileno(readLogfile[i]);
          //int ret=readahead(fileID,0,file_size);  config::file_size
          uint64_t prefetchSize=3221225472; //3GB
          int ret=posix_fadvise(fileID,0L,prefetchSize, POSIX_FADV_WILLNEED);//POSIX_FADV_RANDOM | POSIX_FADV_WILLNEED
          printf("GC log file, ret=%d\n", ret);
          if(ret == -1){
            printf("posix_fadvise调用失败！\n");
          }
        #endif
        //fclose(readLogfile[i]);  
    }
    printf("LogNum[%d]=%d,LogNum[%d]=%d,LogSegments:%d\n",curPartition,LogNum[curPartition],NewestPartition,LogNum[NewestPartition],LogSegments);
    std::stringstream LogName;
    LogName<< LogPrefixName <<curPartition<<"-"<<LogNum[curPartition];
    std::string name=LogName.str();
    int index=curPartition*config::logFileNum+LogNum[curPartition];//-beginLogNum[partition[i]];
    globalLogFile[index]=fopen(name.c_str(),"ab+");
    if(globalLogFile[index]==NULL){     
      printf("Unable to open file:%s!\n",name.c_str());
      fprintf(stderr,"Unable to open file:%s!\n",name.c_str());
      exit(0);
    }
    Compaction* c;
    c = versions_->PickSortedStoreForGC(curPartition);///
    CompactionState* compact = new CompactionState(c);
    printf("c-input.size:%d\n",compact->compaction->num_input_files(1));
    mutex_.Unlock();
    Iterator* SortedStoreIter = versions_->MakeGCterator(c);
    for(SortedStoreIter->SeekToFirst();SortedStoreIter->Valid();){
        Slice key = SortedStoreIter->key();
        std::string addressStr;
        if(compact->builder[0] == NULL){
            status = OpenCompactionOutputFile(compact,curPartition,false,0);
            if (!status.ok()) {
              fprintf(stderr,"OpenCompactionOutputFile failed,break!!\n");
              break;
            }
        }
        if (compact->builder[0]->NumEntries()==0){
            compact->current_output(0)->smallest.DecodeFrom(key);
        }
        compact->current_output(0)->largest.DecodeFrom(key);   
          std::string delimiter = "&";
          size_t pos = 0;
          long length,offset,logPartition,logNumber;           
          std::string addressValue=SortedStoreIter->value().ToString();
          pos=addressValue.find(delimiter);
          logPartition=strtoul(addressValue.substr(0, pos).c_str(),NULL,10);             
          addressValue.erase(0, pos + delimiter.length());
          pos=addressValue.find(delimiter);
          logNumber=strtoul(addressValue.substr(0, pos).c_str(),NULL,10);               
          addressValue.erase(0, pos + delimiter.length());
          pos=addressValue.find(delimiter);
          offset=strtoul(addressValue.substr(0, pos).c_str(),NULL,10);    
          addressValue.erase(0, pos + delimiter.length());   
          length=strtoul(addressValue.c_str(),NULL,10);
          if(logNumber<beginLogNum[logPartition]){
            //fprintf(stderr,"curPartition:%d,logPartition:%d,partition[i]:%d,readLogfile[%d],beginLogNum[%d]:%d error!!\n",curPartition,logPartition,partition[i],logNumber,partition[i],beginLogNum[partition[i]]);
          }
            if(logNumber==LogNum[curPartition]-1 && logPartition==curPartition){
            addressStr=SortedStoreIter->value().ToString();        
            //printf("Key:%s,p:%d,n:%d,offset:%d,Length:%d\n",key.ToString().c_str(),logPartition,logNumber,offset,length);
          }else{
            std::stringstream address_value;    
            if( globalLogFile[index]==NULL){                
              fprintf(stderr,"curPartition:%d,currentLogFile error!!\n",curPartition);
              printf("curPartition:%d,currentLogFile error!!\n",curPartition);
            }
            long curOffset = ftell(globalLogFile[index]);
            address_value<<curPartition<<"&"<<LogNum[curPartition]<<"&"<<curOffset<<"&"<<length;  
            addressStr=address_value.str();
            int curindex=curPartition*config::logFileNum+logNumber;//-beginLogNum[curPartition];
            if(globalLogFile[curindex]==NULL){           
              fprintf(stderr,"curPartition:%d,logPartition:%d,curPartition:%d,readLogfile[%d] error,curindex:%d!!\n",curPartition,logPartition,curPartition,logNumber,curindex);
              printf("curPartition:%d,logPartition:%d,curPartition:%d,readLogfile[%d] error!!\n",curPartition,logPartition,curPartition,logNumber);
            }
            char myBuffer[config::maxValueSize];              
            fseek(globalLogFile[curindex],offset,SEEK_SET);
            //fseek(myLogfile,offset,SEEK_SET);
            int num=fread(myBuffer,sizeof(char),length,globalLogFile[curindex]);
            //int num=fread(buffer,sizeof(char),length,myLogfile);
            //write valid kv pairs to new log segments in partition 
            if(length>config::maxValueSize){
                fprintf(stderr,"length error!!\n");
            }
            int num1=fwrite(myBuffer, sizeof(char),length,globalLogFile[index]);
            writeLogValue+= length;
            writeDataSizeActual+= length;
            //fprintf(stderr,"Key:%s,p:%d,n:%d,offset:%d,Length:%d\n",key.ToString().c_str(),logPartition,logNumber,offset,length);    
          }
        //add to SSTable files in SortedStore
        compact->builder[0]->Add(key, addressStr);
        if(compact->builder[0]->FileSize() >= OutputFileSizeSet(0,length)){//	input->value().size()          
            status = FinishCompactionOutputFile(compact, SortedStoreIter,0);  
            writeDataSizeActual+=compact->current_output(0)->file_size;
            if (!status.ok()) {
              fprintf(stderr,"--FinishCompactionOutputFile failed,break!!\n");
              break;
            }           
        }       
        SortedStoreIter->Next(); 
    }
    if (status.ok()) {  
        if (compact->builder[0] != NULL) {
          status = FinishCompactionOutputFile(compact, SortedStoreIter,0);
        }
    }
    delete SortedStoreIter;
    SortedStoreIter = NULL;
    mutex_.Lock();
    if(status.ok()) {
      int moveToLevel=1;
      status = InstallCompactionResults(compact,curPartition,false,&moveToLevel);//////
    }
    printf("after InstallCompactionResults NumFiles L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,curPartition),versions_->NumLevelFiles(1,curPartition));
    CompactionStats stats[config::kNumLevels+config::kTempLevel];
    int moveToLevel=1;
    for (int which = 0; which < config::kNumLevels+config::kTempLevel; which++) {
      for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
        stats[which].bytes_read += compact->compaction->input(which, i)->file_size;
      }
    }
    for(int k=0;k<2;k++){
        for (size_t i = 0; i < compact->outputs[k].size(); i++) {
          stats[moveToLevel].bytes_written += compact->outputs[k][i].file_size;
        }
    }
    stats[moveToLevel].bytes_written += writeLogValue;
    for(int k=0;k<config::kNumLevels+config::kTempLevel;k++){
        stats_[curPartition][k].Add(stats[k]);
    }
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    DeleteInvalidFiles(compact->compaction->GetAllInput());////
    CleanupCompaction(compact,curPartition);
    c->ReleaseInputs();
    //DeleteObsoleteFiles(partition[i]);
  for(int i=beginLogNum[curPartition];i<LogSegments-1 || i<1;i++){//
    int curindex=curPartition*config::logFileNum+i;//-beginLogNum[curPartition];
    printf("before close old logs,i:%d,curindex:%d\n",i,curindex);
    fclose(globalLogFile[curindex]);
    globalLogFile[curindex]=NULL;
    printf("after close old logs,i:%d,curindex:%d\n",i,curindex);
    std::stringstream LogName;  
    LogName<< LogPrefixName <<curPartition<<"-"<<i;
    std::string name=LogName.str();
    globalLogFile[curindex]=fopen(name.c_str(),"w");
    fclose(globalLogFile[curindex]);
    globalLogFile[curindex]=NULL;
    env_->DeleteFile(name.c_str());///////
    if(remove(name.c_str())==0){
        printf("Remove file:%s\n",name.c_str());
    }
  }
}

void DBImpl::compactHashIndexTable(){
  for(int k=0;k<=NewestPartition;k++){
      if(versions_->NumLevelFiles(0,k) >=config::kL0_CompactionTrigger*4/5){
        Compaction* c = versions_->PickSomeHashIndexFileCompaction(k);///
        CompactionState* compact = new CompactionState(c);
        fprintf(stderr,"before compactHashIndexTable L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,k),versions_->NumLevelFiles(1,k),versions_->NumLevelFiles(config::kNumLevels+config::kTempLevel-1,k));
        Status  status= DoCompactionWork(compact,k,false);
        if (!status.ok()) {
          RecordBackgroundError(status);
        }
        CleanupCompaction(compact,k);
        c->ReleaseInputs();
        DeleteObsoleteFiles(k);//delete old SSTables
        delete c;
	    }   
  }
}

void DBImpl::compactHashIndexPartitionTable(int partition){
	  Compaction* c = versions_->PickSomeHashIndexFileCompaction(partition);///
	  CompactionState* compact = new CompactionState(c);
	  printf("before compactHashIndexPartitionTable L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),versions_->NumLevelFiles(config::kNumLevels+config::kTempLevel-1,partition));
	  fprintf(stderr,"before compactHashIndexPartitionTable L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),versions_->NumLevelFiles(config::kNumLevels+config::kTempLevel-1,partition));
	  Status  status= DoCompactionWork(compact,partition,false);
	  if (!status.ok()) {
	    RecordBackgroundError(status);
	  }
	  CleanupCompaction(compact,partition);
	  c->ReleaseInputs();
	  DeleteObsoleteFiles(partition);//delete old SSTables
	  delete c;
}

void DBImpl::MergeUnsortedStoreFilesTogether(int partition){
    Compaction* c = versions_->PickSomeHashIndexFileCompaction(partition);///
	  CompactionState* compact = new CompactionState(c);
    printf("c-input.size:%d\n",compact->compaction->num_input_files(0));
    //mutex_.Unlock();
    Status status;
    Iterator* SortedStoreIter = versions_->MakeUnsortedStoreIterator(c);
    //fprintf(stderr,"after MakesizeBasedterator c-input.size:%d\n",compact->compaction->num_input_files(0));
    for(SortedStoreIter->SeekToFirst();SortedStoreIter->Valid();){
        Slice key = SortedStoreIter->key();
        std::string addressStr;
        if(compact->builder[0] == NULL){
            status = OpenCompactionOutputFileForScan(compact,partition,false,0);
            if (!status.ok()) {
              fprintf(stderr,"OpenCompactionOutputFileForScan failed,break!!\n");
              break;
            }
        }
        if (compact->builder[0]->NumEntries()==0){
            compact->current_output(0)->smallest.DecodeFrom(key);
        }
        compact->current_output(0)->largest.DecodeFrom(key);   
        addressStr=SortedStoreIter->value().ToString();
        //add to SSTable files in SortedStore
        compact->builder[0]->Add(key, addressStr);
        SortedStoreIter->Next(); 
    }
    if (status.ok()) {  
        if (compact->builder[0] != NULL) {
          status = FinishCompactionOutputFile(compact, SortedStoreIter,0);
        }
    }
    delete SortedStoreIter;
    SortedStoreIter = NULL;
    #ifdef LIST_HASH_INDEX
	    updateIndex.curHashIndex=ListHashIndex[partition];
#endif

#ifdef CUCKOO_HASH_INDEX
	    updateIndex.curHashIndex=CuckooHashIndex[partition];
#endif      
	    updateIndex.deleteFiles=compact->compaction->GetAllInput();
      updateIndex.file_number=compact->outputs[0][0].number;
	    //updateHashTable(compact->compaction->GetAllInput());
	    std::future<void*> ret= Env::Default()->threadPool->addTask(DBImpl::updateBigHashTable,(void *) &updateIndex);   
    //mutex_.Lock();
    if(status.ok()) {
      int moveToLevel=0;
      status = InstallSizeBasedCompactionResults(compact,partition);//
    }
    CompactionStats stats[config::kNumLevels+config::kTempLevel];
    int moveToLevel=0;
    for (int which = 0; which < config::kNumLevels+config::kTempLevel; which++) {
      for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
        stats[which].bytes_read += compact->compaction->input(which, i)->file_size;
      }
    }
    for(int k=0;k<2;k++){
        for (size_t i = 0; i < compact->outputs[k].size(); i++) {
          stats[moveToLevel].bytes_written += compact->outputs[k][i].file_size;
        }
    }
    for(int k=0;k<config::kNumLevels+config::kTempLevel;k++){
        stats_[partition][k].Add(stats[k]);
    }
	  if (!status.ok()) {
	    RecordBackgroundError(status);
	  }
    DeleteInvalidFiles(compact->compaction->GetAllInput());
	  CleanupCompaction(compact,partition);
	  c->ReleaseInputs();
	  //DeleteObsoleteFiles(partition);//delete old SSTables
   try{
	  ret.wait();
	  } catch(const std::future_error &e){ std::cerr<<"wait error\n"; }
    delete c;
}

void DBImpl::CleanupCompaction(CompactionState* compact,int partition) {
  mutex_.AssertHeld();
  for(int k=0;k<2;k++){
      if (compact->builder[k] != NULL) {
	//May happen if we get a shutdown call in the middle of compaction
	compact->builder[k]->Abandon();
	delete compact->builder[k];
      } else {
	assert(compact->outfile[k] == NULL);
      }
      delete compact->outfile[k];
      for (size_t i = 0; i < compact->outputs[k].size(); i++) {
	const CompactionState::Output& out = compact->outputs[k][i];
	pending_outputs_[partition].erase(out.number);
      }
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact,int partition,bool doSplit,int direction) {
  assert(compact != NULL);
  assert(compact->builder[direction] == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_[partition].insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs[direction].push_back(out);
    mutex_.Unlock();
  }
  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s;
  s = env_->NewWritableFile(fname, &compact->outfile[direction]);
  if (s.ok()) {
     compact->builder[direction] = new TableBuilder(options_, compact->outfile[direction],false,options_.bloom_bits);
  }
  return s;
}

Status DBImpl::OpenCompactionOutputFileForScan(CompactionState* compact,int partition,bool doSplit,int direction) {
  assert(compact != NULL);
  assert(compact->builder[direction] == NULL);
  uint64_t file_number;
  {
    //mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_[partition].insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs[direction].push_back(out);
    //mutex_.Unlock();
  }
  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s;
  s = env_->NewWritableFile(fname, &compact->outfile[direction]);
  if (s.ok()) {
     compact->builder[direction] = new TableBuilder(options_, compact->outfile[direction],false,options_.bloom_bits);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input,int direction) {
  assert(compact != NULL);
  assert(compact->outfile[direction] != NULL);
  assert(compact->builder[direction] != NULL);
  const uint64_t output_number = compact->current_output(direction)->number;
  assert(output_number != 0);
  //Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder[direction]->NumEntries();
  if (s.ok()) {
    s = compact->builder[direction]->Finish();
  } else {     
    compact->builder[direction]->Abandon();
  }
  const uint64_t current_bytes = compact->builder[direction]->FileSize();
  compact->current_output(direction)->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder[direction];
  compact->builder[direction]= NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile[direction]->Sync();
  }
  if (s.ok()) {
    s = compact->outfile[direction]->Close();
  }
  delete compact->outfile[direction];
  compact->outfile[direction] = NULL;

  if (s.ok() && current_entries > 0) {
    //fprintf(stderr,"before table_cache_->NewIterator,s.ok():%d\n",s.ok());
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes, false, false);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallSizeBasedCompactionResults(CompactionState* compact,int partition) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));
  compact->compaction->AddInputDeletions(compact->compaction->edit(),partition,false);//////delete old SStable
  int tableN;
  int level = compact->compaction->level();
  Status s;
  level=0;
  for (size_t i = 0; i < compact->outputs[0].size(); i++) {
    const CompactionState::Output& out = compact->outputs[0][i];
    compact->compaction->edit()->AddFile(level,out.number,partition, out.file_size, out.smallest, out.largest);
    //fprintf(stderr,"fileNumber:%d,smallestkey:%s,largestKey:%s,partition:%d\n",out.number,out.smallest.user_key().ToString().c_str(),out.largest.user_key().ToString().c_str(),partition);
  }
  s = versions_->LogAndApply(compact->compaction->edit(), &mutex_, &bg_log_cv_, &bg_log_occupied_,partition);
  return s; 
}

Status DBImpl::InstallCompactionResults(CompactionState* compact,int partition,bool doSplit,int* moveToLevel) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));
  compact->compaction->AddInputDeletions(compact->compaction->edit(),partition,doSplit);//////delete old SStable
  printf("after AddInputDeletions NumFiles L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),versions_->NumLevelFiles(config::kNumLevels+config::kTempLevel-1,partition));
 
  int tableN;
  int level = compact->compaction->level();
  Status s;
  if(doSplit && compact->outputs[1].size()>0){
    int isEmpty=1;
    char beginSmallKey[100],middleSmallKey[100];
    tableN=compact->outputs[0].size();
    int emptyLevel=1; 
    for(int i=0;i<compact->outputs[0].size();i++){
        const CompactionState::Output& out = compact->outputs[0][i];
        //compact->compaction->edit()->AddFile(config::kNumLevels-2,out.number,partition, out.file_size, out.smallest, out.largest);
        compact->compaction->edit()->AddFile(emptyLevel,out.number,partition, out.file_size, out.smallest, out.largest);
       // printf("fileNumber:%d,smallestkey:%d,largestKey:%d,partition:%d\n",out.number,atoi(out.smallest.user_key().ToString().c_str()),atoi(out.largest.user_key().ToString().c_str()),partition);
    }
    s = versions_->LogAndApply(compact->compaction->edit(), &mutex_, &bg_log_cv_, &bg_log_occupied_,partition);
    printf("partition:%d, the smallest key:%s\n",NewestPartition,(char*)compact->outputs[1][0].smallest.user_key().ToString().c_str());  
    for(int i=0;i< compact->outputs[1].size();i++){
      const CompactionState::Output& out = compact->outputs[1][i];
      compact->compaction->edit()->AddFile(emptyLevel,out.number,NewestPartition, out.file_size, out.smallest, out.largest);
      //printf("fileNumber:%d,largestKey:%d\n",out.number,atoi(compact->outputs[i].largest.user_key().ToString().c_str()));
    }
    s = versions_->LogAndApply(compact->compaction->edit(), &mutex_, &bg_log_cv_, &bg_log_occupied_,NewestPartition);
  }else{
        level=1;
        for (size_t i = 0; i < compact->outputs[0].size(); i++) {
          const CompactionState::Output& out = compact->outputs[0][i];
          compact->compaction->edit()->AddFile(level,out.number,partition, out.file_size, out.smallest, out.largest);
          //printf("fileNumber:%d,smallestkey:%d,largestKey:%d,partition:%d\n",out.number,atoi(out.smallest.user_key().ToString().c_str()),atoi(out.largest.user_key().ToString().c_str()),partition);
        }
        s = versions_->LogAndApply(compact->compaction->edit(), &mutex_, &bg_log_cv_, &bg_log_occupied_,partition);
  }

  return s;
}

bool DBImpl::containNewTable(int NewTableNum, std::vector<FileMetaData*> *myfiles){
   for (size_t i = 0; i < myfiles[0].size(); i++) {
      FileMetaData* f = myfiles[0][i];
      if(NewTableNum==(int)f->number){
	    return true;
      }
   }
  return false;
}

void DBImpl::AnalysisHashTable(long int* emptyBucket, long int* totalEntry){
  #ifdef LIST_HASH_INDEX
  for(int p=0;p<=NewestPartition;p++){
    for(int i=0;i<config::bucketNum;i++){
        ListIndexEntry *lastEntry=&ListHashIndex[p][i];
        int curTableNum=bytes2ToInt(ListHashIndex[p][i].TableNum);
        if(curTableNum==0 && lastEntry->nextEntry==NULL){
            *emptyBucket=*emptyBucket+1;
        }else{
          while(lastEntry!=NULL){		
              *totalEntry=*totalEntry+1;
              lastEntry=lastEntry->nextEntry;
          }
        }
    }
  }
  #endif

  #ifdef CUCKOO_HASH_INDEX
    for(int p=0;p<=NewestPartition;p++){
      for(int i=0;i<config::bucketNum;i++){
          ListIndexEntry *lastEntry=&CuckooHashIndex[p][i];
          int curTableNum=bytes2ToInt(CuckooHashIndex[p][i].TableNum);
          if(curTableNum==0){
              *emptyBucket=*emptyBucket+1;
          }else{	
                *totalEntry=*totalEntry+1;             
          }
      }
    }
  #endif
}

void* DBImpl::updateHashTable(void *paraData){
  struct upIndexPara *updateIndex=(struct upIndexPara*)paraData;
  std::vector<FileMetaData*> *deleteFiles=updateIndex->deleteFiles;
#ifdef LIST_HASH_INDEX
  ListIndexEntry* curHashIndex=updateIndex->curHashIndex; 
  for(int i=0;i<config::bucketNum;i++){
    int curTableNum=bytes2ToInt(curHashIndex[i].TableNum);
    if(curTableNum!=0 && containNewTable(curTableNum,deleteFiles)){
      curHashIndex[i].KeyTag[0]=0;
      curHashIndex[i].KeyTag[1]=0;
      curHashIndex[i].TableNum[0]=0;
      curHashIndex[i].TableNum[1]=0;
      curHashIndex[i].TableNum[2]=0;
      }
    ListIndexEntry* nextIndexEntry=curHashIndex[i].nextEntry;
    ListIndexEntry* PrevIndexEntry=&curHashIndex[i];
    while(nextIndexEntry!=NULL){
      int myTableNum=bytes2ToInt(nextIndexEntry->TableNum);
      if(containNewTable(myTableNum,deleteFiles)){
        PrevIndexEntry->nextEntry=nextIndexEntry->nextEntry;
        delete nextIndexEntry;
        //nextIndexEntry=NULL;
        nextIndexEntry=PrevIndexEntry->nextEntry;
        continue;
        }
        PrevIndexEntry=nextIndexEntry;
        nextIndexEntry=nextIndexEntry->nextEntry;
    }
  }
#endif

#ifdef CUCKOO_HASH_INDEX
  ListIndexEntry* curHashIndex=updateIndex->curHashIndex;
  for(int i=0;i<config::bucketNum;i++){
    int curTableNum=bytes2ToInt(curHashIndex[i].TableNum);
    //while(curTableNum!=0 && containNewTable(curTableNum,deleteFiles)){
    if(curTableNum!=0 && containNewTable(curTableNum,deleteFiles)){
      curHashIndex[i].KeyTag[0]=0;
      curHashIndex[i].KeyTag[1]=0;
      curHashIndex[i].TableNum[0]=0;
      curHashIndex[i].TableNum[1]=0;
      curHashIndex[i].TableNum[2]=0;
    }
    ListIndexEntry* nextIndexEntry=curHashIndex[i].nextEntry;
    ListIndexEntry* PrevIndexEntry=&curHashIndex[i];
    while(nextIndexEntry!=NULL){
      int myTableNum=bytes2ToInt(nextIndexEntry->TableNum);
      if(containNewTable(myTableNum,deleteFiles)){
        PrevIndexEntry->nextEntry=nextIndexEntry->nextEntry;
        delete nextIndexEntry;
        //nextIndexEntry=NULL;
        nextIndexEntry=PrevIndexEntry->nextEntry;
        continue;
      }
      PrevIndexEntry=nextIndexEntry;
      nextIndexEntry=nextIndexEntry->nextEntry;
    }
  }
#endif
  //fprintf(stderr,"after in updateHashTable\n");
}

void* DBImpl::updateBigHashTable(void *paraData){
  struct upIndexPara *updateIndex=(struct upIndexPara*)paraData;
  std::vector<FileMetaData*> *deleteFiles=updateIndex->deleteFiles;
  uint64_t file_number=updateIndex->file_number;
  //fprintf(stderr,"in updateBigHashTable,file_number:%d\n",file_number);
  byte newBigTableNum[2];
  intTo2Byte(file_number,newBigTableNum);

#ifdef LIST_HASH_INDEX
  ListIndexEntry* curHashIndex=updateIndex->curHashIndex; 
  //int newBigTableNum=bytes3ToInt(curHashIndex[i].TableNum);
  for(int i=0;i<config::bucketNum;i++){
    ListIndexEntry *lastEntry=&curHashIndex[i];
    while(lastEntry!=NULL){
      int myTableNum=bytes2ToInt(lastEntry->TableNum);
      if(myTableNum!=0 && containNewTable(myTableNum,deleteFiles)){
        lastEntry->TableNum[0]=newBigTableNum[0];
        lastEntry->TableNum[1]=newBigTableNum[1];
        lastEntry->TableNum[2]=newBigTableNum[2];   
      }
      lastEntry=lastEntry->nextEntry;
    }
  }
#endif

#ifdef CUCKOO_HASH_INDEX
  ListIndexEntry* curHashIndex=updateIndex->curHashIndex;
  //ListIndexEntry* overflowBucket=updateIndex->overflowBucket;
  for(int i=0;i<config::bucketNum;i++){
    ListIndexEntry *lastEntry=&curHashIndex[i];
    while(lastEntry!=NULL){
      int myTableNum=bytes2ToInt(lastEntry->TableNum);
      if(myTableNum!=0 && containNewTable(myTableNum,deleteFiles)){
        lastEntry->TableNum[0]=newBigTableNum[0];
        lastEntry->TableNum[1]=newBigTableNum[1];
        lastEntry->TableNum[2]=newBigTableNum[2];   
      }
      lastEntry=lastEntry->nextEntry;
    }
  }
#endif
  //fprintf(stderr,"after in updateBigHashTable\n");
}

void DBImpl::setUpWritePartitionThreads(int partition){
    switch (partition){
      case 0:
        env_->StartThread(&DBImpl::WritePartitionsWrapper0, this);
        num_bg_threads_++;
        break;
      case 1:
        env_->StartThread(&DBImpl::WritePartitionsWrapper1, this);
        num_bg_threads_++;
        break;
      case 2:
        env_->StartThread(&DBImpl::WritePartitionsWrapper2, this);
        num_bg_threads_++;
        break;
      case 3:
        env_->StartThread(&DBImpl::WritePartitionsWrapper3, this);
        num_bg_threads_++;
        break;
      case 4:
        env_->StartThread(&DBImpl::WritePartitionsWrapper4, this);
        num_bg_threads_++;
        break;
      case 5:
        env_->StartThread(&DBImpl::WritePartitionsWrapper5, this);
        num_bg_threads_++;
        break;
      case 6:
        env_->StartThread(&DBImpl::WritePartitionsWrapper6, this);
        num_bg_threads_++;
        break;
      case 7:
        env_->StartThread(&DBImpl::WritePartitionsWrapper7, this);
        num_bg_threads_++;
        break;
      default:
        env_->StartThread(&DBImpl::WritePartitionsWrapper0, this);
        num_bg_threads_++;
        break;
    }
    fprintf(stderr,"------num_bg_threads_:%d\n",num_bg_threads_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact,int partition,bool doSplit) {/////
  const uint64_t start_micros = env_->NowMicros();
  finishUpdateHash=0;
   std::future<void*> ret;
  int64_t writeLogValue=0;
  int newIndex;
  std::stringstream LogName;
  LogName<< LogPrefixName <<partition<<"-"<<LogNum[partition];
  std::string name=LogName.str();
  int pindex=partition*config::logFileNum+LogNum[partition];//-beginLogNum[partition];
  globalLogFile[pindex]=fopen(name.c_str(),"ab+");
    if(globalLogFile[pindex]==NULL){
         printf("Unable to open file for write:%s!\n",name.c_str());
         fprintf(stderr,"Unable to open file for write:%s!\n",name.c_str());
         exit(0);
  }
  double compactBeginTime=Env::Default()->NowMicros();
  double totalMergeTime=0,totalSortTime=0,totalReadTime=0,totalWriteTime=0,totalUpHashTime=0,inPutItearTime=0,otherTime=0;
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions
  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);
  printf("Compacting ");
   for(int i=0;i<config::kNumLevels+config::kTempLevel;i++){
      printf("%d@%d +",compact->compaction->num_input_files(i),i);
  }
   printf(" files in partition=%d\n",partition);
   
   fprintf(stderr,"Compacting ");
   for(int i=0;i<config::kNumLevels+config::kTempLevel;i++){
      fprintf(stderr,"%d@%d +",compact->compaction->num_input_files(i),i);
  }
   fprintf(stderr," files in partition=%d\n",partition);
  assert(versions_->NumLevelFiles(compact->compaction->level(),partition) > 0);
  //assert(compact->builder == NULL);
  //assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }
  int fileSizeFlag=0,isEmpty=1;
  std::string middleKey;
//mutex_.Lock();
  if(doSplit){
    std::stringstream LogName;
     //LogName<< "logfile-"<<partition<<"-"<<LogNum[partition];
     LogName<< LogPrefixName <<NewestPartition<<"-"<<LogNum[NewestPartition];
     std::string name=LogName.str();
     newIndex=NewestPartition*config::logFileNum+LogNum[NewestPartition];//-beginLogNum[NewestPartition];
     globalLogFile[newIndex]=fopen(name.c_str(),"ab+");
      if(globalLogFile[newIndex]==NULL){
         printf("Unable to open file for write:%s!\n",name.c_str());
         exit(0);
     }
     printf("LogNum[%d]=%d, LogNum[%d]=%d\n",partition,LogNum[partition],NewestPartition,LogNum[NewestPartition]);
#ifdef LIST_HASH_INDEX
    ListHashIndex[NewestPartition]=new ListIndexEntry[config::bucketNum];
    for(int i=0;i<config::bucketNum;i++){
      ListHashIndex[NewestPartition][i].KeyTag[0]=0;
      ListHashIndex[NewestPartition][i].KeyTag[1]=0;
      ListHashIndex[NewestPartition][i].TableNum[0]=0;
      ListHashIndex[NewestPartition][i].TableNum[1]=0;
      ListHashIndex[NewestPartition][i].TableNum[2]=0;
      ListHashIndex[NewestPartition][i].nextEntry=NULL;
    }
#endif

#ifdef CUCKOO_HASH_INDEX
    CuckooHashIndex[NewestPartition]=new ListIndexEntry[config::bucketNum];
    //overflowBucket[NewestPartition]=new ListIndexEntry[config::bucketNum];
    for(int i=0;i<config::bucketNum;i++){
      CuckooHashIndex[NewestPartition][i].KeyTag[0]=0;
      CuckooHashIndex[NewestPartition][i].KeyTag[1]=0;
      CuckooHashIndex[NewestPartition][i].TableNum[0]=0;
      CuckooHashIndex[NewestPartition][i].TableNum[1]=0;
      CuckooHashIndex[NewestPartition][i].TableNum[2]=0;
      CuckooHashIndex[NewestPartition][i].nextEntry=NULL;
      //HashIndex[i].nextEntry=NULL;
    }
#endif
    isEmpty=versions_->GetSortedStoreMiddleKey(&middleKey,partition);
    if(!isEmpty){
      splitFinishInstall=false;
      printf("middleKey:%s\n",middleKey.c_str());
      fprintf(stderr,"middleKey:%s, NewestPartition:%d\n",middleKey.c_str(), NewestPartition);
      uint64_t new_log_number = versions_->NewFileNumber();
      ConcurrentWritableFile* lfile = NULL;
      Status status= env_->NewConcurrentWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!status.ok()) {
          // Avoid chewing through file number space in a tight loop.
          versions_->ReuseFileNumber(new_log_number);       
      }   
      logfile_ [NewestPartition].reset(lfile);//
      logfile_number_[NewestPartition]= new_log_number;	  
      logNumVec.push_back(new_log_number);
      printf("buid new logfile_number_:%u,partition:%d\n",new_log_number,NewestPartition);
      log_[NewestPartition].reset(new log::Writer(lfile));////////new logfile
      pmem_[NewestPartition] = new MemTable(internal_comparator_);//
      pmem_[NewestPartition]->Ref();
      //setUpWritePartitionThreads(NewestPartition);//////
    }
  }else{
    bg_fg_cv_.SignalAll();
    bg_memtable_cv_.Signal();
  }
  //Release mutex while we're actually doing the compaction work
  mutex_.Unlock();
  double mergeBeginTime=Env::Default()->NowMicros();
  Iterator* input = versions_->MakeInputIterator(compact->compaction);//
  double inputIteraEndTime=Env::Default()->NowMicros();
  inPutItearTime+=inputIteraEndTime-mergeBeginTime;
  printf("-two Compacting ");
  for(int i=0;i<config::kNumLevels+config::kTempLevel;i++){
      printf("%d@%d +",compact->compaction->num_input_files(i),i);
  }
  printf(" files in partition=%d\n",partition);
   
  fprintf(stderr,"-two Compacting ");
   for(int i=0;i<config::kNumLevels+config::kTempLevel;i++){
      fprintf(stderr,"%d@%d +",compact->compaction->num_input_files(i),i);
  }
  fprintf(stderr," files in partition=%d\n",partition);
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  for (input->SeekToFirst();input->Valid() && !shutting_down_.Acquire_Load();) {
      double otherbeginTime=Env::Default()->NowMicros();
      Slice key = input->key();
      Slice keyValue=input->value();
      uint64_t valueSize=keyValue.size();
      int direction=0;
      if(doSplit && !isEmpty && !(user_comparator()->Compare(key,Slice(middleKey))<0)){
            direction=1;
      }
    bool drop = false;
    double otherendTime=Env::Default()->NowMicros();
    otherTime+=otherendTime-otherbeginTime;
    Slice curKey=key;
    double sortbeginTime=Env::Default()->NowMicros();
    //if (!ParseInternalKey(Slice(keyStr), &ikey)) {
    if (!ParseInternalKey(curKey, &ikey)) {// Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }
      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot){// &&
                // compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }
      last_sequence_for_key = ikey.sequence;
    }
    double sortEndTime=Env::Default()->NowMicros();
    totalSortTime+=sortEndTime-sortbeginTime;
    gKeySize=strlen(ikey.user_key.ToString().c_str());
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif
    double writeBeginTime=Env::Default()->NowMicros();
    if (!drop) {
      // Open output file if necessary
      if (compact->builder[direction] == NULL) {
        status = OpenCompactionOutputFile(compact,partition,doSplit,direction);
        if (!status.ok()) {
          fprintf(stderr,"OpenCompactionOutputFile failed,break!!\n");
          break;
        }
      }
      if (compact->builder[direction]->NumEntries() == 0) {
        compact->current_output(direction)->smallest.DecodeFrom(key);
      }
      compact->current_output(direction)->largest.DecodeFrom(key);
      //////////////////////////
          std::string value=input->value().ToString();
          long size = value.size();
          if(size<config::minValueSize){
              compact->builder[direction]->Add(key, value);
          }else{
              long keySize=strlen(ikey.user_key.ToString().c_str());        
              std::stringstream address_value,vlog_value;
              std::string vlogv=value;
              long offset;
                if(direction==0){              
                    //int num=fwrite(vlogv.c_str(), sizeof(char),vlogv.length(),currentLogfile);
                    offset = ftell(globalLogFile[pindex]);
                    int num=fwrite(vlogv.c_str(), sizeof(char),vlogv.length(),globalLogFile[pindex]);        
                    address_value<<partition<<"&"<<LogNum[partition]<<"&"<<offset<<"&"<<vlogv.length();   
                }else{
                    //int num=fwrite(vlogv.c_str(), sizeof(char),vlogv.length(),newLogfile);
                    offset = ftell(globalLogFile[newIndex]);
                    int num=fwrite(vlogv.c_str(), sizeof(char),vlogv.length(),globalLogFile[newIndex]);
                    address_value<<NewestPartition<<"&"<<LogNum[NewestPartition]<<"&"<<offset<<"&"<<vlogv.length();   
                }               
                writeLogValue+=vlogv.length();
                writeDataSizeActual+=vlogv.length();          
                std::string address = address_value.str();	       
                compact->builder[direction]->Add(key, address);
          }
      //////////////////////////
        if (compact->builder[direction]->FileSize() >= OutputFileSizeSet(fileSizeFlag,valueSize)) {//	input->value().size()
          status = FinishCompactionOutputFile(compact, input,direction);
          writeDataSizeActual+=compact->current_output(direction)->file_size;
        if (!status.ok()) {
          fprintf(stderr,"FinishCompactionOutputFile failed,break!!\n");
          break;
        }
      }
    }
    double writeEndTime=Env::Default()->NowMicros();
    totalWriteTime+=writeEndTime-writeBeginTime;
    double readbeginTime=Env::Default()->NowMicros();
    input->Next();
    double readEndTime=Env::Default()->NowMicros();
    totalReadTime+=readEndTime-readbeginTime;
    key.clear();
  }
  double mergeEndTime=Env::Default()->NowMicros();
  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok()) {
    for(int k=0;k<2;k++){
       if (compact->builder[k] != NULL) {
          status = FinishCompactionOutputFile(compact, input,k);
          writeDataSizeActual+=compact->current_output(k)->file_size;
       }
    }
  }
  fprintf(stderr, "&&&after FinishCompactionOutputFile!\n");
  delete input;
  input = NULL;
  mutex_.Lock();
  printf("compact->compaction->level():%d\n",compact->compaction->level());
  fprintf(stderr,"compact->compaction->level():%d\n",compact->compaction->level());
  CompactionStats stats[config::kNumLevels+config::kTempLevel];
  int moveToLevel=compact->compaction->level()+1;
  if (status.ok()) {
    status = InstallCompactionResults(compact,partition,doSplit,&moveToLevel);/////////////////////////////////////////
     fprintf(stderr,"after InstallCompactionResults\n");
     fprintf(stderr,"partition:%d, NumFiles L0:%d ,L1:%d\n", partition, versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition));        
     splitFinishInstall=true;
  }
 
  double upHashBeginTime=Env::Default()->NowMicros();
  if(compact->compaction->num_input_files(0)>0){
#ifdef LIST_HASH_INDEX
	    updateIndex.curHashIndex=ListHashIndex[partition];
#endif

#ifdef CUCKOO_HASH_INDEX
	    updateIndex.curHashIndex=CuckooHashIndex[partition];
#endif      
	    updateIndex.deleteFiles=compact->compaction->GetAllInput();
	    //updateHashTable(compact->compaction->GetAllInput());
	    ret= Env::Default()->threadPool->addTask(DBImpl::updateHashTable,(void *) &updateIndex);
  }

  if(doSplit && !isEmpty){
    treeRoot->insertNode((char*)middleKey.c_str(),NewestPartition);////
    setUpWritePartitionThreads(NewestPartition);//////
    finishCompaction=true;
    bg_fg_cv_.SignalAll();//
    bg_memtable_cv_.Signal();
  }
  double upHashEndTime=Env::Default()->NowMicros();
  totalUpHashTime+=upHashEndTime-upHashBeginTime;
   
  stats[moveToLevel].micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < config::kNumLevels+config::kTempLevel; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats[which].bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for(int k=0;k<2;k++){
      for (size_t i = 0; i < compact->outputs[k].size(); i++) {
	      stats[moveToLevel].bytes_written += compact->outputs[k][i].file_size;
      }
  }
  stats[moveToLevel].bytes_written += writeLogValue;
  if(status.ok() && doSplit){
      //finishCompaction=true;
     LogNum[NewestPartition]++;
  }
  for(int k=0;k<config::kNumLevels+config::kTempLevel;k++){
      stats_[partition][k].Add(stats[k]);
  }

  if(compact->compaction->num_input_files(0)>0){
      try{
          ret.wait();
      } catch(const std::future_error &e){ std::cerr<<"wait error\n"; }
  }
  printf("merge time:%lf,totalReadTime:%lf,totalSortTime:%lf,totalWriteTime:%lf,totalUpHashTime:%lf,inPutItearTime:%lf,otherTime:%lf\n",mergeEndTime-mergeBeginTime,totalReadTime,totalSortTime,totalWriteTime,totalUpHashTime,inPutItearTime,otherTime);
  duringCompaction = false;
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  double compactEndTime=Env::Default()->NowMicros();
  totalCompactTime[partition]+=compactEndTime-compactBeginTime;
  return status;
}

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* /*arg2*/) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  if (state->mem != NULL) state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options, uint64_t number,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed, bool external_sync) {
  IterState* cleanup = new IterState;
  if (!external_sync) {
    mutex_.Lock();
  }
  ++straight_reads_;
  *latest_snapshot = versions_->LastSequence();
  // Collect together all needed child iterators
  std::vector<Iterator*> list;
 
  for(int k=0;k<=NewestPartition;k++){
    // Collect together all needed child iterators
    list.push_back(pmem_[k]->NewIterator());
    //pmem_[k]->Ref();
    if (pimm_[k] != NULL) {
      list.push_back(pimm_[k]->NewIterator());
      //pimm_[k]->Ref();
    }
    versions_->current()->AddIterators(options, &list,k);
  }
  
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();
  *seed = ++seed_;
  if (!external_sync) {
    mutex_.Unlock();
  }
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), 0, &ignored, &ignored_seed, false);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

void DBImpl::sleepWrites(){
  while(!finishCompaction){
    env_->SleepForMicroseconds(1);
  }
}

int DBImpl::MapCharKeyToPartition(Slice charKey){
  int findPartition= treeRoot->binaryTreeSereach((char*)charKey.ToString().c_str());
  return findPartition;
}


Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  //std::shared_lock<std::shared_mutex> lock(RWmutex_);
  requestEntry *rEntry = new struct requestEntry;
  rEntry->RWFlag = 0;
  rEntry->Roptions = options;
  //rEntry->key = key.ToString();
  strcpy(rEntry->key, key.ToString().c_str());
  int partition=MapCharKeyToPartition(key);
  //fprintf(stderr, "--rEntry.key:%s\n", rEntry->key.c_str());
  requestBatchQueue[partition].push(rEntry);
  while(requestBatchQueue[partition].size()>16){//2,4,8,16,1,1000,8
    mutex_.Unlock();
    env_->SleepForMicroseconds(1);
    mutex_.Lock();
  }
  return s;
}

void DBImpl::PerformGet(ReadOptions& options, char *key, int partition) {
  double getBeginTime=Env::Default()->NowMicros();
  std::shared_lock<std::shared_mutex> lock(RWmutex_);
  double afterLockTime=Env::Default()->NowMicros();
  totalReadLockTime[partition]+=afterLockTime-getBeginTime;
  Status s;
  std::string value;
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }
  //fprintf(stderr, "--in PerformGet, key:%s\n", key);
  MemTable* mem = pmem_[partition];
  MemTable* imm = pimm_[partition];
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  bool have_stat_update = false;
  Version::GetStats stats;
  double readBeginTime=Env::Default()->NowMicros();
  totalReadOther[partition]+=readBeginTime-afterLockTime;
  // Unlock while reading from files and memtables
  {
    RWmutex_.unlock();
    double beforeMemTime=Env::Default()->NowMicros();
    totalReadLockTime[partition]+=beforeMemTime-readBeginTime;
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(Slice(key), snapshot);
  
    if (mem->Get(lkey, &value, &s)) {//
      readMem++;
      double readMemTime=Env::Default()->NowMicros();
      totalReadMem[partition]+=readMemTime-readBeginTime;
    } else if (imm != NULL && imm->Get(lkey, &value, &s)) {//
      readImm++;
      double readIemTime=Env::Default()->NowMicros();
      totalReadMem[partition]+=readIemTime-readBeginTime;
    } else {//
      int readIn0=0;
      double t=0;
      double beginLevelTime=Env::Default()->NowMicros();
     #ifdef LIST_HASH_INDEX
      int index=partition*config::logFileNum+LogNum[partition]-beginLogNum[partition];
      s = current->Get(options, lkey,dbname_,&value, partition,globalLogFile,beginLogNum,&stats,ListHashIndex[partition],&readDataSizeActual,&readIn0,&tableCacheNum,&blockCacheNum,&t);////////////
#endif

#ifdef CUCKOO_HASH_INDEX
      int index=partition*config::logFileNum+LogNum[partition]-beginLogNum[partition];
      s = current->GetwithCukoo(options, lkey,dbname_,&value,partition,globalLogFile,beginLogNum,&stats,CuckooHashIndex[partition],&readDataSizeActual,&readIn0,&tableCacheNum,&blockCacheNum,&t);////////////
#endif
      have_stat_update = true;
      double readLevelTime=Env::Default()->NowMicros();
      if(readIn0){
	        totalReadL0[partition]+=readLevelTime-beginLevelTime;
      }else{
	      totalReadLn[partition]+=readLevelTime-beginLevelTime;
        totalGetCostLn+=t;
      }
    }
    double beforeLockTime=Env::Default()->NowMicros();
    RWmutex_.lock();
    double afterLockTime=Env::Default()->NowMicros();
    totalReadLockTime[partition]+=afterLockTime-beforeLockTime;
  }
  readDataSize+=strlen(key)+value.size();
  ++straight_reads_;
  mem->Unref();
  if (imm != NULL) imm->Unref();
  if(s.ok()==0){
    //fprintf(stderr, "--after PerformGet,s:%d, key%s, value:%s\n", s.ok(), key, value.c_str());
  }
  //return s;
}

void DBImpl::AnalysisTableKeys(const ReadOptions& options){
  for(int k=0;k<=NewestPartition;k++){
    std::vector<Iterator*> list;
    versions_->current()->AddUnsortedStoreIterators(options, &list,k);
  }
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, 0, &latest_snapshot, &seed, false);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot),
      seed);
}

void DBImpl::NewIterator(const ReadOptions& options, Iterator** iter) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  mutex_.Lock();
  latest_snapshot = versions_->LastSequence();
  for(int k=0;k<=NewestPartition;k++){
    std::vector<Iterator*> list;
    Iterator* internal_iter;
    list.push_back(pmem_[k]->NewIterator());
    //pmem_[k]->Ref();
    if (pimm_[k] != NULL) {
      list.push_back(pimm_[k]->NewIterator());
      //pimm_[k]->Ref();
    }
    versions_->current()->AddIterators(options, &list,&mutex_,k);
    internal_iter =NewMergingIterator(&internal_comparator_, &list[0], list.size());
    seed = ++seed_;
    iter[k]=NewDBIterator(
      this, user_comparator(), internal_iter,
      (options.snapshot != NULL
      ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
      : latest_snapshot),
      seed);
  }
  mutex_.Unlock();
}

Iterator*  DBImpl::NewIterator(const ReadOptions& options, char* beginKey) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  IterState* cleanup = new IterState;//
  int partition=0;
  partition=MapCharKeyToPartition(Slice(beginKey));

  if(versions_->NumLevelFiles(0,partition)>config::triggerSizeBasedMerge){
    #ifdef SIZE_BASED_MERGE
      printf("versions_->NumLevelFiles(0,%d)>config::triggerSizeBasedMerge\n",partition);
     bg_scanCompaction_scheduled_ = true;
     mutex_.Lock();
     MergeUnsortedStoreFilesTogether(partition);
     bg_scanCompaction_scheduled_ = false;
     mutex_.Unlock();
      printf("after versions_->NumLevelFiles(0,%d)>config::triggerSizeBasedMerge\n",partition);
    #endif
  }
//fprintf(stderr,"key:%s,partition:%d\n",beginKey,partition);
  mutex_.Lock();
  latest_snapshot = versions_->LastSequence();
    std::vector<Iterator*> list;
    Iterator* internal_iter;
    if(pmem_[partition]!=NULL){//
      list.push_back(pmem_[partition]->NewIterator());
       pmem_[partition]->Ref();
    }
    if (pimm_[partition] != NULL) {//
      list.push_back(pimm_[partition]->NewIterator());
       pimm_[partition]->Ref();
    }
    versions_->current()->AddIterators(options, &list, partition, beginKey);
    internal_iter =NewMergingIterator(&internal_comparator_, &list[0], list.size());
    //////////////////////////
    versions_->current()->Ref();
    cleanup->mu = &mutex_;
    cleanup->mem = pmem_[partition];
    cleanup->imm = pimm_[partition];
    cleanup->version = versions_->current();
    internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);
    /////////////////////////
    seed = ++seed_;
    mutex_.Unlock();
   return NewDBIterator(
      this, user_comparator(), internal_iter,
      (options.snapshot != NULL
      ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
      : latest_snapshot),
      seed);
}

int DBImpl::NewIterator(const ReadOptions& options, char* beginKey, int scanLength) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  IterState* cleanup = new IterState;
  int partition=0;
  partition=MapCharKeyToPartition(Slice(beginKey));
  mutex_.Lock();
  #ifdef SIZE_BASED_MERGE
  if(versions_->NumLevelFiles(0,partition)>config::triggerSizeBasedMerge){
     printf("versions_->NumLevelFiles(0,%d)>config::triggerSizeBasedMerge\n",partition);
      bg_scanCompaction_scheduled_ = true;
     MergeUnsortedStoreFilesTogether(partition);
     bg_scanCompaction_scheduled_ = false;
     printf("after versions_->NumLevelFiles(0,%d)>config::triggerSizeBasedMerge\n",partition);
  }
  #endif
  //mutex_.Lock();
  latest_snapshot = versions_->LastSequence();
  std::vector<Iterator*> list;
  Iterator* internal_iter;
  if(pmem_[partition]!=NULL){//
    list.push_back(pmem_[partition]->NewIterator());
    pmem_[partition]->Ref();
  }
  if (pimm_[partition] != NULL) {//
    list.push_back(pimm_[partition]->NewIterator());
    pimm_[partition]->Ref();
  }
  versions_->current()->AddIterators(options, &list,partition,beginKey);
  internal_iter =NewMergingIterator(&internal_comparator_, &list[0], list.size());
    //////////////////////////
    versions_->current()->Ref();
    cleanup->mu = &mutex_;
    cleanup->mem = pmem_[partition];
    cleanup->imm = pimm_[partition];
    cleanup->version = versions_->current();
    internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);
    ////////////////////////////////***
    seed = ++seed_;
    Iterator* iter = NewDBIterator(
      this, user_comparator(), internal_iter,
      (options.snapshot != NULL
      ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
      : latest_snapshot),
      seed);

#ifdef SCAN_AIO
  mutex_.Unlock();
  int result=scanAIOreads(iter,partition,beginKey,scanLength);
#endif

#ifdef SCAN_THREADS
  mutex_.Unlock();
  //fprintf(stderr, "----before seekValueParallel\n");
  int result=seekValueParallel(iter,partition,beginKey,scanLength);
#endif

  delete iter;
  iter=NULL;
  return result;
}

void aio_completion_handler(sigval_t sigval){
  struct aiocb *req;
  req = (struct aiocb *)sigval.sival_ptr;
  /* Did the request complete? */
  if (aio_error( req ) == 0) {
    /* Request completed successfully, get the return status */
    int ret = aio_return( req );
  }
  return;
}

int DBImpl::scanAIOreads(Iterator* iter,int partition,char* beginKey,int scanLength){
    int found=0,flag=0;
    iter->Seek(beginKey);
    if(iter->Valid()){
        if(iter->key() == beginKey){
          found++;	
          int count=0;
          std::future<void*> ret[config::maxScanLength];//config::maxScanLength
          int numAIOReqs=0;
          for (int j = 0; j < scanLength && iter->Valid(); j++) {
            Slice value=iter->value();
            //fprintf(stderr, "next key:%s!\n",iter->key().ToString().c_str());
            if(value.size()<config::minValueSize){                    
                  std::string valueStr=value.ToString();
                  std::string delimiter = "&";
                  size_t pos = 0;
                  long length,offset,logPartition,logNumber;
                  std::string::size_type sz;
                  pos=valueStr.find(delimiter);        
                  logPartition=strtoul(valueStr.substr(0, pos).c_str(),NULL,10);                 
                  valueStr.erase(0, pos + delimiter.length());
                  pos=valueStr.find(delimiter);
                  logNumber=strtoul(valueStr.substr(0, pos).c_str(),NULL,10);                 
                  valueStr.erase(0, pos + delimiter.length());
                  pos=valueStr.find(delimiter);
                  offset=strtoul(valueStr.substr(0, pos).c_str(),NULL,10);     
                  valueStr.erase(0, pos + delimiter.length());
                  length=strtoul(valueStr.c_str(),NULL,10);   
                  FILE* readLogfile=globalLogFile[logPartition*config::logFileNum+logNumber];//-beginLogNum
                  int fileID=fileno(readLogfile);
                  //Initialize the necessary fields in the aiocb
                  my_aiocb[numAIOReqs].aio_fildes = fileID;
                  my_aiocb[numAIOReqs].aio_nbytes = length;
                  my_aiocb[numAIOReqs].aio_offset = offset;

                  int ret = aio_read(&my_aiocb[numAIOReqs]);
                  if (ret < 0) perror("aio_read");
                  numAIOReqs++;       
            }
            iter->Next();
          }
          for(int j=0;j<numAIOReqs;j++){
            while ( aio_error( &my_aiocb[j] ) == EINPROGRESS ) ;
          }
          //mutex_.Unlock();
        }
    }	
    return found;
}


int DBImpl::seekValueParallel(Iterator* iter,int partition,char* beginKey,int scanLength){
    int found=0,flag=0;
    iter->Seek(beginKey);
    if(iter->Valid()){
        if(iter->key() == beginKey){
          found++;	
          int count=0;
          std::future<void*> ret[scanLength];
          struct getValuePara getValue[scanLength];//
          for (int j = 0; j < scanLength && iter->Valid(); j++) {
            Slice value=iter->value();
            if(value.size()<config::minValueSize){              
                  //parallelGetValue(dbname_,value.ToString());   
#ifdef SEEK_PREFETCH     
                  if(flag==0){
                    std::string delimiter = "&";
                    size_t pos = 0;
                    long length,offset,logPartition,logNumber;
                    std::string::size_type sz;
                    std::string valueStr=value.ToString();
                    pos=valueStr.find(delimiter);
                    logPartition=strtoul(valueStr.substr(0, pos).c_str(),NULL,10);                 
                    valueStr.erase(0, pos + delimiter.length());
                    pos=valueStr.find(delimiter);
                    logNumber=strtoul(valueStr.substr(0, pos).c_str(),NULL,10);                 
                    valueStr.erase(0, pos + delimiter.length());
                    pos=valueStr.find(delimiter);
                    offset=strtoul(valueStr.substr(0, pos).c_str(),NULL,10);     
                    valueStr.erase(0, pos + delimiter.length());
                    length=strtoul(valueStr.c_str(),NULL,10);   

                    FILE* fetchLogfile=globalLogFile[logPartition*config::logFileNum+logNumber]; //-beginLogNum[logPartition]                 
                    int fileID=fileno(fetchLogfile);       
                    uint64_t scanFetchSize=scanLength*gValueSize*3/4;//*2/4;//config::file_size
                    int ret1=posix_fadvise(fileID,offset,scanFetchSize, POSIX_FADV_WILLNEED);//POSIX_FADV_RANDOM | POSIX_FADV_WILLNEED
                    //printf("seek prefetch ret=%d,fileID=%d\n", ret1,fileID);
                    if(ret1 == -1){
                      printf("posix_fadvise调用失败！\n");
                    }
                    flag=1;
                  }
#endif
                  getValue[count].dbname_=dbname_;
                  getValue[count].valueStr=value.ToString();
                  getValue[count].beginLogNum=beginLogNum[partition];
                  //getValue[count].curLogFile=globalLogFile;
                  //getValue[count].curLogFile=logFile[logPartition*config::logFileNum+logNumber];      
                  ret[count]= Env::Default()->threadPool->addTask(DBImpl::parallelGetValue,(void *) &getValue[count]);
                  count++;                  
            }
            /////////////////////////////////////////
            if(count>=config::maxScanThread){
              for(int k=0;k<count;k++){
                try{
                  ret[k].wait();
                } catch(const std::future_error &e){ std::cerr<<"wait error\n"; }
              }
              count=0;
            }//////////////////////////////////
            iter->Next();
          }   
          //mutex_.Unlock();       
          for(int k=0;k<count;k++){
            try{
              ret[k].wait();
            } catch(const std::future_error &e){ std::cerr<<"wait error\n"; }
          }     
        }
    }
    return found;
}

void* DBImpl::parallelGetValue(void *paraData){
   struct getValuePara *myPara=(struct getValuePara*)paraData;
   std::string dbname_=myPara->dbname_;
   std::string valueStr=myPara->valueStr; 
   int beginLogNum=myPara->beginLogNum;
   std::string delimiter = "&";
   size_t pos = 0;
   long length,offset,logPartition,logNumber;
   std::string::size_type sz;
   pos=valueStr.find(delimiter);
   logPartition=strtoul(valueStr.substr(0, pos).c_str(),NULL,10);                 
   valueStr.erase(0, pos + delimiter.length());
   pos=valueStr.find(delimiter);
   logNumber=strtoul(valueStr.substr(0, pos).c_str(),NULL,10);                 
   valueStr.erase(0, pos + delimiter.length());
   pos=valueStr.find(delimiter);
   offset=strtoul(valueStr.substr(0, pos).c_str(),NULL,10);     
   valueStr.erase(0, pos + delimiter.length());
   length=strtoul(valueStr.c_str(),NULL,10);   

   FILE* readLogfile=globalLogFile[logPartition*config::logFileNum+logNumber];//-beginLogNum
   //fprintf(stderr,"before open log file for read,logPartition:%d,logNumber:%d,Offset:%llu,length:%d!!\n",logPartition,logNumber,offset,length);
   char buffer[config::maxValueSize];            
   int t=fseek(readLogfile,offset,SEEK_SET);
   int num=fread(buffer,sizeof(char),length,readLogfile);
   std::string oneRecord(buffer);  
   //fprintf(stderr,"after open log file for read.oneRecord.size:%d,num:%d,buffer size:%d!!\n",oneRecord.size(),num,strlen(buffer));
 }

void DBImpl::GetReplayTimestamp(std::string* timestamp) {
  uint64_t file = 0;
  uint64_t seqno = 0;
  {
    MutexLock l(&mutex_);
    file = versions_->NewFileNumber();
    versions_->ReuseFileNumber(file);
    seqno = versions_->LastSequence();
  }

  WaitOutWriters();
  timestamp->clear();
  PutVarint64(timestamp, file);
  PutVarint64(timestamp, seqno);
}

void DBImpl::AllowGarbageCollectBeforeTimestamp(const std::string& timestamp) {
  Slice ts_slice(timestamp);
  uint64_t file = 0;
  uint64_t seqno = 0;

  if (timestamp == "all") {
    // keep zeroes
  } else if (timestamp == "now") {
    MutexLock l(&mutex_);
    seqno = versions_->LastSequence();
    if (manual_garbage_cutoff_ < seqno) {
      manual_garbage_cutoff_ = seqno;
    }
  } else if (GetVarint64(&ts_slice, &file) &&
             GetVarint64(&ts_slice, &seqno)) {
    MutexLock l(&mutex_);
    if (manual_garbage_cutoff_ < seqno) {
      manual_garbage_cutoff_ = seqno;
    }
  }
}

bool DBImpl::ValidateTimestamp(const std::string& ts) {
  uint64_t file = 0;
  uint64_t seqno = 0;
  Slice ts_slice(ts);
  return ts == "all" || ts == "now" ||
         (GetVarint64(&ts_slice, &file) &&
          GetVarint64(&ts_slice, &seqno));
}

int DBImpl::CompareTimestamps(const std::string& lhs, const std::string& rhs) {
  uint64_t now = 0;
  uint64_t lhs_seqno = 0;
  uint64_t rhs_seqno = 0;
  uint64_t tmp;
  if (lhs == "now" || rhs == "now") {
    MutexLock l(&mutex_);
    now = versions_->LastSequence();
  }
  if (lhs == "all") {
    lhs_seqno = 0;
  } else if (lhs == "now") {
    lhs_seqno = now;
  } else {
    Slice lhs_slice(lhs);
    GetVarint64(&lhs_slice, &tmp);
    GetVarint64(&lhs_slice, &lhs_seqno);
  }
  if (rhs == "all") {
    rhs_seqno = 0;
  } else if (rhs == "now") {
    rhs_seqno = now;
  } else {
    Slice rhs_slice(rhs);
    GetVarint64(&rhs_slice, &tmp);
    GetVarint64(&rhs_slice, &rhs_seqno);
  }

  if (lhs_seqno < rhs_seqno) {
    return -1;
  } else if (lhs_seqno > rhs_seqno) {
    return 1;
  } else {
    return 0;
  }
}

Status DBImpl::GetReplayIterator(const std::string& timestamp,
                                 ReplayIterator** iter) {
  *iter = NULL;
  Slice ts_slice(timestamp);
  uint64_t file = 0;
  uint64_t seqno = 0;

  if (timestamp == "all") {
    seqno = 0;
  } else if (timestamp == "now") {
    {
      MutexLock l(&mutex_);
      file = versions_->NewFileNumber();
      versions_->ReuseFileNumber(file);
      seqno = versions_->LastSequence();
    }
    WaitOutWriters();
  } else if (!GetVarint64(&ts_slice, &file) ||
             !GetVarint64(&ts_slice, &seqno)) {
    return Status::InvalidArgument("Timestamp is not valid");
  }

  ReadOptions options;
  options.fill_cache = false;
  SequenceNumber latest_snapshot;
  uint32_t seed;
  MutexLock l(&mutex_);
  Iterator* internal_iter = NewInternalIterator(options, file, &latest_snapshot, &seed, true);
  internal_iter->SeekToFirst();
  ReplayIteratorImpl* iterimpl;
  iterimpl = new ReplayIteratorImpl(
      this, &mutex_, user_comparator(), internal_iter, pmem_[0], SequenceNumber(seqno));/////////////////////////
  *iter = iterimpl;
  replay_iters_.push_back(iterimpl);
  return Status::OK();
}

void DBImpl::ReleaseReplayIterator(ReplayIterator* _iter) {
  MutexLock l(&mutex_);
  ReplayIteratorImpl* iter = reinterpret_cast<ReplayIteratorImpl*>(_iter);
  for (std::list<ReplayIteratorImpl*>::iterator it = replay_iters_.begin();
      it != replay_iters_.end(); ++it) {
    if (*it == iter) {
      iter->cleanup(); // calls delete
      replay_iters_.erase(it);
      return;
    }
  }
}

void DBImpl::RecordReadSample(Slice key) {
  /*MutexLock l(&mutex_);
  ++straight_reads_;
  if (versions_->current()->RecordReadSample(key)) {
    bg_compaction_cv_.Signal();
  }*/
}

SequenceNumber DBImpl::LastSequence() {
  SequenceNumber ret;

  {
    MutexLock l(&mutex_);
    ret = versions_->LastSequence();
  }

  WaitOutWriters();
  return ret;
}

const Snapshot* DBImpl::GetSnapshot() {
  const Snapshot* ret;

  {
    MutexLock l(&mutex_);
    ret = snapshots_.New(versions_->LastSequence());
  }

  WaitOutWriters();
  return ret;
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

Status DBImpl::rebuildHashIndex(const ReadOptions& options){
  Status status;
  recoveryHashTable();
  fprintf(stderr,"after recoveryHashTable\n");
  return status;
}

void DBImpl::initValueLogNumber(){
 for(int p=0;p<=NewestPartition;p++){
   int flag=0;
  for(int t=0;;t++){
      std::stringstream LogName;                              
      LogName<< LogPrefixName<<p<<"-"<<t;
      std::string name=LogName.str();          
      FILE* tlogFile=fopen(name.c_str(),"rb+");
      if(tlogFile==NULL){
        if(flag==1){
            LogNum[p]=t;
            break;
        }
        continue;
      }else{
        if(flag==0){
          beginLogNum[p]=t;
          flag=1;
        }
      }
      fclose(tlogFile);
      tlogFile=NULL;
  }
 }
  for(int p=0;p<=NewestPartition;p++){
    fprintf(stderr,"p:%d,beginLogNum:%d,LogNum:%d\n",p,beginLogNum[p],LogNum[p]);
  }
}

void DBImpl::initGlobalLogFile(){
  for(int p=0;p<=NewestPartition;p++){
    for(int i=beginLogNum[p];i<LogNum[p];i++){
        std::stringstream LogName;
        LogName<< LogPrefixName <<p<<"-"<<i;
        std::string name=LogName.str();
        //currentLogfile= fopen(name.c_str(),"wb+");
        int index=p*config::logFileNum+i;//-beginLogNum[p];
        globalLogFile[index]=fopen(name.c_str(),"ab+");
        if(globalLogFile[index]==NULL){
          fprintf(stderr,"open file:%s error!!\n",name.c_str());
        }
    }
  }
}

void DBImpl::initHashIndex(){
  #ifdef CUCKOO_HASH_INDEX
    for(int p=0;p<=NewestPartition;p++){
      CuckooHashIndex[p]=new ListIndexEntry[config::bucketNum];
      //overflowBucket[p]=new ListIndexEntry[config::bucketNum];
      for(int i=0;i<config::bucketNum;i++){
        CuckooHashIndex[p][i].KeyTag[0]=0;
        CuckooHashIndex[p][i].KeyTag[1]=0;
        CuckooHashIndex[p][i].TableNum[0]=0;
        CuckooHashIndex[p][i].TableNum[1]=0;
        CuckooHashIndex[p][i].TableNum[2]=0;
        CuckooHashIndex[p][i].nextEntry=NULL;
        //HashIndex[i].nextEntry=NULL;
      }
    }
  #endif

  #ifdef LIST_HASH_INDEX
    for(int p=0;p<=NewestPartition;p++){
      ListHashIndex[p]=new ListIndexEntry[config::bucketNum];
      for(int i=0;i<config::bucketNum;i++){
        ListHashIndex[p][i].KeyTag[0]=0;
        ListHashIndex[p][i].KeyTag[1]=0;
        ListHashIndex[p][i].TableNum[0]=0;
        ListHashIndex[p][i].TableNum[1]=0;
        ListHashIndex[p][i].TableNum[2]=0;
        ListHashIndex[p][i].nextEntry=NULL;
      }
    }
  #endif
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&writers_mutex_);
  
  Status s;
  Slice key, value;
  updates->GetKeyValue(&key, &value);
  w.partition=MapCharKeyToPartition(key);
  s = SequenceWriteBegin(&w, updates);
  //fprintf(stderr,"after SequenceWriteBegin,key:%s,p:%d\n",myKVPair.key.ToString().c_str(),w.partition);
  double logBeginTime=Env::Default()->NowMicros();
  if (s.ok() && updates != NULL) { // NULL batch is for compactions
    double logEndTime=Env::Default()->NowMicros();
	  totalLogTime[w.partition]+=logEndTime-logBeginTime;
    
    while(requestBatchQueue[w.partition].size()>1000){//10, 50000
      env_->SleepForMicroseconds(1);//5
    }

    requestEntry *wEntry = new struct requestEntry;
    while(!finishCompaction){
        env_->SleepForMicroseconds(1);
    }
    
    wEntry->RWFlag = 1;
    wEntry->Woptions = options;
    updates->GetKeyValueInQueue(&(wEntry->wBatch), treeRoot);
    WriteBatchInternal::SetSequence(&(wEntry->wBatch), w.start_sequence_);
    requestBatchQueue[w.partition].push(wEntry);
    double QueueEndTime=Env::Default()->NowMicros();
    wCostTimeInQueue[w.partition]+=QueueEndTime-logEndTime;
  }
  
  if (!s.ok()) {
    mutex_.Lock();
    RecordBackgroundError(s);
    mutex_.Unlock();
  }
  SequenceWriteEnd(&w);
  return s;
}

void DBImpl::WritePartitionThread(int partition) {
  MutexLock l(&mutex_);
  Status s;
  mutex_.Unlock();
  while (!shutting_down_.Acquire_Load()) {
      //if the log of partition is not empty.
      //do while the log is not empty.
      //Get KV pairs from it's log and write to this partition in background
      while(!requestBatchQueue[partition].empty()){

          struct requestEntry *curEntry = requestBatchQueue[partition].front();
          //fprintf(stderr,"-partition:%d, queue size:%d!\n",partition, requestQueue[partition].size());
          if(curEntry->RWFlag==1){           
            mutex_.Lock();
              double logBeginTime=Env::Default()->NowMicros();
              Status s = log_[partition]->AddRecord(WriteBatchInternal::Contents(&(curEntry->wBatch)));
              //fprintf(stderr,"after AddRecord,key:%s,p:%d\n",key.ToString().c_str(),partition);
              if (s.ok() && curEntry->Woptions.sync) {
                s = logfile_[partition]->Sync();
              }
              double logEndTime=Env::Default()->NowMicros();
	            totalLogTime[partition]+=logEndTime-logBeginTime;
            mutex_.Unlock();
              double memBeginTime=Env::Default()->NowMicros();
              s = WriteBatchInternal::InsertInto(&(curEntry->wBatch), pmem_);
              double MemEndTime=Env::Default()->NowMicros();
              totalMemTableTime[partition]+=MemEndTime-memBeginTime;
          }else{
              //fprintf(stderr,"key:%s\n", (*curEntry).key.c_str());
              PerformGet(curEntry->Roptions, curEntry->key, partition);
          }
          //fprintf(stderr,"--partition:%d, after insert MemTable!\n", partition);
          requestBatchQueue[partition].pop();
          delete curEntry;                     
      }
      
    }
    mutex_.Lock();
    Log(options_.info_log, "cleaning up WritePartitionThread");
    num_bg_threads_ -= 1;
    bg_fg_cv_.SignalAll();
}


Status DBImpl::SequenceWriteBegin(Writer* w, WriteBatch* updates) {
  Status s;
  {
    MutexLock l(&mutex_);
    straight_reads_ = 0;
    bool force = updates == NULL;
    bool enqueue_mem = false;
    w->micros_ = versions_->NumLevelFiles(0,w->partition);
    while (true) {
      if (!bg_error_.ok()) {
        // Yield previous error
        s = bg_error_;
        break;
      } else if (!force && finishCompaction && versions_->NumLevelFiles(0,w->partition)<config::kL0_StopWritesTrigger &&
                 (pmem_[w->partition]->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
                   //&& partitonUnsortedBuffered[w->partition]<config::kL0_CompactionTrigger
        // There is room in current memtable
        // Note that this is a sloppy check.  We can overfill a memtable by the
        // amount of concurrently written data.
        break;
      } else if (pimm_[w->partition] != NULL) {
        // We have filled up the current memtable, but the previous
        // one is still being compacted, so we wait.
	      //fprintf(stderr,"in SequenceWriteBegin bg_fg_cv_.Wait()\n");
        bg_memtable_cv_.Signal();
        //bg_compaction_cv_.Wait();
        bg_fg_cv_.Wait();
        
      }else if(pmem_[w->partition]->ApproximateMemoryUsage() > options_.write_buffer_size){
        // Attempt to switch to a new memtable and trigger compaction of old
	      //fprintf(stderr,"build new MemTable, w->partition:%d\n", w->partition);
        //assert(versions_->PrevLogNumber(w->partition) == 0);
        uint64_t new_log_number = versions_->NewFileNumber();
        ConcurrentWritableFile* lfile = NULL;
        s = env_->NewConcurrentWritableFile(LogFileName(dbname_, new_log_number), &lfile);
        if (!s.ok()) {
          // Avoid chewing through file number space in a tight loop.
          versions_->ReuseFileNumber(new_log_number);
          break;
        }

        for(int k=0;k<logNumVec.size();k++){
          if(logNumVec[k]==logfile_number_[w->partition]){
              logNumVec.erase(logNumVec.begin()+k);
              k--;
          }
	       }
        logImNumVec.push_back(logfile_number_[w->partition]);
        logfileImm_number_[w->partition]=logfile_number_[w->partition];
        logfile_[w->partition].reset(lfile);
        logfile_number_[w->partition]= new_log_number;	  
	      logNumVec.push_back(new_log_number);
       // logfile_number_ = new_log_number;
        log_[w->partition].reset(new log::Writer(lfile));

        pimm_[w->partition] = pmem_[w->partition];
        w->has_imm_= true;
        pmem_[w->partition] = new MemTable(internal_comparator_);
        pmem_[w->partition]->Ref();
        flushedMemTable[w->partition] = false;
        bg_memtable_cv_.Signal();///////////
        force = false; //Do not force another compaction if have room
        enqueue_mem = true;
        break;
      }else{       
        bg_fg_cv_.Wait();
      }
    }

    if (s.ok()) {
      w->log_[w->partition] = log_[w->partition];
      w->logfile_[w->partition] = logfile_[w->partition];
      w->mem_[w->partition] = pmem_[w->partition];
      pmem_[w->partition]->Ref();
    }

    if (enqueue_mem) {
      for (std::list<ReplayIteratorImpl*>::iterator it = replay_iters_.begin();
          it != replay_iters_.end(); ++it) {
        (*it)->enqueue(pmem_[w->partition], w->start_sequence_);
      }
    }
  }

  if (s.ok()) {
    uint64_t diff = updates ? WriteBatchInternal::Count(updates) : 0;
    uint64_t ticket = 0;
    w->linked_ = true;
    w->next_ = NULL;

    //writers_mutex_.Lock();
    if (writers_tail_) {
      writers_tail_->next_ = w;
      w->prev_ = writers_tail_;
    }
    writers_tail_ = w;
    ticket = __sync_add_and_fetch(&writers_upper_, 1 + diff);
    while (w->block_if_backup_in_progress_ &&
           backup_in_progress_.Acquire_Load()) {
      w->wake_me_when_head_ = true;
      w->cv_.Wait();
      w->wake_me_when_head_ = false;
    }
    //writers_mutex_.Unlock();
    w->start_sequence_ = ticket - diff;
    w->end_sequence_ = ticket;
  }

  return s;
}
void DBImpl::SequenceWriteEnd(Writer* w) {
  if (!w->linked_) {
    return;
  }
  versions_->SetLastSequence(w->end_sequence_);
  //writers_mutex_.Lock();
  if (w->prev_) {
    w->prev_->next_ = w->next_;
    if (w->has_imm_) {
      w->prev_->has_imm_ = true;
      w->has_imm_ = false;
    }
  }
  if (w->next_) {
    w->next_->prev_ = w->prev_;
    // if we're the head and we're setting someone else to be the head who wants
    // to be notified when they become the head, signal them.
    if (w->next_->wake_me_when_head_ && !w->prev_) {
      w->next_->cv_.Signal();
    }
  }
  if (writers_tail_ == w) {
    assert(!w->next_);
    writers_tail_ = NULL;
  }
  //writers_mutex_.Unlock();

  if (w->has_imm_ && !w->prev_) {
    has_imm_[w->partition].Release_Store(pimm_[w->partition]);
    w->has_imm_ = false;
    bg_memtable_cv_.Signal();
  }

}

void DBImpl::WaitOutWriters() {
  Writer w(&writers_mutex_);
  writers_mutex_.Lock();
  if (writers_tail_) {
    writers_tail_->next_ = &w;
    w.prev_ = writers_tail_;
  }
  writers_tail_ = &w;
  w.linked_ = true;
  w.next_ = NULL;
  while (w.prev_) {
    w.wake_me_when_head_ = true;
    w.cv_.Wait();
  }
  assert(!w.prev_);
  if (w.next_) {
    w.next_->prev_ = NULL;
    // if we're the head and we're setting someone else to be the head who wants
    // to be notified when they become the head, signal them.
    if (w.next_->wake_me_when_head_) {
      w.next_->cv_.Signal();
    }
  }
  if (writers_tail_ == &w) {
    assert(!w.next_);
    writers_tail_ = NULL;
  }
  writers_mutex_.Unlock();

  if (w.has_imm_) {
    mutex_.Lock();
    has_imm_[w.partition].Release_Store(pimm_[w.partition]);
    w.has_imm_ = false;
    bg_memtable_cv_.Signal();
    mutex_.Unlock();
  }
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();
  //fprintf(stderr,"------in GetProperty:%s\n",property.ToString().c_str());
  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("unikv.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());
  
  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels+config::kTempLevel) {
      return false;
    } else {
      	int sum=0;
       for(int k=0;k<=NewestPartition;k++){
          int num=versions_->NumLevelFiles(static_cast<int>(level),k);
          sum=sum+num;
       }
      char buf[100];
     // snprintf(buf, sizeof(buf), "%d",
     //          versions_->NumLevelFiles(static_cast<int>(level)));
       snprintf(buf, sizeof(buf), "%d",sum);
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Partition  Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    //printf("%s", buf);
    value->append(buf);
    
     for(int k=0;k<=NewestPartition;k++){
      for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
        int files = versions_->NumLevelFiles(level,k);
        if (stats_[k][level].micros > 0 || files > 0) {
          snprintf(
              buf, sizeof(buf),
              "%3d  %6d %8d %8.3f %9.3f %8.3f %9.3f\n",
              k,
              level,
              files,
              versions_->NumLevelBytes(level,k) / 1048576.0,
              stats_[k][level].micros / 1e6,
              stats_[k][level].bytes_read / 1048576.0,
              stats_[k][level].bytes_written / 1048576.0);
          value->append(buf);
	      }
      }
      snprintf(buf, sizeof(buf),
             "--------------------------------------------------\n"
             );
      value->append(buf);
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    Slice starKey=k1.user_key();
    Slice endKey=k2.user_key();
    int partiton=MapCharKeyToPartition(starKey);
    int partiton1=MapCharKeyToPartition(endKey);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1,partiton);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2,partiton1);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

Status DBImpl::LiveBackup(const Slice& _name) {
  Slice name = _name;
  size_t name_sz = 0;

  for (; name_sz < name.size() && name.data()[name_sz] != '\0'; ++name_sz)
      ;

  name = Slice(name.data(), name_sz);
  std::set<uint64_t> live;

  {
    MutexLock l(&writers_mutex_);
    backup_in_progress_.Release_Store(this);
    while (backup_waiter_has_it_) {
      ++backup_waiters_;
      backup_cv_.Wait();
      --backup_waiters_;
    }
    backup_waiter_has_it_ = true;
  }

  Writer w(&writers_mutex_);
  w.block_if_backup_in_progress_ = false;
  SequenceWriteBegin(&w, NULL);

  {
    MutexLock l(&writers_mutex_);
    Writer* p = &w;
    while (p->prev_) {
      p = p->prev_;
    }
    while (p != &w) {
      assert(p);
      p->block_if_backup_in_progress_ = false;
      p->cv_.Signal();
      p = p->next_;
    }
    while (w.prev_) {
      w.wake_me_when_head_ = true;
      w.cv_.Wait();
    }
  }

  {
    MutexLock l(&mutex_);
    versions_->SetLastSequence(w.end_sequence_);
    while (bg_log_occupied_) {
      bg_log_cv_.Wait();
    }
    bg_log_occupied_ = true;
    // note that this logic assumes that DeleteObsoleteFiles never releases
    // mutex_, so that once we release at this brace, we'll guarantee that it
    // will see backup_in_progress_.  If you change DeleteObsoleteFiles to
    // release mutex_, you'll need to add some sort of synchronization in place
    // of this text block.
    versions_->AddLiveFiles(&live);
  }

  Status s;
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  std::string backup_dir = dbname_ + "/backup-" + name.ToString() + "/";

  if (s.ok()) {
    s = env_->CreateDir(backup_dir);
  }

  uint64_t number;
  FileType type;

  for (size_t i = 0; i < filenames.size(); i++) {
    if (!s.ok()) {
      continue;
    }
    if (ParseFileName(filenames[i], &number, &type)) {
      std::string src = dbname_ + "/" + filenames[i];
      std::string target = backup_dir + "/" + filenames[i];
      switch (type) {
        case kLogFile:
        case kDescriptorFile:
        case kCurrentFile:
        case kInfoLogFile:
          s = env_->CopyFile(src, target);
          break;
        case kTableFile:
          // If it's a file referenced by a version, we have logged that version
          // and applied it.  Our MANIFEST will reflect that, and the file
          // number assigned to new files will be greater or equal, ensuring
          // that they aren't overwritten.  Any file not in "live" either exists
          // past the current manifest (output of ongoing compaction) or so far
          // in the past we don't care (we're going to delete it at the end of
          // this backup).  I'd rather play safe than sorry.
          //
          // Under no circumstances should you collapse this to a single
          // LinkFile without the conditional as it has implications for backups
          // that share hardlinks.  Opening an older backup that has files
          // hardlinked with newer backups will overwrite "immutable" files in
          // the newer backups because they aren't in our manifest, and we do an
          // open/write rather than a creat/rename.  We avoid linking these
          // files.
          if (live.find(number) != live.end()) {
            s = env_->LinkFile(src, target);
          }
          break;
        case kTempFile:
        case kDBLockFile:
          break;
        default:
          break;
      }
    }
  }

  {
    MutexLock l(&mutex_);
    if (s.ok() && backup_deferred_delete_) {
      for(int k=0;k<=NewestPartition;k++){
        DeleteObsoleteFiles(k);
      }
    }
    backup_deferred_delete_ = false;
    bg_log_occupied_ = false;
    bg_log_cv_.Signal();
  }

  {
    MutexLock l(&writers_mutex_);
    backup_waiter_has_it_ = false;
    if (backup_waiters_ > 0) {
      backup_in_progress_.Release_Store(this);
      backup_cv_.Signal();
    } else {
      backup_in_progress_.Release_Store(NULL);
    }
  }

  SequenceWriteEnd(&w);
  return s;
}
WriteBatch mybatch;
// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  Status s;
  mybatch.Put(key, value);
  if(WriteBatchInternal::Count(&mybatch)<32){//8, 32,64,256
    //fprintf(stderr,"batch size:%d\n",batchCount);
    return s;
  }
  s = Write(opt, &mybatch);
  //Status s = InsertRequestQueue(opt, key, value);
  mybatch.Clear();
  return s;
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;
  DBImpl* impl = new DBImpl(options, dbname);
  fprintf(stderr,"in open,after new DBImpl\n");
  impl->mutex_.Lock();
  VersionEdit edit;
// Recover handles create_if_missing, error_if_exists
  double recoveryBeginTime=Env::Default()->NowMicros();
  //bool save_manifest = false;
  ///////////////////////////////
  int totalPartitions=impl->recoveryB_Tree(impl->treeRoot);
  if(totalPartitions>0){
      impl->NewestPartition=totalPartitions-1;
  }else{
      impl->NewestPartition=totalPartitions;
  }
  printf("after rebuildTree,totalPartitions,:%d,NewestPartition:%d\n",totalPartitions,impl->NewestPartition);
  impl->treeRoot->printfTree();
   impl->initHashIndex();
  Status s1=impl->rebuildHashIndex(ReadOptions());/////////////////////
  double recoveryIndexTime=Env::Default()->NowMicros();
  printf("recoveryIndexTime:%lf\n",recoveryIndexTime-recoveryBeginTime);
  fprintf(stderr,"recoveryIndexTime:%lf\n",recoveryIndexTime-recoveryBeginTime);

  Status s = impl->Recover(&edit); // Handles create_if_missing, error_if_exists
  fprintf(stderr,"in open,after impl->Recover,s.ok:%d\n",s.ok());
  int emptyDB=impl->versions_->initFileMetadataAndprefetchPinterFile(impl->NewestPartition+1);
  if(!emptyDB){
    std::cerr<<"Not emptyDB, initValueLogNumber\n";
    impl->initValueLogNumber();
  }
  impl->initGlobalLogFile();

  ConcurrentWritableFile* lfile[config::kNumPartition]={NULL};

for(int i=1;i<=impl->NewestPartition;i++){
    impl->setUpWritePartitionThreads(i);//////
}

for(int i=0;i<=impl->NewestPartition;i++){
  if (s.ok() && impl->pmem_[i] == NULL) {
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    s = options.env->NewConcurrentWritableFile(LogFileName(dbname, new_log_number),
                                               &lfile[i]);
    fprintf(stderr,"in open,after NewConcurrentWritableFile\n");
    if (s.ok()) {
      edit.SetLogNumber(new_log_number,i);
      //edit.SetLogNumber(new_log_number);
      impl->logfile_[i].reset(lfile[i]);
      impl->logfile_number_[i] = new_log_number;
      impl->log_[i].reset(new log::Writer(lfile[i]));
      impl->pmem_[i]=new MemTable(impl->internal_comparator_);
      impl->pmem_[i]->Ref();
      std::cerr<<"before new MemTable\n";
      impl->memBaseSize=impl->pmem_[i]->ApproximateMemoryUsage();

      impl->logNumVec.push_back( impl->logfile_number_[i]);
      edit.SetPrevLogNumber(impl->logfileImm_number_[i],i);  // No older logs needed after recovery.
    	edit.SetLogNumber(impl->logfile_number_[i],i);

      fprintf(stderr,"in open,before LogAndApply\n"); 
	    s = impl->versions_->LogAndApply(&edit, &impl->mutex_, &impl->bg_log_cv_, &impl->bg_log_occupied_,i);
    }
  }
  }
    if (s.ok()) {
      //impl->DeleteObsoleteFiles();
      for(int i=0;i<=impl->NewestPartition;i++){
	      impl->DeleteObsoleteFiles(i);
	      impl->pending_outputs_[i].clear();
      }
      impl->bg_compaction_cv_.Signal();
      impl->bg_memtable_cv_.Signal();
    }
    
  double recoveryEndTime=Env::Default()->NowMicros();
  printf("recoveryTime:%lf\n",recoveryEndTime-recoveryIndexTime);
  fprintf(stderr,"recoveryTime:%lf\n",recoveryEndTime-recoveryIndexTime);

  //impl->pending_outputs_.clear();
  impl->allow_background_activity_ = true;
  impl->bg_compaction_cv_.SignalAll();
  impl->bg_memtable_cv_.SignalAll();
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
    impl = NULL;
  }
  if (impl) {
    impl->writers_mutex_.Lock();
    impl->writers_upper_ = impl->versions_->LastSequence();
    impl->writers_mutex_.Unlock();
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
