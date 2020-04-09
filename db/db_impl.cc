// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

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
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "unikv/db.h"
#include "unikv/env.h"
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

using namespace std;
namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  int partition;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
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
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs[2];

  // State kept for output being generated
  WritableFile* outfile[2];
  TableBuilder* builder[2];
  uint64_t total_bytes;

  Output* current_output(int i) { return &outputs[i][outputs[i].size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        //outfile(NULL),
        //builder(NULL),     
        total_bytes(0) {
          
      outfile[0]=NULL;
      outfile[1]=NULL;
      builder[0]=NULL;
      builder[1]=NULL;
  }
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
  result.filter_policy =(src.filter_policy != NULL) ? ipolicy : NULL;
  result.write_buffer_size=67108864;
  result.max_file_size=2097152;

  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
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
   // fprintf(stdout, "block_cache size:    %llu\n", result.block_cache);
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
     /*internal_filter_policy_(NULL),
     filter_policy_half(NewBloomFilterPolicy(options_.bloom_bits/2)),
     filter_policy_add(NewBloomFilterPolicy(options_.bloom_bits*2)),*/
     internal_filter_policy_half(filter_policy_half),
     internal_filter_policy_add(filter_policy_add),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      seed_(0),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      bg_gc_scheduled_(false),
      manual_compaction_(NULL) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);

  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_);
  LogPrefixName=dbname_+"/" + "logfile-";
  printf("LogPrefixName:%s\n",LogPrefixName.c_str());
  //internal_filter_policy_half=new InternalFilterPolicy(filter_policy_half);
 // internal_filter_policy_add=new InternalFilterPolicy(filter_policy_add);
   for(int i=0;i<config::kNumPartition;i++){
      LogNum[i]=0;
      beginLogNum[i]=0;
      continueFlushBytes[i]=0;
      logfile_[i]=NULL;
      logfile_number_[i]=0;
      logfileImm_number_[i]=0;
      log_[i]=NULL;
      pmem_[i]=NULL;
      pimm_[i]=NULL;
      has_imm_[i].Release_Store(NULL);
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
  totalLogTime=0,totalCacheTime=0,totalFlushTime=0,totalCompactTime=0;
  totalReadMem=0,totalReadL0=0,totalReadLn=0,totalReadOther=0;totalGetCostLn=0;
  readDataSizeActual=0;
  finishCompaction=true;
  ampliFactor = 1.0;
  continueSplitCounts =0;
  doCompact=false;
  bg_scanCompaction_scheduled_=false;
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
 // shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_ || bg_gc_scheduled_ ){
    bg_cv_.Wait();
  }
  Status s;
  printf("NewestPartition:%d\n",NewestPartition);
  for(int i=0;i<=NewestPartition;i++){
      if(pimm_[i]!=NULL ){
	      CompactMemTable(i); 
	      //printf("after CompactIMemTable!!\n");
      }
     if(pmem_[i]!=NULL ){
      	pimm_[i] = pmem_[i];
	      has_imm_[i].Release_Store(pimm_[i]);
	      CompactMemTable(i);
	      //printf("after CompactMemTable!!\n");
      }
  }
  printf("totalLogTime:%lf,totalCacheTime:%lf,totalFlushTime:%lf,totalMergeTime:%lf\n",totalLogTime,totalCacheTime,totalFlushTime,totalCompactTime);
  printf("totalReadMem:%lf,totalReadL0:%lf,totalReadLn:%lf,totalCostGetLn:%lf,totalReadOther:%lf\n",totalReadMem,totalReadL0,totalReadLn,totalGetCostLn,totalReadOther);
  persistentHashTable();
  printf("Get in Memtable:%d,IMemtable:%d\n",readMem,readImm);
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  mutex_.Unlock();
  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }
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
 // treeRoot->printfTree();
  persistentB_Tree();
  treeRoot->destroyTree();
  delete versions_;
  delete filter_policy_half;
  delete filter_policy_add;
  /*delete internal_filter_policy_half;
  delete internal_filter_policy_add;*/
  /*for(int i=0;i<config::kNumPartition;i++){
     if(pmem_[i] != NULL){
        pmem_[i]->Unref();
    }
    if(pimm_[i] != NULL){
       pimm_[i]->Unref();
    } 
  }*/
  for(int i=0;i<=NewestPartition;i++){
    if(log_[i]!=NULL){
     delete log_[i];
    }
    if(logfile_[i]!=NULL){
     delete logfile_[i];
    }
  }
  delete tmp_batch_;
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
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
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
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }
  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_[partition];
  versions_->AddLiveFiles(&live);
  //fprintf(stderr, "after versions_->AddLiveFiles\n");
  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number == versions_->LogNumber(partition)) ||
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
      }
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
        if(!keep){
          //fprintf(stderr,"delete file:%d,filenames[i]:%s\n",number,filenames[i].c_str());
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

Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest) {
    //return Status();
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
  //versions_->initVersion();
  //std::cerr<<"before versions_->Recover\n";
  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);
  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  uint64_t min_log=versions_->LogNumber(0);
  uint64_t prev_log=versions_->PrevLogNumber(0);
  for(int k=0;k<=NewestPartition;k++){
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
  //for(int k=0;k<=NewestPartition;k++){  
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number >= prev_log))){
        fprintf(stderr,"logs number:%u,partition:%d\n",number,0);
        printf("logs number:%u,partition:%d\n",number,0);
        logs.push_back(number);
      }
    }
 //}
  }
  if (!expected.empty()) {
    //char buf[50];
    //snprintf(buf, sizeof(buf), "%d missing files; e.g.",
    //	static_cast<int>(expected.size()));
    //return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
    set<uint64_t>::iterator itSet=expected.begin(); 
    for(;itSet!=expected.end();itSet++){
          int filePartition=-1,fileLevel=-1;
          versions_->fileLocation(*itSet, NewestPartition+1, &filePartition, &fileLevel);
          if(filePartition>=0 && fileLevel>=0){
            fprintf(stderr,"delete invalid number:%d in partition:%d,level:%d\n",*itSet,filePartition,fileLevel);
            edit->DeleteFile(fileLevel,filePartition, *itSet);///////delete old SSTable File after compaction
          }
    }
  }
  // Recover in the order in which the logs were generated
  fprintf(stderr,"before RecoverLogFile,log.size:%d\n",logs.size());
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
      s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
            &max_sequence);
      if (!s.ok()) {
        return s;
      }
      // The previous incarnation may not have written any MANIFEST
      // records after allocating this log number.  So we manually
      // update the file number allocation counter in VersionSet.
      versions_->MarkFileNumberUsed(logs[i]);
  }
  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }
  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
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
  printf(" recovery,log_number:%lu,fname:%s\n",log_number,fname.c_str());
  // We intentionally make log::Reader do checksumming even if
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
  MemTable* mem [config::kNumPartition]= {NULL};
  for(int i=0;i<=NewestPartition;i++){
    if (mem[i] == NULL) {
      mem[i] = new MemTable(internal_comparator_);
      mem[i]->Ref();
    }
  }
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);
    Slice key;
    batch.GetKeyValue(&key);
    int partition=MapCharKeyToPartition(key);
    //batch.putPartitionNumber(partition);
    //fprintf(stderr,"batch size:%d, k:%s,partition:%d\n",batch.ApproximateSize(), key.ToString().substr(0,24).c_str(),partition);
    if (mem[partition] == NULL) {
      mem[partition] = new MemTable(internal_comparator_);
      mem[partition]->Ref();
    }
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
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem[partition], edit, NULL,partition);
      mem[partition]->Unref();
      mem[partition] = NULL;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }
  delete file;
  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    for(int i=0;i<=NewestPartition;i++){
        assert(logfile_[i] == NULL);
        assert(log_[i]== NULL);
        assert(mem_ == NULL);
        uint64_t lfile_size;
        if (env_->GetFileSize(fname, &lfile_size).ok() && env_->NewAppendableFile(fname, &logfile_[i]).ok()) {
          Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
          log_[i]= new log::Writer(logfile_[i], lfile_size);
          logfile_number_[i]= log_number;
          logNumVec.push_back(log_number);
          if (mem[i] != NULL) {
            pmem_[i] = mem[i];
            mem[i] = NULL;
          } else {
            // mem can be NULL if lognum exists but was empty.
            pmem_[i] = new MemTable(internal_comparator_);
            pmem_[i]->Ref();
          }
        }
    }
  }
 for(int i=0;i<config::kNumPartition;i++){
  if (mem[i] != NULL) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem[i], edit, NULL,i);
    }
    mem[i]->Unref();
  }
 }
  return status;
}

//record the location of KV pairs in hash index
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
      //	printf( "intKey:%u,hashKey:%u,bucket:%d,%s,keyBytes[2]:%u,keyBytes[3]:%u\n",intKey,hashKey,keyBucket,(char*)myKey.ToString().substr(0,config::kKeyLength).c_str(),(unsigned int )keyBytes[2],(unsigned int )keyBytes[3]);
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
          //keyBucket=curHashfunc.cuckooHash((char*)key.ToString().substr(0,config::kKeyLength).c_str(),config::cuckooHashNum-1,config::kKeyLength);
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
    //delete iter;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base,int partition) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  meta.partition=partition;
  pending_outputs_[partition].insert(meta.number);
  Iterator* iter = mem->NewIterator();/////
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);
  //struct recordIndexPara recordIndex;
  recordIndex.number=meta.number;
  recordIndex.memIter=mem->NewIterator();
#ifdef LIST_HASH_INDEX
  recordIndex.curHashIndex=ListHashIndex[partition];
#endif

#ifdef CUCKOO_HASH_INDEX
  recordIndex.curHashIndex=CuckooHashIndex[partition];
#endif
  //record the location of KV pairs in hash index
  std::future<void*> ret1= Env::Default()->threadPool->addTask(DBImpl::recordHashIndex,(void *) &recordIndex); 
  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);//write MemTable to sstable in Level0
    mutex_.Lock();
  }
  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  pending_outputs_[partition].erase(meta.number);
  //*********************SSTable information be added to the manifest***
  // Note that if file_size is zero, the file has been deleted and should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0){
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    edit->AddFile(level, meta.number, meta.partition,meta.file_size,
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
  writeDataSizeActual=writeDataSizeActual+meta.file_size;
  writeDataSize+=meta.file_size;
  stats_[partition][level].Add(stats);
  return s;
}

//persistent hash index into a disk file
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

//recovery the hash index from a disk file
void DBImpl::recoveryHashTable(){
  ifstream hashFile;
  hashFile.open(config::HashTableFile);
  if (!hashFile){ 
      printf("open fail\n");     
      fprintf(stderr,"open fail\n");     
  }
  unsigned int partitionNumber=0,tag0,tag1,fileID,bucketNumber=0;
  char temp;
#ifdef LIST_HASH_INDEX
    while(hashFile>>partitionNumber){
            hashFile>>temp>>bucketNumber >> temp >> tag0 >> temp>>tag1>>temp>>fileID>>temp;
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
        hashFile>>temp>>bucketNumber >> temp >> tag0 >> temp>>tag1>>temp>>fileID>>temp;
            //fprintf(stderr,"partitionNumber:%d,bucketNumber:%d,tag0:%d,tag1:%d,fileID:%d\n",partitionNumber,bucketNumber,tag0,tag1,fileID);
            byte* tableNumBytes=new byte[2];
            intTo2Byte(fileID,tableNumBytes);
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
            // printf("addEntry bucket:%u,tag0:%u,tag1:%u,fileID:%u\n",bucketNumber,addEntry->KeyTag[0],addEntry->KeyTag[1],bytes3ToInt(addEntry->TableNum));
          }
          delete []tableNumBytes;
      } 
#endif
  hashFile.close();
}

//persistent the partition index into a disk file
void DBImpl::persistentB_Tree(){
  TreeFile.open(config::B_TreeFile);
  if(treeRoot->getLeftChild()!=NULL && treeRoot->getRightChild()!=NULL){
    treeRoot->persistentB_Tree(&TreeFile);
  }
  TreeFile.close();
}

//recovery the partition index from a disk file
int DBImpl::recoveryB_Tree(TreeNode* Root){
  ifstream treeFile;
  int totalPartition=0,partition,addPartition=0;
  char k[3]="k:",p[3]="p:";
  string str,indexKey,tagChar,temp;
  treeFile.open(config::B_TreeFile);
  if (!treeFile){ 
      printf("open fail\n");
     // return;
  } 
  if(getline(treeFile,str)){
    tagChar=str.substr(0,1);
    indexKey=str.substr(2,str.length()-2);
    Root->setIndexCharKey((char*)indexKey.c_str());
    Root->setLeafFlag(0);   
    //getline(treeFile,str);
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

//flush MemTable to UnsortedStore
void DBImpl::CompactMemTable(int partition) {
  mutex_.AssertHeld();
  double flushBeginTime=Env::Default()->NowMicros();
  assert(pimm_[partition] != NULL);
  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  Status s = WriteLevel0Table(pimm_[partition], &edit, base,partition);//write MemTable to SSTable in UnsortedStore
  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }
  // Replace immutable memtable with the generated Table
  if (s.ok()) {//MemTable write to UnsortedStore successfully
    edit.SetPrevLogNumber(logfileImm_number_[partition],partition);
    edit.SetLogNumber(logfile_number_[partition],partition); 
    s = versions_->LogAndApply(&edit, &mutex_,partition);//record edit into manifest log
  }
  for(int k=0;k<logImNumVec.size();k++){
	  if(logImNumVec[k]==logfileImm_number_[partition]){
      logImNumVec.erase(logImNumVec.begin()+k);
      k--;
	  }
	}
  
  if (s.ok()) {// Commit to the new state
    pimm_[partition]->Unref();
    pimm_[partition] = NULL;
    has_imm_[partition].Release_Store(NULL);
    DeleteObsoleteFiles(partition);
  } else {
    RecordBackgroundError(s);
  }
  double flushEndTime=Env::Default()->NowMicros();
  totalFlushTime+=flushEndTime-flushBeginTime;
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end,int partition) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels+config::kTempLevel; level++) {
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

void DBImpl::TEST_CompactRange(int level, const Slice* begin,const Slice* end) {
  assert(level >= 0);
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
      MaybeScheduleCompaction(0);
    } else {  // Running either my compaction or another compaction.
      bg_cv_.Wait();
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
      bg_cv_.Wait();
    }
    if (pimm_[0] != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}


void DBImpl::MaybeScheduleCompaction(int partition) {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (pimm_[partition] == NULL &&
             manual_compaction_ == NULL &&
             !versions_->NeedsCompaction(partition)) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    compactPartition=partition;
    env_->Schedule(&DBImpl::BGWork, this);//BGWork() schedule in background
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}


void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
     printf("in bg_error_,partition:%d\n",compactPartition);
  } else {
    BackgroundCompaction(compactPartition);
  }
  bg_compaction_scheduled_ = false;
  //Previous compaction may have produced too many files in a level,so reschedule another compaction if needed.
  //MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}

void DBImpl::BackgroundCompaction(int partition) {
  mutex_.AssertHeld();
  bool doSplit=false;
  if (pimm_[partition] != NULL) {
    CompactMemTable(partition);
    //return when the SSTable number of UnsortedStore smaller than the compaction throhold
	  if(versions_->NumLevelFiles(0,partition) < config::kL0_CompactionTrigger*ampliFactor || bg_scanCompaction_scheduled_){
      return;
	  }
  }
  if(versions_->NumLevelFiles(0,partition) < config::kL0_CompactionTrigger*ampliFactor || bg_scanCompaction_scheduled_){
	  return;
  }
   doCompact=true;
   uint64_t curPartitionSize=versions_->TotalPartitionBytes(partition,gKeySize,gValueSize);
   continueFlushBytes[partition]=continueFlushBytes[partition]+versions_->flushL0Bytes(partition);
   //fprintf(stderr,"curPartitionSize:%llu\n",curPartitionSize);
   uint64_t splitLimit=config::kSplitBytes;
   if(gValueSize>16000){// || gValueSize<500
     if(gValueSize>64000){
       splitLimit+=26106127360;
     }else{
      splitLimit+=20106127360;
     }
   }
   if(continueSplitCounts>3){
      splitLimit+=25737418240;//20737418240
      printf("add splitLimit to:%llu\n",splitLimit);
   }
   printf("splitLimit:%llu, curPartitionSize:%llu,continueFlushBytes:%llu, continueSplitCounts:%d\n",splitLimit,curPartitionSize,continueFlushBytes[partition],continueSplitCounts);
   if(curPartitionSize>=splitLimit){
      doSplit=true;
      NewestPartition++;
      finishCompaction=false;
      printf("doSplit=true, pmem_[partition]!=NULL,curPartitionSize:%llu\n",curPartitionSize);
      //if(pmem_[partition]!=NULL && pmem_[partition]->ApproximateMemoryUsage()>memBaseSize){
      if(pmem_[partition]!=NULL){
        uint64_t new_log_number = versions_->NewFileNumber();
        WritableFile* lfile = NULL;
        Status s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
        if (!s.ok()) {
          // Avoid chewing through file number space in a tight loop.
          versions_->ReuseFileNumber(new_log_number);
        }
        delete log_[partition];
        delete logfile_[partition];
        for(int k=0;k<logNumVec.size();k++){
          if(logNumVec[k]==logfile_number_[partition]){
            logNumVec.erase(logNumVec.begin()+k);
            k--;
          }
        }
        logImNumVec.push_back(logfile_number_[partition]);
        logfileImm_number_[partition]=logfile_number_[partition];
        printf("split,buid new logfile_number_:%u,partition:%d\n",new_log_number,partition);
        logfile_[partition]= lfile;
        logfile_number_[partition]= new_log_number;	  
        logNumVec.push_back(new_log_number);
        log_[partition]= new log::Writer(lfile);//new logfile
        pimm_[partition] = pmem_[partition];
        has_imm_[partition].Release_Store(pimm_[partition]);
        pmem_[partition] = new MemTable(internal_comparator_);//new MemTable
        pmem_[partition]->Ref();
        //fprintf(stderr,"split,buid new logfile_number_:%u,partition:%d\n",new_log_number,partition);
        CompactMemTable(partition);
      }
    }

  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end,partition);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    fprintf(stderr, "@@@@ManualCompaction in level %d\n",m->level);
    fprintf(stderr,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
	    c = versions_->PickAllCompaction(partition);
	    //fprintf(stderr, "Level:%d,num:%d, Level:%d,num:%d, Level:%d,num:%d!!!!\n",c->level(),c->num_input_files(0),c->level()+1,c->num_input_files(1),c->level()+2,c->num_input_files(2));//--
  }
  Status status;
  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {//trivial Compaction，move to next level directly 
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(),partition, f->number);
    c->edit()->AddFile(c->level() + 1, f->number,f->partition, f->file_size,
                       f->smallest, f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_,partition);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  }else {  
     CompactionState* compact = new CompactionState(c);
     printf("before DoCompactionWork NumFiles L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),versions_->NumLevelFiles(config::kNumLevels+config::kTempLevel-1,partition));
     //fprintf(stderr,"before DoCompactionWork NumFiles L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),versions_->NumLevelFiles(config::kNumLevels+config::kTempLevel-1,partition));
     status = DoCompactionWork(compact,partition,doSplit);//merge KV pairs from UnsortedStore into SortedStore
     DeleteInvalidFiles(compact->compaction->GetAllInput());
     LogNum[partition]++;
     //fprintf(stderr,"after DoCompactionWork NumFiles L0:%d ,L1:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition));
      if (!status.ok()) {
        RecordBackgroundError(status);
      }
     CleanupCompaction(compact,partition);
     c->ReleaseInputs();
     //DeleteObsoleteFiles(partition); 
  }
  delete c;
  //trigger GC in partition after split it
    if(continueFlushBytes[partition]>=config::kcontinueWriteBytes && curPartitionSize>=config::kGCBytes && !doSplit && config::kGCBytes!=config::kSplitBytes){
        bg_gc_scheduled_=true;
        finishCompaction=true;
        bg_cv_.SignalAll();
        printf("before doGCWithinPartition NumFiles L0:%d ,L1:%d,beginLogNum:%d,LogNum:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),beginLogNum[partition],LogNum[partition]);
        //fprintf(stderr,"before doBackgroundGC NumFiles L0:%d ,L1:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition));        
        doGCWithinPartition(partition,LogNum[partition]);
        beginLogNum[partition]=LogNum[partition]-1;
        LogNum[partition]++;
        //LogNum[NewestPartition]++;
        printf("after doBackgroundGC NumFiles L0:%d ,L1:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition));
        //fprintf(stderr,"after doBackgroundGC NumFiles L0:%d ,L1:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition));
        bg_gc_scheduled_=false;
        continueFlushBytes[partition]=0;
    }
    
    if(doSplit){
        bg_gc_scheduled_=true;
        finishCompaction=true;
        bg_cv_.SignalAll();
        printf("before doBackgroundGC NumFiles L0:%d ,L1:%d,beginLogNum:%d,LogNum:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),beginLogNum[partition],LogNum[partition]);
        doBackgroundGC(partition,LogNum[partition]);//operate GC in log files
        beginLogNum[partition]=LogNum[partition]-1;
        LogNum[partition]++;
        LogNum[NewestPartition]++;
        //DeleteObsoleteFiles(partition);
        printf("after doBackgroundGC NumFiles L0:%d ,L1:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition));
        bg_gc_scheduled_=false;
        continueSplitCounts++;
    }
    //bg_cv_.SignalAll();
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
}

//operate GC in log files
void DBImpl::doBackgroundGC(int curPartition,int LogSegments){
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
        int index=curPartition*config::logFileNum+i;//-beginLogNum[curPartition];
        readLogfile[i]=globalLogFile[index];
        if( readLogfile[i]==NULL){
            fprintf(stderr,"curPartition:%d,myLogfile:%s error,index:%d!!\n",curPartition,name.c_str(),index);
            printf("curPartition:%d,myLogfile:%s error!!\n",curPartition,name.c_str());
        }
      //  fprintf(stderr,"curPartition:%d,open Logfile:%s GC,index:%d!!\n",curPartition,name.c_str(),index);
        printf("curPartition:%d,open Logfile:%s GC!!\n",curPartition,name.c_str());
        #ifdef READ_AHEAD
          int fileID=fileno(readLogfile[i]);
          //int ret=readahead(fileID,0,file_size);  config::file_size
          uint64_t prefetchSize=3221225472; //3GB
          int ret=posix_fadvise(fileID,0L,prefetchSize, POSIX_FADV_WILLNEED);//POSIX_FADV_RANDOM | POSIX_FADV_WILLNEED
          printf("GC log file, ret=%d\n", ret);
          if(ret == -1){
            printf("posix_fadvise failed！\n");
          }
        #endif
    }
    printf("LogNum[%d]=%d,LogNum[%d]=%d,LogSegments:%d\n",curPartition,LogNum[curPartition],NewestPartition,LogNum[NewestPartition],LogSegments);
//mutex_.Unlock();
    for(int i=0;i<2;i++){   
        std::stringstream LogName;
        LogName<< LogPrefixName <<partition[i]<<"-"<<LogNum[partition[i]];
        std::string name=LogName.str();
        int index=partition[i]*config::logFileNum+LogNum[partition[i]];//-beginLogNum[partition[i]];
        globalLogFile[index]=fopen(name.c_str(),"ab+");
        //globalLogFile[partition[i]][LogNum[partition[i]]]=fopen(name.c_str(),"ab+");
      // fprintf(stderr," open file:%s,partition:%d,LogNum:%d,beginLogNum:%d,index:%d\n",name.c_str(),partition[i],LogNum[partition[i]],beginLogNum[partition[i]],index);
        if(globalLogFile[index]==NULL){     
          printf("Unable to open file:%s!\n",name.c_str());
          exit(0);
        }
        Compaction* c;
        c = versions_->PickSortedStoreForGC(partition[i]);
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
            }else{
                std::stringstream address_value;
                if( globalLogFile[index]==NULL){                
                  fprintf(stderr,"curPartition:%d,currentLogFile error!!\n",curPartition);
                }
                long curOffset = ftell(globalLogFile[index]);
                address_value<<partition[i]<<"&"<<LogNum[partition[i]]<<"&"<<curOffset<<"&"<<length;  
                addressStr=address_value.str();
                int curindex=curPartition*config::logFileNum+logNumber;//-beginLogNum[curPartition];
                if(globalLogFile[curindex]==NULL){           
                  fprintf(stderr,"curPartition:%d,logPartition:%d,partition[i]:%d,readLogfile[%d] error,curindex:%d!!\n",curPartition,logPartition,partition[i],logNumber,curindex);
                }
                char myBuffer[config::maxValueSize];              
                fseek(globalLogFile[curindex],offset,SEEK_SET);
                int num=fread(myBuffer,sizeof(char),length,globalLogFile[curindex]);
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
        //fprintf(stderr,"Remove file:%s\n",name.c_str());
        env_->DeleteFile(name.c_str());//////
        if(remove(name.c_str())==0){
            printf("Remove file:%s\n",name.c_str());
        }
    }
}

void DBImpl::doGCWithinPartition(int curPartition,int LogSegments){
    FILE* readLogfile[LogSegments];
    for(int i=0;i<LogSegments;i++){
        readLogfile[i]=NULL;
    }
    Status status;
    int64_t writeLogValue=0;
    int partition[2];
    for(int i=beginLogNum[curPartition];i<LogSegments-1 || i<1;i++){
        std::stringstream readLogName;
        readLogName<< LogPrefixName <<curPartition<<"-"<<i;      
        std::string name=readLogName.str();
        int index=curPartition*config::logFileNum+i;//-beginLogNum[curPartition];
        readLogfile[i]=globalLogFile[index];
        if( readLogfile[i]==NULL){
            fprintf(stderr,"curPartition:%d,myLogfile:%s error,index:%d!!\n",curPartition,name.c_str(),index);
        }
        printf("curPartition:%d,open Logfile:%s GC!!\n",curPartition,name.c_str());
        #ifdef READ_AHEAD
          int fileID=fileno(readLogfile[i]);
          //int ret=readahead(fileID,0,file_size);  config::file_size
          uint64_t prefetchSize=3221225472; //3GB
          int ret=posix_fadvise(fileID,0L,prefetchSize, POSIX_FADV_WILLNEED);//POSIX_FADV_RANDOM | POSIX_FADV_WILLNEED
          printf("GC log file, ret=%d\n", ret);
          if(ret == -1){
            printf("posix_fadvise failed！\n");
          }
        #endif
    }
    printf("LogNum[%d]=%d,LogNum[%d]=%d,LogSegments:%d\n",curPartition,LogNum[curPartition],NewestPartition,LogNum[NewestPartition],LogSegments);
    std::stringstream LogName;
    LogName<< LogPrefixName <<curPartition<<"-"<<LogNum[curPartition];
    std::string name=LogName.str();
    int index=curPartition*config::logFileNum+LogNum[curPartition];//-beginLogNum[partition[i]];
    globalLogFile[index]=fopen(name.c_str(),"ab+");
    if(globalLogFile[index]==NULL){
      fprintf(stderr,"Unable to open file:%s!\n",name.c_str());
      exit(0);
    }
    Compaction* c;
    c = versions_->PickSortedStoreForGC(curPartition);
    CompactionState* compact = new CompactionState(c);
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
            }
            long curOffset = ftell(globalLogFile[index]);
            address_value<<curPartition<<"&"<<LogNum[curPartition]<<"&"<<curOffset<<"&"<<length;  
            addressStr=address_value.str();    
            int curindex=curPartition*config::logFileNum+logNumber;//-beginLogNum[curPartition];
            if(globalLogFile[curindex]==NULL){           
              fprintf(stderr,"curPartition:%d,logPartition:%d,curPartition:%d,readLogfile[%d] error,curindex:%d!!\n",curPartition,logPartition,curPartition,logNumber,curindex);
            }
            char myBuffer[config::maxValueSize];              
            fseek(globalLogFile[curindex],offset,SEEK_SET);
            int num=fread(myBuffer,sizeof(char),length,globalLogFile[curindex]);
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
    
  for(int i=beginLogNum[curPartition];i<LogSegments-1 || i<1;i++){//
    int curindex=curPartition*config::logFileNum+i;//-beginLogNum[curPartition];
    fclose(globalLogFile[curindex]);
    globalLogFile[curindex]=NULL;
    std::stringstream LogName;  
    LogName<< LogPrefixName <<curPartition<<"-"<<i;
    std::string name=LogName.str();
    globalLogFile[curindex]=fopen(name.c_str(),"w");
    fclose(globalLogFile[curindex]);
    globalLogFile[curindex]=NULL;
    //fprintf(stderr,"Remove file:%s\n",name.c_str());
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

void DBImpl::MergeUnsortedStoreFilesTogether(int partition){
    Compaction* c = versions_->PickSomeHashIndexFileCompaction(partition);///
	  //fprintf(stderr,"before compactHashIndexPartitionTable L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),versions_->NumLevelFiles(config::kNumLevels+config::kTempLevel-1,partition));
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
	 // DeleteObsoleteFiles(partition);//delete old SSTables
   try{
	  ret.wait();
	  } catch(const std::future_error &e){ std::cerr<<"wait error\n"; }
    //fprintf(stderr,"after InstallSizeBasedCompactionResults NumFiles L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition));
	  delete c;
}

void DBImpl::compactHashIndexPartitionTable(int partition){
	  Compaction* c = versions_->PickSomeHashIndexFileCompaction(partition);///
	  CompactionState* compact = new CompactionState(c);
	  printf("before compactHashIndexPartitionTable L0:%d ,L1:%d,L2:%d\n",versions_->NumLevelFiles(0,partition),versions_->NumLevelFiles(1,partition),versions_->NumLevelFiles(config::kNumLevels+config::kTempLevel-1,partition));
	  Status  status= DoCompactionWork(compact,partition,false);
	  if (!status.ok()) {
	    RecordBackgroundError(status);
	  }
	  CleanupCompaction(compact,partition);
	  c->ReleaseInputs();
	  DeleteObsoleteFiles(partition);//delete old SSTables
	  delete c;
}

void DBImpl::CleanupCompaction(CompactionState* compact,int partition) {
  mutex_.AssertHeld();
  for(int k=0;k<2;k++){
      if (compact->builder[k] != NULL) {
        // May happen if we get a shutdown call in the middle of compaction
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
  // Check for iterator errors
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
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes,false,false);
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
  s=versions_->LogAndApply(compact->compaction->edit(), &mutex_,partition);
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
        compact->compaction->edit()->AddFile(emptyLevel,out.number,partition, out.file_size, out.smallest, out.largest);
       // printf("fileNumber:%d,smallestkey:%d,largestKey:%d,partition:%d\n",out.number,atoi(out.smallest.user_key().ToString().c_str()),atoi(out.largest.user_key().ToString().c_str()),partition);
      }
      s=versions_->LogAndApply(compact->compaction->edit(), &mutex_,partition);//
     // treeRoot->insertNode((char*)compact->outputs[1][0].smallest.user_key().ToString().c_str(),NewestPartition);///////        
	    printf("partition:%d, the smallest key:%s\n",NewestPartition,(char*)compact->outputs[1][0].smallest.user_key().ToString().c_str());
      for(int i=0;i< compact->outputs[1].size();i++){
        const CompactionState::Output& out = compact->outputs[1][i];
        compact->compaction->edit()->AddFile(emptyLevel,out.number,NewestPartition, out.file_size, out.smallest, out.largest);
      }
      s=versions_->LogAndApply(compact->compaction->edit(), &mutex_,NewestPartition);//
  }else{
    level=1;
    for (size_t i = 0; i < compact->outputs[0].size(); i++) {
      const CompactionState::Output& out = compact->outputs[0][i];
      compact->compaction->edit()->AddFile(level,out.number,partition, out.file_size, out.smallest, out.largest);
    }
    s=versions_->LogAndApply(compact->compaction->edit(), &mutex_,partition);//
  }

  /*if(doSplit){
	  uint64_t new_log_number = versions_->NewFileNumber();
	  WritableFile* lfile = NULL;  
	  Status s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
	  logfile_ [NewestPartition]= lfile;
	  logfile_number_[NewestPartition]= new_log_number;
	  logNumVec.push_back(new_log_number);
	  printf("buid new logfile_number_:%u,partition:%d\n",new_log_number,NewestPartition);
	  log_[NewestPartition]= new log::Writer(lfile);
	  pmem_[NewestPartition] = new MemTable(internal_comparator_);
	  pmem_[NewestPartition]->Ref();	 
  }*/
  return s;
}

bool DBImpl::containNewTable(uint64_t NewTableNum, std::vector<FileMetaData*> *myfiles){
  for (size_t i = 0; i < myfiles[0].size(); i++) {
      //FileMetaData* f = current_->files_[level][i];     
      FileMetaData* f = myfiles[0][i];
      if(NewTableNum==f->number){
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

/*void DBImpl::updateHashTable(std::vector<FileMetaData*> *deleteFiles){
  for(int i=0;i<config::bucketNum;i++){
    int curTableNum=bytes3ToInt(HashIndex[i].TableNum);
	if(curTableNum!=0 && containNewTable(curTableNum,deleteFiles)){
     // if(curTableNum!=0 && HashIndex[bucketNumber].KeyTag[0]==keyBytes[2] && HashIndex[bucketNumber].KeyTag[1]==keyBytes[3] && HashIndex[bucketNumber].KeyTag[2]==keyBytes[0] && HashIndex[bucketNumber].KeyTag[3]==keyBytes[1] ){
	 //int curTableNum=bytes3ToInt(HashIndex[bucketNumber].TableNum);
	// if(containNewTable(curTableNum,compact->compaction->GetAllInput())){
	  HashIndex[i].KeyTag[0]=0;
	  HashIndex[i].KeyTag[1]=0;
	  //HashIndex[i].KeyTag[2]=0;
	 // HashIndex[i].KeyTag[3]=0;
	  HashIndex[i].TableNum[0]=0;
	  HashIndex[i].TableNum[1]=0;
	  HashIndex[i].TableNum[2]=0;
	// }
      }//els
      IndexEntry* nextIndexEntry=HashIndex[i].nextEntry;
      IndexEntry* PrevIndexEntry=&HashIndex[i];
	//IndexEntry* myIndexEntry;
	while(nextIndexEntry!=NULL){
	   int myTableNum=bytes3ToInt(nextIndexEntry->TableNum);
	   if(containNewTable(myTableNum,deleteFiles)){
	      //if(nextIndexEntry->KeyTag[0]==keyBytes[2] && nextIndexEntry->KeyTag[1]==keyBytes[3]&& nextIndexEntry->KeyTag[2]==keyBytes[0] && nextIndexEntry->KeyTag[3]==keyBytes[1]){//&& containNewTable(myTableNum,compact->compaction->GetAllInput())){
		//if(nextIndexEntry->KeyTag[0]==keyBytes[3]){
		  PrevIndexEntry->nextEntry=nextIndexEntry->nextEntry;
		  delete nextIndexEntry;
		  nextIndexEntry=PrevIndexEntry->nextEntry;
		 //  printf("delete nextIndexEntry\n");
		  continue;
		  //break;
	      }
	      PrevIndexEntry=nextIndexEntry;
	      nextIndexEntry=nextIndexEntry->nextEntry;
	}
  }
}*/

//update the hash index after merging the UnsortedStore into SortedStore
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
}

void* DBImpl::updateBigHashTable(void *paraData){
  struct upIndexPara *updateIndex=(struct upIndexPara*)paraData;
  std::vector<FileMetaData*> *deleteFiles=updateIndex->deleteFiles;
  uint64_t file_number=updateIndex->file_number;
 // fprintf(stderr,"in updateBigHashTable,file_number:%d\n",file_number);
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
}

//merge KV pairs from UnsortedStore into SortedStore
#ifdef SPLIT_DURING_COMPACT
Status DBImpl::DoCompactionWork(CompactionState* compact,int partition,bool doSplit){
  const uint64_t start_micros = env_->NowMicros();
  std::future<void*> ret;
  int64_t writeLogValue=0;
  int newIndex;
  std::stringstream LogName;
  LogName<< LogPrefixName <<partition<<"-"<<LogNum[partition];
  std::string name=LogName.str();
  int pindex=partition*config::logFileNum+LogNum[partition];//-beginLogNum[partition];
  globalLogFile[pindex]=fopen(name.c_str(),"ab+");
  if(globalLogFile[pindex]==NULL){
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
  assert(versions_->NumLevelFiles(compact->compaction->level(),partition) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }
  int fileSizeFlag=0,isEmpty=1;
  std::string middleKey;
  if(doSplit){
    std::stringstream LogName;
     //LogName<< "logfile-"<<partition<<"-"<<LogNum[partition];
     LogName<< LogPrefixName <<NewestPartition<<"-"<<LogNum[NewestPartition];
     std::string name=LogName.str();
     newIndex=NewestPartition*config::logFileNum+LogNum[NewestPartition];//-beginLogNum[NewestPartition];
     globalLogFile[newIndex]=fopen(name.c_str(),"ab+");
     if(globalLogFile[newIndex]==NULL){
        fprintf(stderr,"Unable to open file for write:%s!\n",name.c_str());
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
      printf("middleKey:%s\n",middleKey.c_str());
      treeRoot->insertNode((char*)middleKey.c_str(),NewestPartition);////
      
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;  
      Status s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      logfile_ [NewestPartition]= lfile;
      logfile_number_[NewestPartition]= new_log_number;
      logNumVec.push_back(new_log_number);
      printf("buid new logfile_number_:%u,partition:%d\n",new_log_number,NewestPartition);
      log_[NewestPartition]= new log::Writer(lfile);
      pmem_[NewestPartition] = new MemTable(internal_comparator_);
      pmem_[NewestPartition]->Ref();	 

      finishCompaction=true;
      bg_cv_.SignalAll();//
    }
  }
  // Release mutex while we're actually doing the compaction work
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
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (input->SeekToFirst();input->Valid() && !shutting_down_.Acquire_Load();) {
    // Prioritize immutable compaction work
      double otherbeginTime=Env::Default()->NowMicros();
      Slice key = input->key();
      Slice keyValue=input->value();
      size_t valueSize=keyValue.size();
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
          std::string delimiter = "&";
          size_t pos =0;
          pos=value.find(delimiter);
          //if(pos>0 && size<config::minValueSize){
          if(size<config::minValueSize){
              compact->builder[direction]->Add(key, value);
          }else{
              long keySize=strlen(ikey.user_key.ToString().c_str());        
              std::stringstream address_value,vlog_value;
              std::string vlogv=value;
              long offset;
                if(direction==0){              
                    offset = ftell(globalLogFile[pindex]);
                    int num=fwrite(vlogv.c_str(), sizeof(char),vlogv.length(),globalLogFile[pindex]);        
                    address_value<<partition<<"&"<<LogNum[partition]<<"&"<<offset<<"&"<<vlogv.length();   
                }else{
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
          // Close output file if it is big enough
          if (compact->builder[direction]->FileSize() >= OutputFileSizeSet(fileSizeFlag,valueSize)) {
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
  delete input;
  input = NULL;
  mutex_.Lock();
  printf("compact->compaction->level():%d\n",compact->compaction->level());
  CompactionStats stats[config::kNumLevels+config::kTempLevel];
  int moveToLevel=compact->compaction->level()+1;
  if (status.ok()) {
      status = InstallCompactionResults(compact,partition,doSplit,&moveToLevel);////
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
	    ret= Env::Default()->threadPool->addTask(DBImpl::updateHashTable,(void *) &updateIndex);
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
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  double compactEndTime=Env::Default()->NowMicros();
  totalCompactTime+=compactEndTime-compactBeginTime;
  return status;
}
#endif

#ifdef NORMAL_COMPACT
Status DBImpl::DoCompactionWork(CompactionState* compact,int partition,bool doSplit){
  const uint64_t start_micros = env_->NowMicros();
  std::future<void*> ret;
  FILE* currentLogfile=NULL;
  std::stringstream LogName;
  LogName<< LogPrefixName <<partition<<"-"<<LogNum[partition];
  std::string name=LogName.str();
  int index=partition*config::logFileNum+LogNum[partition];//-beginLogNum[partition];
  globalLogFile[index]=fopen(name.c_str(),"ab+");
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
  assert(versions_->NumLevelFiles(compact->compaction->level(),partition) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }
  int fileSizeFlag=0,isEmpty=1;
  std::string middleKey;
  if(doSplit){
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
    }
#endif
    versions_->GetSortedStoreMiddleKey(&middleKey,partition);
    printf("middleKey:%s\n",middleKey.c_str());
  }
  // Release mutex while we're actually doing the compaction work
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
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (input->SeekToFirst();input->Valid() && !shutting_down_.Acquire_Load();) {
   double otherbeginTime=Env::Default()->NowMicros();
   Slice key = input->key();
   Slice keyValue=input->value();
   size_t valueSize=keyValue.size();
   int direction=0;
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
      std::string value=input->value().ToString();
      long size = value.length();
      std::string delimiter = "&";
      size_t pos =0;
      pos=value.find(delimiter);
      if(size<config::minValueSize){
          compact->builder[direction]->Add(key, input->value());
      }else{   
          long offset = ftell (currentLogfile);
          //long keySize=strlen(key.ToString().c_str());
          long keySize=strlen(ikey.user_key.ToString().c_str());        
          std::stringstream address_value,vlog_value;
          std::string vlogv=value;
          int num=fwrite (vlogv.c_str(), sizeof(char),vlogv.length(),currentLogfile);
          writeDataSizeActual+=vlogv.length();
          address_value<<partition<<"&"<<LogNum[partition]<<"&"<<offset<<"&"<<vlogv.length();      
          std::string address = address_value.str();	       
          compact->builder[direction]->Add(key, address);
      }
      // Close output file if it is big enough
        if (compact->builder[direction]->FileSize() >= OutputFileSizeSet(fileSizeFlag,valueSize)) {
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
  }
  double mergeEndTime=Env::Default()->NowMicros();
  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok()) {
    if (compact->builder[0] != NULL) {
      status = FinishCompactionOutputFile(compact, input,0);
      writeDataSizeActual+=compact->current_output(0)->file_size;
    }
  }
  delete input;
  input = NULL;
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
  double upHashEndTime=Env::Default()->NowMicros();
  totalUpHashTime+=upHashEndTime-upHashBeginTime;
  mutex_.Lock();
  printf("compact->compaction->level():%d\n",compact->compaction->level());
  CompactionStats stats[config::kNumLevels+config::kTempLevel];
  int moveToLevel=compact->compaction->level()+1;
  if (status.ok()) {
    status = InstallCompactionResults(compact,partition,doSplit,&moveToLevel);//
  }
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
  if(status.ok() && doSplit){
     //finishCompaction=true;
     printf("*********partition:%d, compaction output size:%ld,do split\n",partition,stats[moveToLevel].bytes_written);   
  }
  close(currentLogfile);
  for(int k=0;k<config::kNumLevels+config::kTempLevel;k++){
    stats_[partition][k].Add(stats[k]);
  }
  if(compact->compaction->num_input_files(0)>0){
    try{
	    ret.wait();
	  } catch(const std::future_error &e){ std::cerr<<"wait error\n"; }
  }
  printf("merge time:%lf,totalReadTime:%lf,totalSortTime:%lf,totalWriteTime:%lf,totalUpHashTime:%lf,inPutItearTime:%lf,otherTime:%lf\n",mergeEndTime-mergeBeginTime,totalReadTime,totalSortTime,totalWriteTime,totalUpHashTime,inPutItearTime,otherTime);
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  double compactEndTime=Env::Default()->NowMicros();
  totalCompactTime+=compactEndTime-compactBeginTime;
  return status;
}
#endif

namespace {
  
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
    /*IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
            : mu(mutex), version(version), mem(mem), imm(imm) { }*/
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  if (state->mem != NULL)    state->mem->Unref();
  //state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

/*Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  for(int k=0;k<config::kNumPartition;k++){
  *latest_snapshot = versions_->LastSequence();
  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(pmem_[k]->NewIterator());
  pmem_[k]->Ref();
  if (pimm_[k] != NULL) {
    list.push_back(pimm_[k]->NewIterator());
    pimm_[k]->Ref();
  }
  versions_->current()->AddIterators(options, &list,k);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();
  cleanup->mu = &mutex_;
  cleanup->mem = pmem_[k];
  cleanup->imm = pimm_[k];
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);
  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
  }
}*/

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();
  std::vector<Iterator*> list;
  Iterator* internal_iter;
  for(int k=0;k<config::kNumPartition;k++){
  // Collect together all needed child iterators
  list.push_back(pmem_[k]->NewIterator());
  if (pimm_[k] != NULL) {
    list.push_back(pimm_[k]->NewIterator());
  }
  versions_->current()->AddIterators(options, &list,k);
  }
  internal_iter =NewMergingIterator(&internal_comparator_, &list[0], list.size());
  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
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
  double getBeginTime=Env::Default()->NowMicros();
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }
  int Partition;
  Partition=MapCharKeyToPartition(key);
  //search memtable,immutable,sstable
  MemTable* mem = pmem_[Partition];
  MemTable* imm = pimm_[Partition];
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();
  bool have_stat_update = false;
  Version::GetStats stats;
  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    double readBeginTime=Env::Default()->NowMicros();
    totalReadOther+=readBeginTime-getBeginTime;
    if (mem->Get(lkey, value, &s)) {
      readMem++;
      double readMemTime=Env::Default()->NowMicros();
      totalReadMem+=readMemTime-readBeginTime;
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      readImm++;
      double readIemTime=Env::Default()->NowMicros();
      totalReadMem+=readIemTime-readBeginTime;
    } else {//search in SSTables of UnsortedStore and SortedStore
      int lastReadL0=readIn0;
      double t=0;
#ifdef LIST_HASH_INDEX
      int index=Partition*config::logFileNum+LogNum[Partition]-beginLogNum[Partition];
      s = current->Get(options, lkey,dbname_,value, Partition,globalLogFile,beginLogNum,&stats,ListHashIndex[Partition],&readDataSizeActual,&readIn0,&tableCacheNum,&blockCacheNum,&t);////////////
#endif

#ifdef CUCKOO_HASH_INDEX
      int index=Partition*config::logFileNum+LogNum[Partition]-beginLogNum[Partition];
      s = current->GetwithCukoo(options, lkey,dbname_,value,Partition,globalLogFile,beginLogNum,&stats,CuckooHashIndex[Partition],&readDataSizeActual,&readIn0,&tableCacheNum,&blockCacheNum,&t);////////////
#endif
      have_stat_update = true;
      double readLevelTime=Env::Default()->NowMicros();
      if(lastReadL0<readIn0){
	        totalReadL0+=readLevelTime-readBeginTime;
      }else{
	      totalReadLn+=readLevelTime-readBeginTime;
        totalGetCostLn+=t;
      }
    }
    mutex_.Lock();
  }
  readDataSize+=key.size()+value->size();
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
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
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
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
    if (pimm_[k] != NULL) {
      list.push_back(pimm_[k]->NewIterator());
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
    mutex_.Unlock();
   return NewDBIterator(
    this, user_comparator(), internal_iter,
    (options.snapshot != NULL
    ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
    : latest_snapshot),
    seed);
 // mutex_.Unlock();
}

int DBImpl::NewIterator(const ReadOptions& options, char* beginKey,int scanLength) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  IterState* cleanup = new IterState;
  int partition=0;
  partition=MapCharKeyToPartition(Slice(beginKey));
  mutex_.Lock();
  #ifdef SIZE_BASED_MERGE
  //mutex_.Lock();
  if(versions_->NumLevelFiles(0,partition)>config::triggerSizeBasedMerge){
     printf("versions_->NumLevelFiles(0,%d)>config::triggerSizeBasedMerge\n",partition);
     //fprintf(stderr, "versions_->NumLevelFiles(0,%d):%d >config::triggerSizeBasedMerge\n",partition, versions_->NumLevelFiles(0,partition));
     bg_scanCompaction_scheduled_ = true;
     MergeUnsortedStoreFilesTogether(partition);
     bg_scanCompaction_scheduled_ = false;
     printf("after versions_->NumLevelFiles(0,%d)>config::triggerSizeBasedMerge\n",partition);
  }
  //mutex_.Unlock();
  #endif
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
  int result=seekValueParallel(iter,partition,beginKey,scanLength);
#endif
  //mutex_.Unlock();
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
    //fprintf(stderr, "after seek key!\n");
    if(iter->Valid()){
        if(iter->key() == beginKey){
          found++;	
          int count=0;
          std::future<void*> ret[config::maxScanLength];
          int numAIOReqs=0;
          for (int j = 0; j < scanLength && iter->Valid(); j++) {
            Slice value=iter->value();
            //fprintf(stderr, "next key:%s!\n",iter->key().ToString().c_str());
            if(value.size()<config::minValueSize){              
                  //struct aiocb my_aiocb;       
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
                  //Link the AIO request with a thread callback 
                  /*my_aiocb.aio_sigevent.sigev_notify = SIGEV_THREAD;
                  my_aiocb.aio_sigevent.sigev_notify_function = aio_completion_handler;
                  my_aiocb.aio_sigevent.sigev_notify_attributes = NULL;
                  my_aiocb.aio_sigevent.sigev_value.sival_ptr = &my_aiocb;*/

                  int ret = aio_read(&my_aiocb[numAIOReqs]);
                  if (ret < 0) perror("aio_read");
                  numAIOReqs++;
                  /*while ( aio_error( &my_aiocb ) == EINPROGRESS ) ;
                  if ((ret = aio_return( &my_aiocb )) > 0) {
                    //got ret bytes on the read
                    //fprintf(stderr,"AIO read value, ret=%d\n",ret);
                  } else {
                    //read failed, consult errno 
                    fprintf(stderr,"AIO read value failed!!, ret=%d\n",ret);
                  }*/
                //   fprintf(stderr,"before open log file for read,logPartition:%d,logNumber:%d,Offset:%llu,length:%d!!\n",logPartition,logNumber,offset,length);
                  /*char buffer[config::maxValueSize];      
                  //pthread_mutex_lock(&file_mutex);        
                  int t=fseek(readLogfile,offset,SEEK_SET);
                  //fprintf(stderr,"t:%d\n",t);
                  int num=fread(buffer,sizeof(char),length,readLogfile);
                  std::string oneRecord(buffer);*/               
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
                    uint64_t scanFetchSize=scanLength*gValueSize*3/4;//*2/4;
                    int ret1=posix_fadvise(fileID,offset,scanFetchSize, POSIX_FADV_WILLNEED);//POSIX_FADV_RANDOM | POSIX_FADV_WILLNEED
                    //printf("seek prefetch ret=%d,fileID=%d\n", ret1,fileID);
                    if(ret1 == -1){
                      printf("posix_fadvise failed！\n");
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
                  //fprintf(stderr,"after threadPool->addTask,logPartition:%d,logNumber:%d,Offset:%llu,length:%d!!\n",logPartition,logNumber,offset,length);
                  /*std::string valueStr=value.ToString();
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
                //   fprintf(stderr,"before open log file for read,logPartition:%d,logNumber:%d,Offset:%llu,length:%d!!\n",logPartition,logNumber,offset,length);
                  char buffer[config::maxValueSize];                   
                  int t=fseek(readLogfile,offset,SEEK_SET);          
                  int num=fread(buffer,sizeof(char),length,readLogfile);
                  std::string oneRecord(buffer);*/
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
          //mutex_.Unlock();////
          for(int k=0;k<count;k++){
            try{
              ret[k].wait();
            } catch(const std::future_error &e){ std::cerr<<"wait error\n"; }
          }
        }/*else{
          //mutex_.Unlock();
        }*/
    }/*else{
      //mutex_.Unlock();
    }*/	
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


void DBImpl::RecordReadSample(Slice key) {
  /*MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction(0);
  }*/
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::rebuildHashIndex(const ReadOptions& options){
  Status status;
  /*for(int k=0;k<config::kNumPartition;k++){
     versions_->current()->rebuildHashIndexIterators(options,k,HashIndex);
  }*/
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

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

//write KV pairs into UniKV
Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;
  MutexLock l(&mutex_);
 //fprintf(stderr,"batch size1:%d\n",w.batch->ApproximateSize());
  Slice key1;
  w.batch->GetKeyValue(&key1);
  int partition = MapCharKeyToPartition(key1);
  w.partition=partition;
  w.batch->putPartitionNumber(partition);//

  writers_.push_back(&w);
  while ((!w.done && &w != writers_.front())) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }
  // May temporarily unlock and wait.
  Writer* last_writer = &w;
  
  Status status = MakeRoomForWrite(my_batch == NULL,last_writer->partition);
  uint64_t last_sequence = versions_->LastSequence();
  //fprintf(stderr,"K:%s,%d, ",key.ToString().c_str(),last_writer->partition);
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    double logBeginTime=Env::Default()->NowMicros();
    WriteBatch* updates = BuildBatchGroup(&last_writer);//BuildBatchGroup iter all Writer in writers_
    //fprintf(stderr,"batch size2:%d\n",updates->ApproximateSize());
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    uint64_t batchCount = WriteBatchInternal::Count(updates);
    last_sequence += batchCount;
    if(batchCount>1){//////
      //fprintf(stderr,"count:%d\n", batchCount);
      ampliFactor=2.2;
    }
    {
      mutex_.Unlock();
      if(log_[last_writer->partition]==NULL){
         fprintf(stderr,"log file error!\n");
      }
      status = log_[last_writer->partition]->AddRecord(WriteBatchInternal::Contents(updates));//Add to log
      bool sync_error = false;
      if (status.ok() && options.sync) {
        fprintf(stderr,"log file SyncSync!\n");
        status = logfile_[last_writer->partition]->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {//write to log successfully,then write to MemTable
        double logEndTime=Env::Default()->NowMicros();
        totalLogTime+=logEndTime-logBeginTime;
        status = WriteBatchInternal::InsertInto(updates, pmem_);//Add to memtable
        double MemEndTime=Env::Default()->NowMicros();
        totalCacheTime+=MemEndTime-logEndTime;
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();
    versions_->SetLastSequence(last_sequence); //set new seqence number
  }
  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }
  
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }
  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {//
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);
  size_t size = WriteBatchInternal::ByteSize(first->batch);
  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }
  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }
    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }
      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force,int partition) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  if(gValueSize<500){
    ampliFactor=1.5;
  }
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      fprintf(stderr, "Yield previous error, break !!!!\n");
      break;
    }else if(versions_->NumLevelFiles(0,partition) >= config::kL0_CompactionTrigger*ampliFactor && !bg_compaction_scheduled_){ 
    //else if(versions_->NumLevelFiles(0,partition) >= config::kL0_CompactionTrigger && !bg_compaction_scheduled_){//
        printf( "Too many L0 files:%d,partition:%d; Trigger Compaction\n",versions_->NumLevelFiles(0,partition),partition);
        if(bg_compaction_scheduled_ || doCompact){
            bg_cv_.Wait();
            break;
        }
        MaybeScheduleCompaction(partition);
    } else if (versions_->NumLevelFiles(0,partition) >= config::kL0_StopWritesTrigger*ampliFactor) {
      printf("Too many L0 files:%d,partition:%d; stop and waiting...\n",versions_->NumLevelFiles(0,partition),partition);
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      bg_cv_.Wait();
    } else if (!force && finishCompaction && //updatePartitionMessage &&
        (pmem_[partition]->ApproximateMemoryUsage() <= options_.write_buffer_size)) {// There is room in current memtable
      break;
    }else if (pimm_[partition] != NULL) {//memtable is full,the previous memtable is being compacted, so Wait
      //fprintf(stderr,"memtable is full,the previous memtable is being compacted, so Wait,P:%d\n",partition);
      Log(options_.info_log, "Current memtable full; waiting...\n");    
      if(!bg_compaction_scheduled_){
          MaybeScheduleCompaction(partition);
      } 
      bg_cv_.Wait();
    } else if(pmem_[partition]->ApproximateMemoryUsage() > options_.write_buffer_size){
      //fprintf(stderr,"build new memtable;partition:%d build...\n",partition);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);//create a log  
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_[partition];
      delete logfile_[partition];
      for(int k=0;k<logNumVec.size();k++){
        if(logNumVec[k]==logfile_number_[partition]){
          logNumVec.erase(logNumVec.begin()+k);
          k--;
        }
      }
      logImNumVec.push_back(logfile_number_[partition]);
      logfileImm_number_[partition]=logfile_number_[partition];
      logfile_ [partition]= lfile;// 
      logfile_number_[partition]= new_log_number;
      logNumVec.push_back(new_log_number);
      log_[partition]= new log::Writer(lfile);//new logfile
      pimm_[partition] = pmem_[partition];//transfer Memtable to immutable Memtable
      has_imm_[partition].Release_Store(pimm_[partition]);
      pmem_[partition] = new MemTable(internal_comparator_);//new MemTable
      pmem_[partition]->Ref();
      force = false;  // Do not force another compaction if have room
      //MaybeScheduleCompaction();
      CompactMemTable(partition);
    }else{
      printf("in wait!!\n");
      bg_cv_.Wait();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();
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
       for(int k=0;k<config::kNumPartition;k++){
          int num=versions_->NumLevelFiles(static_cast<int>(level),k);
          sum=sum+num;
       }
      char buf[100];
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
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    for(int k=0;k<config::kNumPartition;k++){
      if (pmem_[k]) {
	      total_usage += pmem_[k]->ApproximateMemoryUsage();
      }
      if (pimm_[k]) {
	      total_usage += pimm_[k]->ApproximateMemoryUsage();
      }
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }
  return false;
}

void DBImpl::GetApproximateSizes(//
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
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }
  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {//
  WriteBatch batch;
  batch.Put(key, value);
  gKeySize=key.ToString().length();//
  gValueSize=value.ToString().length();//
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {//
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  //std::cerr<<"open1\n";
  *dbptr = NULL;
  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  double recoveryBeginTime=Env::Default()->NowMicros();
  bool save_manifest = false;
  ///////////////////////////////
  int totalPartitions=impl->recoveryB_Tree(impl->treeRoot);
  if(totalPartitions>0){
      impl->NewestPartition=totalPartitions-1;
  }else{
      impl->NewestPartition=totalPartitions;
  }
  fprintf(stderr,"after rebuildTree,totalPartitions,:%d,NewestPartition:%d\n",totalPartitions,impl->NewestPartition);
  //printf("after rebuildTree,totalPartitions,:%d,NewestPartition:%d\n",totalPartitions,impl->NewestPartition);
  impl->treeRoot->printfTree();
  ////////////////////////////////////////////
  impl->initHashIndex();
  Status s1=impl->rebuildHashIndex(ReadOptions());//
  double recoveryIndexTime=Env::Default()->NowMicros();
  printf("recoveryIndexTime:%lf\n",recoveryIndexTime-recoveryBeginTime);
  fprintf(stderr,"recoveryIndexTime:%lf\n",recoveryIndexTime-recoveryBeginTime);
  //impl->TreeFile.open("B_TreeStore.txt");
  Status s = impl->Recover(&edit, &save_manifest);
  //std::cerr<<"after Recover\n";
  int emptyDB=impl->versions_->initFileMetadataAndprefetchPinterFile(impl->NewestPartition+1);
  if(!emptyDB){
    std::cerr<<"Not emptyDB, initValueLogNumber\n";
    impl->initValueLogNumber();
  }
  impl->initGlobalLogFile();
  WritableFile* lfile[config::kNumPartition]={NULL};
  for(int i=0;i<=impl->NewestPartition;i++){
      //impl->versions_->current()->printfIterators(ReadOptions(), i);//
      if (s.ok() && impl->pmem_[i] == NULL) {
        // Create new log and a corresponding memtable.
        uint64_t new_log_number = impl->versions_->NewFileNumber();
        fprintf(stderr,"partition:%d,new_log_number:%u\n",i,new_log_number);
        s = options.env->NewWritableFile(LogFileName(dbname, new_log_number), &lfile[i]);
        if (s.ok()) {
          edit.SetLogNumber(new_log_number,i);
          impl->logfile_[i] = lfile[i];
          impl->logfile_number_[i]= new_log_number;
          impl->log_[i]= new log::Writer(lfile[i]);
          impl->pmem_[i]=new MemTable(impl->internal_comparator_);
          impl->pmem_[i]->Ref();
          //std::cerr<<"before new MemTable\n";
          impl->memBaseSize=impl->pmem_[i]->ApproximateMemoryUsage();
        }
      }
  }
  for(int i=0;i<=impl->NewestPartition;i++){
      impl->logNumVec.push_back( impl->logfile_number_[i]);
  }
  if (s.ok() && save_manifest) {
    for(int i=0;i<=impl->NewestPartition;i++){
       edit.SetPrevLogNumber(impl->logfileImm_number_[i],i);//No older logs needed after recovery.
	     edit.SetLogNumber(impl->logfile_number_[i],i);
     }
    //edit.setPartitionInfoVec(impl->myPartitionInfo,impl->NewestPartition+1);//
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  double recoveryEndTime=Env::Default()->NowMicros();
  fprintf(stderr,"recoveryTime:%lf\n",recoveryEndTime-recoveryIndexTime);
  if (s.ok()) {
      for(int i=0;i<config::kNumPartition;i++){
	      impl->DeleteObsoleteFiles(i);
      }
  }
  //impl->versions_->InitializeTableCacheFileMetaData();
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->pmem_[0] != NULL);
    *dbptr = impl;
  } else {
    delete impl;
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
