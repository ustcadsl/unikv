// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "unikv/env.h"
#include "unikv/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

static int TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  // Result for both level-0 and level-1
  double result = 10. * 1048576.0;
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);
  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;
  // Drop references to files
  for(int k=0;k<config::kNumPartition;k++){
    for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
      std::vector<FileMetaData*>& files=pfiles_[k][level];
      for (size_t i = 0; i < pfiles_[k][level].size(); i++) {
          FileMetaData* f =files[i] ;
          assert(f->refs > 0);
          f->refs--;
          if (f->refs <= 0) {
            delete f;
          }
      }
    }
  }
}

int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files,
             const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

static bool AfterFile(const Comparator* ucmp,
                      const Slice* user_key, const FileMetaData* f) {
  // NULL user_key occurs before all keys and is therefore never after *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp,
                       const Slice* user_key, const FileMetaData* f) {
  // NULL user_key occurs after all keys and is therefore never before *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != NULL) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small(*smallest_user_key, kMaxSequenceNumber,kValueTypeForSeek);
    index = FindFile(icmp, files, small.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }
  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp),
        flist_(flist),
        index_(flist->size()) {        // Marks as invalid
  }
  virtual bool Valid() const {
    return index_ < flist_->size();
  }
  virtual void Seek(const Slice& target) {
    index_ = FindFile(icmp_, *flist_, target);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  virtual void Next() {
    assert(Valid());
    index_++;
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_+8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }
 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

static Iterator* GetFileIteratorPrefetch(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    bool doPrefetch=false;
    if(scanedNumber==DecodeFixed64(file_value.data())){
        doPrefetch=true;
        //printf("scanedNumber:%d\n",scanedNumber);
    }
      return cache->NewIterator(options,
                              DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8),false,doPrefetch);
    
  }
}

static Iterator* GetFileIterator(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
      return cache->NewIterator(options,
                              DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8),false,config::seekPrefetch);
    
  }
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level,int partition,char* beginKey) const {
  int fileIndex=0;
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  const std::vector<FileMetaData*>& files = pfiles_[partition][level];
  //int fileIndex = FindFile(vset_->icmp_, &pfiles_[partition][level],Slice(beginKey));
  for(int i=0;i<files.size();i++){
    const Slice file_limit = files[i]->largest.user_key();
    const Slice file_left = files[i]->smallest.user_key();
    if(user_cmp->Compare(file_left, Slice(beginKey))<=0 && user_cmp->Compare(file_limit, Slice(beginKey))>0){
        fileIndex=i;
        break;
    }
  }
  scanedNumber=files[fileIndex]->number;
  //printf("ID:%d,K:%s,s:%s,L:%s\n",scanedNumber,beginKey,files[fileIndex]->smallest.user_key().ToString().c_str(),files[fileIndex]->largest.user_key().ToString().c_str());
  
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &pfiles_[partition][level]),
      &GetFileIteratorPrefetch, vset_->table_cache_, options);
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level,int partition) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &pfiles_[partition][level]),
      &GetFileIteratorPrefetch, vset_->table_cache_, options);
}



void Version::AddUnsortedStoreIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters,int partition) {
  // Merge all level zero files together since they may overlap
  std::vector<FileMetaData*>& files=pfiles_[partition][0];
  int size=pfiles_[partition][0].size();
  int num = 0;
	  for (size_t i = 0; i < files.size(); i++) {
		num=0;
		Iterator** list = new Iterator*[size];
		list[num++]=vset_->table_cache_->NewIterator(options, files[i]->number, files[i]->file_size,false,true);
		Iterator* result = NewMergingIterator(&vset_->icmp_, list, num);
		 
		ofstream UnSortedFile;
		char name[50];
		snprintf(name, sizeof(name), "../tableFiles/keyFile-p%d-n%d.txt",partition, i);
		UnSortedFile.open(name);
		//fprintf(stderr,"before iter->SeekToFirst\n");
		//printf("before iter->SeekToFirst\n");
		result->SeekToFirst();
		for(;result->Valid();result->Next()){
		  UnSortedFile<<strtoul(result->key().ToString().substr(4,config::kKeyLength).c_str(),NULL,10)<<"\n";
		}	
		UnSortedFile.close();
		delete[] list;
		delete result;	
      } 
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters,int partition) {
  // Merge all level zero files together since they may overlap
  std::vector<FileMetaData*>& files=pfiles_[partition][0];
  for (size_t i = 0; i < pfiles_[partition][0].size(); i++) {
    FileMetaData* f=files[i];
    iters->push_back(
        vset_->table_cache_->NewIterator(
            options, f->number, f->file_size,false,config::seekPrefetch));
  }
  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them lazily.
  for (int level = 1; level < config::kNumLevels+config::kTempLevel; level++) {
    if (!pfiles_[partition][level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level,partition));
    }
  }
}

void* Version::doSeekFiles(void *data){
  struct versionSetPara *myPara=(struct versionSetPara*)data;
//  int threadID=myPara->t_id;
  //printf("thread id=%d\n",(int)myPara->myFileMeta->number);
  // myPara->tableIter=myPara->myTable_cache->NewIterator(myPara->options,myPara->myFileMeta->number,myPara->myFileMeta->file_size,myPara->myIndexEntry,NULL);
   myPara->tableIter=myPara->myTable_cache->NewIterator(myPara->options,myPara->myFileMeta->number,myPara->myFileMeta->file_size,false,false,//config::seekPrefetch,
                                                        nullptr);
   #ifdef PREFETCH_UNSORTEDSTORE
   //prefetch UnsortedStore to page cache
   myPara->tableIter->SeekToFirst();
   while(myPara->tableIter->Valid()){
      myPara->tableIter->key();
      myPara->tableIter->value();
    	myPara->tableIter->Next();
    }        
  #endif
   return nullptr;
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters,port::Mutex* mu,int partition) {
  // Merge all level zero files together since they may overlap
  std::vector<FileMetaData*>& files=pfiles_[partition][0];
  void *status;
  struct versionSetPara Para[files.size()];
  myMu=mu;
  std::future<void*> ret[files.size()];
  for (size_t i = 0; i < files.size(); i++) {
    FileMetaData* f=files[i];
    Para[i].options=options;
    Para[i].t_id=i;
    Para[i].myFileMeta=f;
    Para[i].myTable_cache=vset_->table_cache_;
     ret[i] = Env::Default()->threadPool->addTask(Version::doSeekFiles, (void *) &Para[i]);
  }
  for(size_t i=0;i<files.size();i++){
      try{
      ret[i].wait();
      } catch(const std::future_error &e){ std::cerr<<"wait error\n"; }
      iters->push_back(Para[i].tableIter);
  }
  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them lazily.
  for (int level = 1; level < config::kNumLevels+config::kTempLevel; level++) {
    if (!pfiles_[partition][level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level,partition));
    }
  }
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters,int partition,char* beginKey) {
  // Merge all level zero files together since they may overlap
  std::vector<FileMetaData*>& files=pfiles_[partition][0];
  void *status;
   const Comparator* user_cmp = vset_->icmp_.user_comparator();
  struct versionSetPara Para[files.size()];
  std::future<void*> ret[files.size()];
  int threadCount=0;
  for (size_t i = 0; i < files.size(); i++) {
    FileMetaData* f=files[i];
    const Slice file_limit = f->largest.user_key();
    if (beginKey != NULL && user_cmp->Compare(file_limit, Slice(beginKey)) < 0) {
      //printf("not check this table ID=%d\n",(int)f->number);
      continue;
      // "f" is completely before specified range; skip it
    }
    Para[threadCount].options=options;
   // Para[i].myIndexEntry=HashIndex;
   // Para[i].list=iters;
    Para[threadCount].t_id=i;
    Para[threadCount].myFileMeta=f;
    Para[threadCount].myTable_cache=vset_->table_cache_;
    ret[threadCount] = Env::Default()->threadPool->addTask(Version::doSeekFiles, (void *) &Para[threadCount]);
    threadCount++;
  }
  for(size_t i=0;i<threadCount;i++){
      try{
      ret[i].wait();
      } catch(const std::future_error &e){ std::cerr<<"wait error\n"; }
      iters->push_back(Para[i].tableIter);
  }
  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them lazily.
  for (int level = 1; level < config::kNumLevels+config::kTempLevel; level++) {
    if (!pfiles_[partition][level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level,partition,beginKey));
    }
  }
}

void Version::printfIterators(const ReadOptions& options,int partition) {
  for(int level=0;level<2;level++){
      std::vector<FileMetaData*>& files=pfiles_[partition][level];
      int ret;
      pthread_t pid[files.size()];
      pthread_attr_t attr;
      void *status;
      for (size_t i = 0; i < pfiles_[partition][level].size(); i++) {
        FileMetaData* f=files[i];
        Iterator* iters=vset_->table_cache_->NewIterator(
                options, f->number, f->file_size,false,config::seekPrefetch);          
        printf("fileID:%d\n",(int)f->number);
        //fprintf(stderr,"meta.number:%d,tableNumBytes:%d\n",(int)meta.number,bytes2ToInt(tableNumBytes));
        for(iters->SeekToFirst();iters->Valid();iters->Next()){	
            Slice myKey = iters->key();
            //unsigned int intKey=strtoul(myKey.ToString().c_str(),NULL,10);	
            printf("key:%s,p:%d,L:%d\n",myKey.ToString().substr(0,24).c_str(),partition,level);
        }
        printf("\n");   
      }
  }
}

void Version::findKeyIterators(const ReadOptions& options, int partition, char* key) {
  for(int level=0;level<2;level++){
    std::vector<FileMetaData*>& files=pfiles_[partition][level];
    int ret;
    pthread_t pid[files.size()];
    pthread_attr_t attr;
    void *status;
    int find=0;
    for (size_t i = 0; i < pfiles_[partition][level].size(); i++) {
      FileMetaData* f=files[i];
      Iterator* iters=vset_->table_cache_->NewIterator(
              options, f->number, f->file_size,false,config::seekPrefetch);          
      //printf("fileID:%d\n",(int)f->number);
        //fprintf(stderr,"meta.number:%d,tableNumBytes:%d\n",(int)meta.number,bytes2ToInt(tableNumBytes));
        for(iters->SeekToFirst();iters->Valid();iters->Next()){	
          Slice myKey = iters->key();
          if(strcmp(myKey.ToString().substr(0,24).c_str(),key)==0){
            //unsigned int intKey=strtoul(myKey.ToString().c_str(),NULL,10);	
            find=1;
            printf("----find key:%s,p:%d,L:%d\n",myKey.ToString().substr(0,24).c_str(),partition,level);
            break;
          }
        }
        if(find) break;  
    }
  }
}

void Version::rebuildHashIndexIterators(const ReadOptions& options,int partition,CuckooIndexEntry* myHashIndex) {
  // Merge all level zero files together since they may overlap
  std::vector<FileMetaData*>& files=pfiles_[partition][0];
  int ret;
  pthread_t pid[files.size()];
  pthread_attr_t attr;
  void *status;
  struct versionSetPara Para[files.size()];
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
   for (int k= 0; k < files.size(); k++) {
     for(int j=0;j<files.size()-k-1;j++){
       if((int)files[j]->number > (int)files[j+1]->number){
          swap(files[j],files[j+1]);
          /*FileMetaData* f=files[j+1];
          int number=files[j+1]->number;
          files[j+1]=files[j];
          files[j]=f;*/
      }
    }
  }
  for (size_t i = 0; i < files.size(); i++) {
    FileMetaData* f=files[i];
    Para[i].options=options;
    Para[i].t_id=i;
    Para[i].myFileMeta=f;
    Para[i].myTable_cache=vset_->table_cache_;
    ret=pthread_create(&pid[(int)i],NULL,doSeekFiles,(void *)&Para[i]);
    if(ret!=0){
      printf ("Create pthread error!\n");
    }
  }
    pthread_attr_destroy(&attr);
    for(int i=0; i <files.size(); i++ ){
       int rc = pthread_join(pid[i], NULL);    
       if (rc){
          printf("Error:unable to join:%d\n",rc);
          exit(-1);
       }
    }
   for(int i=0; i <files.size(); i++ ){
     Iterator* myIter=Para[i].tableIter;
     byte* tableNumBytes=new byte[2];
     intTo2Byte(int(Para[i].myFileMeta->number),tableNumBytes);	////////////////////////
      //fprintf(stderr,"meta.number:%d,tableNumBytes:%d\n",(int)meta.number,bytes2ToInt(tableNumBytes));
      for(myIter->SeekToFirst();myIter->Valid();myIter->Next()){
      byte* keyBytes=new byte[4];
      Slice myKey = myIter->key();
      unsigned int intKey;
      unsigned int hashKey;
	    if(strlen(myKey.data())>20){
        intKey=strtoul((char*)myKey.ToString().substr(4,config::kKeyLength).c_str(),NULL,10);
        hashKey=verhashfunc.RSHash((char*)myKey.ToString().substr(0,config::kKeyLength).c_str(),config::kKeyLength);
	    }else{
	      intKey=strtoul(myKey.ToString().c_str(),NULL,10);
	      hashKey=verhashfunc.RSHash((char*)myKey.ToString().c_str(),config::kKeyLength-8);
	    }
	   int keyBucket=intKey%config::bucketNum;
      intTo4Byte(hashKey,keyBytes);//
      bool findEmptyBucket=false;
        for(int k=0;k<config::cuckooHashNum;k++){
          keyBucket=verhashfunc.cuckooHash((char*)myKey.ToString().substr(0,config::kKeyLength).c_str(),k,config::kKeyLength);
          if(myHashIndex[keyBucket].KeyTag[0]==0 && myHashIndex[keyBucket].KeyTag[1]==0){
            findEmptyBucket=true;
            break;
          }
        }
        if(findEmptyBucket){
          myHashIndex[keyBucket].KeyTag[0]=keyBytes[2];
          myHashIndex[keyBucket].KeyTag[1]=keyBytes[3];     
          myHashIndex[keyBucket].TableNum[0]=tableNumBytes[0];
          myHashIndex[keyBucket].TableNum[1]=tableNumBytes[1];
          myHashIndex[keyBucket].TableNum[2]=tableNumBytes[2];
        }
        else{
          printf("cuckoo hash overflow!!add to list\n");
          //fprintf(stderr,"cuckoo hash overflow!!add to list\n");
          CuckooIndexEntry overflowEntry;
          overflowEntry.KeyTag[0]=keyBytes[2];
          overflowEntry.KeyTag[1]=keyBytes[3];     
          overflowEntry.TableNum[0]=tableNumBytes[0];
          overflowEntry.TableNum[1]=tableNumBytes[1];
          overflowEntry.TableNum[2]=tableNumBytes[2];
          //overflowEntryList[partition].push_back(overflowEntry);
          //myPara->EntryList.push_back(overflowEntry);
        }
	     delete []keyBytes; 
      }
      delete []tableNumBytes;
    }
}

static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key,
                                 void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  // TODO(sanjay): Change Version::Get() to use this function.
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  unsigned int kn=strtoul(user_key.ToString().c_str(),NULL,10);
  //int partition=kn%config::kNumPartition;
  int partition=kn/config::baseRange;
  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(pfiles_[partition][0].size());
  std::vector<FileMetaData*>& files=pfiles_[partition][0];
  for (uint32_t i = 0; i < pfiles_[partition][0].size(); i++) {
    FileMetaData* f = files[i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }
  // Search other levels.
  for (int level = 1; level < config::kNumLevels+config::kTempLevel; level++) {
    size_t num_files = pfiles_[partition][level].size();
    if (num_files == 0) continue;
    // Binary search to find earliest index whose largest key >= internal_key.
    uint32_t index = FindFile(vset_->icmp_, pfiles_[partition][level], internal_key);
    std::vector<FileMetaData*>& files=pfiles_[partition][level];
    if (index < num_files) {
      //FileMetaData* f = files_[level][index];
      FileMetaData* f = files[index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

uint64_t  Version:: DBTotalSize(){//
  uint64_t DBsize=0;
  for(int k=0;k<config::kNumPartition;k++){
    for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
      size_t num_files = pfiles_[k][level].size();
	    // Get the list of files to search in this level
      std::vector<FileMetaData*>& files=pfiles_[k][level];
      for (uint32_t i = 0; i < num_files; i++) {//
        FileMetaData* f = files[i];
        DBsize=DBsize+f->file_size;
      }
    }
  }
  return DBsize;
}

Status Version::doQueryOperation(const ReadOptions& options,FILE* LogFile[config::kNumPartition*config::logFileNum],
                    const LookupKey& k,FileMetaData* f,int level,int partition,
                   Saver* saver,unsigned long int* readDataSizeActual,uint64_t*  tableCacheNum,uint64_t*  blockCacheNum,int stage){
  Status s;
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  std::string* value=saver->value;
  double endTime=Env::Default()->NowMicros();      
  int BFfilter;
  //fprintf(stderr, "--look key:%d, in level:%d ,f->number=%d\n", atoi(ikey.ToString().c_str()),level,f->number);////
  s = vset_->table_cache_->Get(options, f->number, f->file_size,level,
                      ikey, saver, SaveValue,readDataSizeActual,tableCacheNum,blockCacheNum,&BFfilter);
  if (!s.ok()) {//
      return s;
  }
  double L0endTime=Env::Default()->NowMicros();
  switch (saver->state) {
      case kNotFound:
          if(stage<0){
              printf("Link List look key:%s, in level:%d ,state=failed!,partition:%d\n",user_key.ToString().c_str(),level,partition);                   
          }else if(stage==0){
              printf("cuckoo hash look key:%s, in level:%d ,state=failed!,partition:%d\n",user_key.ToString().c_str(),level,partition);
          }else{
              printf( "look key:%s, in level:%d,state=failed!,partition:%d\n",user_key.ToString().c_str(),level,partition);
              /*for(int i=0;i<config::kNumPartition;i++){
                findKeyIterators(options, i, (char*)user_key.ToString().substr(0,24).c_str());
              }*/
          }
          if(BFfilter==0){
             //printf( "real read,look key:%s, in level:%d ,partition:%d,state=failed!\n",ikey.ToString().c_str(),level,partition);
          }
          s = Status::NotFound(Slice());  // Use empty error message for speed
          return s;
      case kFound:
          if(stage==1){ 
              std::string delimiter = "&";
              size_t pos = 0;
              long length,offset,logPartition,logNumber;
              std::string::size_type sz;
              pos=(*value).find(delimiter);
              //logPartition=std::stol((*value).substr(0, pos),&sz); 
              logPartition=strtoul((*value).substr(0, pos).c_str(),NULL,10);               
              (*value).erase(0, pos + delimiter.length());
              pos=(*value).find(delimiter);
              logNumber=strtoul((*value).substr(0, pos).c_str(),NULL,10);               
              (*value).erase(0, pos + delimiter.length());
              pos=(*value).find(delimiter);
              offset=strtoul((*value).substr(0, pos).c_str(),NULL,10); 
              (*value).erase(0, pos + delimiter.length());
              length=strtoul((*value).c_str(),NULL,10); 
  //          cout <<"key:"<<user_key.ToString().c_str()<<",p:"<<logPartition<<",n:"<<logNumber<< ",Offset: " << offset << ",Length: " << length<< endl;                    
              int index=logPartition*config::logFileNum+logNumber;//-beginLog[logPartition];
              //fprintf(stderr,"before open log file for read,partition:%d,logPartition:%d,logNumber:%d,Offset:%llu,length:%d!!\n",partition,logPartition,logNumber,offset,length);                              
              //FILE* readLogfile=logFile[logPartition*config::logFileNum+logNumber];
              char buffer[config::maxValueSize];              
              int t=fseek(LogFile[index],offset,SEEK_SET);                 
              int num=fread(buffer,sizeof(char),length,LogFile[index]);
              std::string oneRecord(buffer);  
              *value=oneRecord;
              *readDataSizeActual=*readDataSizeActual+4096;//length;
              //fprintf(stderr,"after open log file for read.oneRecord.size:%d,num:%d,buffer size:%d!!\n",oneRecord.size(),num,strlen(buffer));
              /*pos=0;              
              long keySize,valueSize;
              std::string readkey;   
              pos=oneRecord.find(delimiter);      
              //fprintf(stderr,"pos:%d\n",pos);                
              //keySize=std::stol(oneRecord.substr(0, pos),&sz);
              keySize=strtoul(oneRecord.substr(0, pos).c_str(),NULL,10); 
              //fprintf(stderr,"pos:%d,keySize:%ld\n",pos,keySize);
              oneRecord.erase(0, pos + delimiter.length());      
              //cout<<"pos1:"<<pos;     
              pos=oneRecord.find(delimiter);               
              //valueSize=std::stol(oneRecord.substr(0, pos),&sz);
              valueSize=strtoul(oneRecord.substr(0, pos).c_str(),NULL,10); 
              //fprintf(stderr,"pos:%d,valueSize:%ld\n",pos,valueSize);
              oneRecord.erase(0, pos + delimiter.length());
              //cout<<",pos2:"<<pos;
              pos=oneRecord.find(delimiter);                
              readkey=oneRecord.substr(0, pos);
              //fprintf(stderr,"pos:%d,readkey:%s\n",pos,readkey.c_str());
              oneRecord.erase(0, pos + delimiter.length());                
              //cout<<",pos3:"<<pos<<endl;
              *value=oneRecord;*/
 //           cout<<"keySize:"<<keySize<<",valueSize:"<<valueSize<<",readkey:"<<readkey<<",value:"<<*value;
          }
          //printf(" look key:%u, in level:%d success, f->number=%d\n",InKey,level,f->number);
          return s;
      case kDeleted:
          s = Status::NotFound(Slice());  // Use empty error message for speed
          return s;
      case kCorrupt:
          s = Status::Corruption("corrupted key for ", user_key);
          return s;
      }
      return s;
}

Status Version::GetKeyinUnsortedStoreByCukoo(const ReadOptions& options,FILE* LogFile[config::kNumPartition*config::logFileNum],const LookupKey& Lkey,Saver* saver,int partition,ListIndexEntry* myHashIndex,
                  unsigned long int* readDataSizeActual,uint64_t*  tableCacheNum,uint64_t*  blockCacheNum){
  int level=0;
  Status s;
  Slice ikey = Lkey.internal_key();
  Slice user_key = Lkey.user_key();   
  std::vector<FileMetaData*>& myfiles=pfiles_[partition][level];
  size_t num_files = pfiles_[partition][level].size();
  FileMetaData* const* files = &myfiles[0];////get list of files in each level
  byte* keyBytes=new byte[4];
  unsigned int InKey,hashKey;
  if(strlen(ikey.data())>20){
      InKey=strtoul((char*)ikey.ToString().substr(4,config::kKeyLength).c_str(),NULL,10);
      hashKey=verhashfunc.RSHash((char*)ikey.ToString().substr(0,config::kKeyLength).c_str(),config::kKeyLength);
      //printf("strlen(ikey.data()):%d,ikey:%s,InKey:%u\n",strlen(ikey.data()),ikey.ToString().substr(0,config::kKeyLength).c_str(),InKey);
  }else{
      InKey=strtoul(ikey.ToString().c_str(),NULL,10);
      hashKey=verhashfunc.RSHash((char*)ikey.ToString().c_str(),config::kKeyLength-8);
  }
	intTo4Byte(hashKey,keyBytes);//
  //printf(" begin look key:%u, ikey:%d, in partition:%d\n",stoul(user_key.ToString().c_str(),NULL,10),InKey,partition);
  int tableNum=-1,fileNum=-1;
for(int k=config::cuckooHashNum-1;k>=0;k--){
  tableNum=-1,fileNum=-1;
  int bucketNumber=verhashfunc.cuckooHash((char*)user_key.ToString().substr(0,config::kKeyLength).c_str(),k,config::kKeyLength);
  ListIndexEntry *lastEntry=&myHashIndex[bucketNumber];
	while(lastEntry!=NULL){
        //printf( "in Link List look key:%u, in level:%d,bucket:%d\n",InKey,level,bucketNumber);
        if(lastEntry->KeyTag[0]==keyBytes[2] && lastEntry->KeyTag[1]==keyBytes[3]){	
            //tableNum=bytes3ToInt(lastEntry->TableNum);
            tableNum=bytes2ToInt(lastEntry->TableNum);
            if(tableNum<=0){
              lastEntry=lastEntry->nextEntry;
              continue;
            }
        }else{
            lastEntry=lastEntry->nextEntry;
            continue;
        }
        for(int k=0;k<num_files;k++){
            if(tableNum==(int)myfiles[k]->number){
                fileNum=k;            
                break;
            }
        }
      if(fileNum<0){
          //fprintf(stderr, "&&&&&&&&&&key=%d,tableNum:%d\n",atoi(ikey.ToString().c_str()),tableNum);
          lastEntry=lastEntry->nextEntry;
          continue;
        }
        FileMetaData* f=files[fileNum];  
        s=doQueryOperation(options,LogFile,Lkey,f,level,partition,saver, readDataSizeActual,tableCacheNum, blockCacheNum,-1);
        if(saver->state==kFound){
            return s;
        }
        lastEntry=lastEntry->nextEntry;
    }
}
    delete []keyBytes;  
}

Status Version::GetKeyinUnsortedStore(const ReadOptions& options,FILE* LogFile[config::kNumPartition*config::logFileNum],const LookupKey& Lkey,Saver* saver,int partition,ListIndexEntry* myHashIndex,
                               unsigned long int* readDataSizeActual,uint64_t*  tableCacheNum,uint64_t*  blockCacheNum){
  int level=0;
  Status s;
  Slice ikey = Lkey.internal_key();
  Slice user_key = Lkey.user_key();   
  std::vector<FileMetaData*>& myfiles=pfiles_[partition][level];
  size_t num_files = pfiles_[partition][level].size();
  FileMetaData* const* files = &myfiles[0];////get list of files in each level
  byte* keyBytes=new byte[4];
  unsigned int InKey,hashKey;
  if(strlen(ikey.data())>20){
      InKey=strtoul((char*)ikey.ToString().substr(4,config::kKeyLength).c_str(),NULL,10);
      hashKey=verhashfunc.RSHash((char*)ikey.ToString().substr(0,config::kKeyLength).c_str(),config::kKeyLength);
      //printf("strlen(ikey.data()):%d,ikey:%s,InKey:%u\n",strlen(ikey.data()),ikey.ToString().substr(0,config::kKeyLength).c_str(),InKey);
  }else{
      InKey=strtoul(ikey.ToString().c_str(),NULL,10);
      hashKey=verhashfunc.RSHash((char*)ikey.ToString().c_str(),config::kKeyLength-8);
  }
	int bucketNumber=InKey%config::bucketNum;
	intTo4Byte(hashKey,keyBytes);//
  int tableNum=-1,fileNum=-1;
  //printf(" begin look key:%u, ikey:%d, in partition:%d\n",stoul(user_key.ToString().c_str(),NULL,10),InKey,partition);
  ListIndexEntry *lastEntry=&myHashIndex[bucketNumber];
	while(lastEntry!=NULL){
        if(lastEntry->KeyTag[0]==keyBytes[2] && lastEntry->KeyTag[1]==keyBytes[3]){	
            //tableNum=bytes3ToInt(lastEntry->TableNum);
            tableNum=bytes2ToInt(lastEntry->TableNum);
            if(tableNum<=0){
              lastEntry=lastEntry->nextEntry;
              continue;
            }
        }else{
            lastEntry=lastEntry->nextEntry;
            continue;
        }
        for(int k=0;k<num_files;k++){
            if(tableNum==(int)myfiles[k]->number){
                fileNum=k;            
                break;
            }
        }
      if(fileNum<0){
          //fprintf(stderr, "&&&&&&&&&&key=%d,tableNum:%d\n",atoi(ikey.ToString().c_str()),tableNum);
          lastEntry=lastEntry->nextEntry;
          continue;
        }
        FileMetaData* f=files[fileNum];  
        s=doQueryOperation(options,LogFile,Lkey,f,level,partition,saver, readDataSizeActual,tableCacheNum, blockCacheNum,0);
        if(saver->state==kFound){
            return s;
        }
        lastEntry=lastEntry->nextEntry;
    }
    delete []keyBytes;  
}

Status Version::GetKeyinSortedStore(const ReadOptions& options,FILE* LogFile[config::kNumPartition*config::logFileNum],const LookupKey& Lkey,Saver* saver,int partition,unsigned long int* readDataSizeActual,uint64_t*  tableCacheNum,uint64_t*  blockCacheNum){
  int level=1;
  Status s;
  Slice ikey = Lkey.internal_key();
  Slice user_key = Lkey.user_key();  
  FileMetaData* tmp2; 
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  std::vector<FileMetaData*>& myfiles=pfiles_[partition][level];
  FileMetaData* const* files = &myfiles[0];////get list of files in each level
  size_t num_files = pfiles_[partition][level].size();
  uint32_t index = FindFile(vset_->icmp_, pfiles_[partition][level], ikey);///////////
  //fprintf(stderr, "index:%lu,num_files:%lu\n",index,num_files);
  if (index >= num_files) {//
    files = NULL;
    num_files = 0;
    //continue;//add
  }else{
      tmp2 = files[index];///SSTable MetaData which contain lookedup key in level1-7
      if (ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0) {
      // All of "tmp2" is past any data for user_key
        files = NULL;
        num_files = 0;
        //continue;//add
      } else {
        //fprintf(stderr, "index:%lu,files = &tmp2;\n",index);
        files = &tmp2;
        num_files = 1;
      }
  } 
  FileMetaData* f =files[0];
  s=doQueryOperation(options,LogFile,Lkey,f,level,partition,saver,readDataSizeActual,tableCacheNum, blockCacheNum,1);
  return s;
}

Status Version::GetwithCukoo(const ReadOptions& options,
                    const LookupKey& Lkey, std::string dbname,
                    std::string* value,int partition,FILE* LogFile[config::kNumPartition*config::logFileNum],int* beginLogNum,
                    GetStats* stats,ListIndexEntry* myHashIndex,unsigned long int* readDataSizeActual,int* readIn0,uint64_t*  tableCacheNum,uint64_t*  blockCacheNum,double* GetL1costTime) {
  Slice ikey = Lkey.internal_key();
  Slice user_key = Lkey.user_key();
  dbname_=dbname;
  //currentFile=logFile;
  beginLog=beginLogNum;
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  Status s;
  stats->seek_file = NULL;
  stats->seek_file_level = -1;
  FileMetaData* last_file_read = NULL;
  int last_file_read_level = -1;
    Saver saver;
    saver.state = kNotFound;
    saver.ucmp = ucmp;
    saver.user_key = user_key;
    saver.value = value;
    //fprintf(stderr,"look key:%s\n",user_key.ToString().c_str());
   for (int level = 0; level < config::kNumLevels; level++){
    size_t num_files = pfiles_[partition][level].size();
    if (num_files == 0){
      continue;//
    } 
     double beginTime=Env::Default()->NowMicros();
    ///////find k-v pair in level0 according to hashTable
    if (level == 0) {//search in UnsortedStore
       // fprintf(stderr,"look key:%s in L0\n",user_key.ToString().c_str());
       s=GetKeyinUnsortedStoreByCukoo(options,LogFile,Lkey,&saver,partition,myHashIndex,readDataSizeActual,tableCacheNum,blockCacheNum);
       if(saver.state==kFound){
            return s;
        }
    } 
    else {// Binary search to find earliest index whose largest key >= ikey.
      //fprintf(stderr,"look key:%s in L1,partition:%d\n",user_key.ToString().c_str(),partition);
       s=GetKeyinSortedStore(options,LogFile,Lkey,&saver,partition,readDataSizeActual,tableCacheNum,blockCacheNum);
       if(saver.state==kFound){
        return s;
       }
    }
   }
  return Status::NotFound(Slice());  // Use an empty error message for speed
}

Status Version::Get(const ReadOptions& options,
                    const LookupKey& Lkey, std::string dbname,
                    std::string* value,int partition,FILE* LogFile[config::kNumPartition*config::logFileNum],int* beginLogNum,
                    GetStats* stats,ListIndexEntry* myHashIndex,unsigned long int* readDataSizeActual,int* readIn0,uint64_t*  tableCacheNum,uint64_t*  blockCacheNum,double* GetL1costTime) {
  Slice ikey = Lkey.internal_key();
  Slice user_key = Lkey.user_key();
  dbname_=dbname;
  beginLog=beginLogNum;
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  Status s;
  stats->seek_file = NULL;
  stats->seek_file_level = -1;
  FileMetaData* last_file_read = NULL;
  int last_file_read_level = -1;
    Saver saver;
    saver.state = kNotFound;
    saver.ucmp = ucmp;
    saver.user_key = user_key;
    saver.value = value;
    //fprintf(stderr,"look key:%s\n",user_key.ToString().c_str());
   for (int level = 0; level < config::kNumLevels; level++) {
    size_t num_files = pfiles_[partition][level].size();
    if (num_files == 0){
      continue;//
    } 
     double beginTime=Env::Default()->NowMicros();
    ///////find k-v pair in level0 according to hashTable
    if (level == 0) {//
       // fprintf(stderr,"look key:%s in L0\n",user_key.ToString().c_str());
       s=GetKeyinUnsortedStore(options,LogFile,Lkey,&saver,partition,myHashIndex,readDataSizeActual,tableCacheNum,blockCacheNum);
       if(saver.state==kFound){
            return s;
        }
    } 
    else {//Binary search to find earliest index whose largest key >= ikey.
      //fprintf(stderr,"look key:%s in L1,partition:%d\n",user_key.ToString().c_str(),partition);
      s=GetKeyinSortedStore(options,LogFile,Lkey,&saver,partition,readDataSizeActual,tableCacheNum,blockCacheNum);
      if(saver.state==kFound){
        return s;
      }
    }
   }
    return Status::NotFound(Slice());  // Use an empty error message for speed
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != NULL) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == NULL) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }
  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() {
  ++refs_;
}

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key,int partition) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), pfiles_[partition][level],
                               smallest_user_key, largest_user_key);
}

int Version::PickLevelForMemTableOutput(
    const Slice& smallest_user_key,
    const Slice& largest_user_key,int partition) {
  int level = 0;
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key,partition)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key,partition)) {
        break;
      }
      if (level + 2 < config::kNumLevels+config::kTempLevel) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps,partition);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}


void Version::GetOverlappingInputs(
    int level,
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<FileMetaData*>* inputs,int partition) {
  assert(level >= 0);
  assert(level < config::kNumLevels+config::kTempLevel);
  inputs->clear();
  Slice user_begin, user_end;
  
  if (begin != NULL) {
    user_begin = begin->user_key();
  }
  if (end != NULL) {
    user_end = end->user_key();
  }
  double keyRange=atof(user_end.ToString().c_str())-atof(user_begin.ToString().c_str());
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  std::vector<FileMetaData*>& myfiles=pfiles_[partition][level];
  for (size_t i = 0; i < pfiles_[partition][level].size(); ) {////////////////////////////
    FileMetaData* f = myfiles[i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != NULL && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != NULL && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    }
    else {
      inputs->push_back(f);
    }
  }
 }
 
 void Version::GetAllOverlappingInputs(
    int level,
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<FileMetaData*>* inputs,int partition) {
    assert(level >= 0);
    assert(level < config::kNumLevels+config::kTempLevel);
    std::vector<FileMetaData*>& myfiles=pfiles_[partition][level];
    for (size_t i = 0; i < pfiles_[partition][level].size();i++) {
      FileMetaData* f = myfiles[i];
      inputs->push_back(f);
    }
 }

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::GetOverlappingInputsL0(
    int level,
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<FileMetaData*>* inputs,int partition) {
  assert(level >= 0);
  assert(level < config::kNumLevels+config::kTempLevel);
  inputs->clear();//
  Slice user_begin, user_end;
  if (begin != NULL) {
    user_begin = begin->user_key();
  }
  if (end != NULL) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  std::vector<FileMetaData*>& myfiles=pfiles_[partition][level];
  for (size_t i = 0; i < myfiles.size(); ) {//
    FileMetaData* f = myfiles[i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != NULL && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != NULL && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    }
    else {
      inputs->push_back(f);
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for(int k=0;k<config::kNumPartition;k++){
    for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
      // E.g.,
      //   --- level 1 ---
      //   17:123['a' .. 'd']
      //   20:43['e' .. 'g']
      r.append("--- partition ");
      AppendNumberTo(&r, k);
      r.append("--- level ");
      AppendNumberTo(&r, level);
      r.append(" ---\n");
      const std::vector<FileMetaData*>& files = pfiles_[k][level];
      for (size_t i = 0; i < files.size(); i++) {
	r.push_back(' ');
	AppendNumberTo(&r, files[i]->number);
	r.push_back(':');
	AppendNumberTo(&r, files[i]->file_size);
	r.append("[");
	r.append(files[i]->smallest.DebugString());
	r.append(" .. ");
	r.append(files[i]->largest.DebugString());
	r.append("]\n");
      }
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumPartition][config::kNumLevels+config::kTempLevel];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base)
      : vset_(vset),
        base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for(int k=0;k<config::kNumPartition;k++){
      for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
	      levels_[k][level].added_files = new FileSet(cmp);
      }
    }
  }

  ~Builder() {
    for(int k=0;k<config::kNumPartition;k++){
      for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++){
          const FileSet* added = levels_[k][level].added_files;
          std::vector<FileMetaData*> to_unref;
          to_unref.reserve(added->size());
          for (FileSet::const_iterator it = added->begin();
              it != added->end(); ++it) {
            to_unref.push_back(*it);
          }
          delete added;
          for (uint32_t i = 0; i < to_unref.size(); i++){
            FileMetaData* f = to_unref[i];
            f->refs--;
            if (f->refs <= 0){
              delete f;
            }
          }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
    for(int k=0;k<config::kNumPartition;k++){
      for (size_t i = 0; i < edit->compact_pointers_[k].size(); i++) {
        const int level = edit->compact_pointers_[k][i].first;
        vset_->compact_pointer_[k][level] = edit->compact_pointers_[k][i].second.Encode().ToString();
      }
      // Delete files
      const VersionEdit::DeletedFileSet& del = edit->deleted_files_[k];
      for (VersionEdit::DeletedFileSet::const_iterator iter = del.begin();
          iter != del.end();
          ++iter) {
        const int level = iter->first;
        const uint64_t number = iter->second;
        levels_[k][level].deleted_files.insert(number);//////////////delete old SSTables
      }
      // Add new files
      for (size_t i = 0; i < edit->new_files_[k].size(); i++) {
        const int level = edit->new_files_[k][i].first;
        FileMetaData* f = new FileMetaData(edit->new_files_[k][i].second);///////////
        f->refs = 1;
        // We arrange to automatically compact this file after
        // a certain number of seeks.  Let's assume:
        //   (1) One seek costs 10ms
        //   (2) Writing or reading 1MB costs 10ms (100MB/s)
        //   (3) A compaction of 1MB does 25MB of IO:
        //         1MB read from this level
        //         10-12MB read from next level (boundaries may be misaligned)
        //         10-12MB written to next level
        // This implies that 25 seeks cost the same as the compaction
        // of 1MB of data.  I.e., one seek costs approximately the
        // same as the compaction of 40KB of data.  We are a little
        // conservative and allow approximately one seek for every 16KB
        // of data before triggering a compaction.
  /*      f->allowed_seeks = (f->file_size / 16384);//
        if (f->allowed_seeks < 100) f->allowed_seeks = 100;//
  */	 f->allowed_seeks =f->file_size;//
        levels_[f->partition][level].deleted_files.erase(f->number);//
        levels_[f->partition][level].added_files->insert(f);//
      }
    }
  }
  
  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit,int partition) {
    // Update compaction pointers
      for (size_t i = 0; i < edit->compact_pointers_[partition].size(); i++) {
        const int level = edit->compact_pointers_[partition][i].first;
        vset_->compact_pointer_[partition][level] =
            edit->compact_pointers_[partition][i].second.Encode().ToString();
      }
    // Delete files
    const VersionEdit::DeletedFileSet& del = edit->deleted_files_[partition];
    for (VersionEdit::DeletedFileSet::const_iterator iter = del.begin();
         iter != del.end();
         ++iter) {
      const int level = iter->first;
      const uint64_t number = iter->second;
      levels_[partition][level].deleted_files.insert(number);////delete old SSTables
    }
    // Add new files
    //fprintf(stderr, "In Apply,edit->new_files_[partition].size():%d,partition:%d!!\n",edit->new_files_[partition].size(),partition);
    for (size_t i = 0; i < edit->new_files_[partition].size(); i++) {
      const int level = edit->new_files_[partition][i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[partition][i].second);//
      f->refs = 1;
      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
/*      f->allowed_seeks = (f->file_size / 16384);///
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;//
*/	 f->allowed_seeks =f->file_size;//
      levels_[partition][level].deleted_files.erase(f->number);//
      levels_[partition][level].added_files->insert(f);//
    }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for(int k=0;k<config::kNumPartition;k++){
      for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
        // Merge the set of added files with the set of pre-existing files.
        // Drop any deleted files.  Store the result in *v.
        const std::vector<FileMetaData*>& base_files = base_->pfiles_[k][level];
        std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
        std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
        const FileSet* added = levels_[k][level].added_files;//
        v->pfiles_[k][level].reserve(base_files.size() + added->size());
        for (FileSet::const_iterator added_iter = added->begin();//
            added_iter != added->end();
            ++added_iter) {
          // Add all smaller files listed in base_
          for (std::vector<FileMetaData*>::const_iterator bpos
            = std::upper_bound(base_iter, base_end, *added_iter, cmp);
              base_iter != bpos;
              ++base_iter) {
            MaybeAddFile(v, level, *base_iter,k);
          }
          MaybeAddFile(v, level, *added_iter,k);
        }

        // Add remaining base files
        for (; base_iter != base_end; ++base_iter) {
          MaybeAddFile(v, level, *base_iter,k);
        }
        #ifndef NDEBUG
        // Make sure there is no overlap in levels > 0
        if (level > 0) {
          for (uint32_t i = 1; i < v->pfiles_[k][level].size(); i++) {
            const InternalKey& prev_end = v->pfiles_[k][level][i-1]->largest;
            const InternalKey& this_begin = v->pfiles_[k][level][i]->smallest;
            if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
              fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                prev_end.DebugString().c_str(),
                this_begin.DebugString().c_str());
              abort();
            }
          }
        }
        #endif
      }
    }
  }
 
   void SaveTo(Version* v,int partition) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for(int k=0;k<config::kNumPartition;k++){
        for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
          // Merge the set of added files with the set of pre-existing files.
          // Drop any deleted files.  Store the result in *v.   
          const std::vector<FileMetaData*>& base_files = base_->pfiles_[k][level];
          std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
          std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
          const FileSet* added = levels_[k][level].added_files;//
          v->pfiles_[k][level].reserve(base_files.size() + added->size());
          for (FileSet::const_iterator added_iter = added->begin();
              added_iter != added->end();
              ++added_iter) {
            // Add all smaller files listed in base_
            for (std::vector<FileMetaData*>::const_iterator bpos
                    = std::upper_bound(base_iter, base_end, *added_iter, cmp);
                base_iter != bpos;
                ++base_iter) {
              MaybeAddFile(v, level, *base_iter,k);
            }

            MaybeAddFile(v, level, *added_iter,k);
          }
          // Add remaining base files
          for (; base_iter != base_end; ++base_iter) {
            MaybeAddFile(v, level, *base_iter,k);
          }
    #ifndef NDEBUG
          // Make sure there is no overlap in levels > 0
          if (level > 0) {
            for (uint32_t i = 1; i < v->pfiles_[k][level].size(); i++) {
        std::vector<FileMetaData*>& files=v->pfiles_[k][level];
              const InternalKey& prev_end = files[i-1]->largest;
              const InternalKey& this_begin = files[i]->smallest;
              if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
                fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                        prev_end.DebugString().c_str(),
                        this_begin.DebugString().c_str());
                abort();
              }
            }
          }
    #endif
        }
    }
  }
  
  void MaybeAddFile(Version* v, int level, FileMetaData* f,int partition) {
    if (levels_[partition][level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
      //fprintf(stderr, "File is deleted: do nothing\n");
    } else {
      std::vector<FileMetaData*>* files = &v->pfiles_[partition][level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);
    }
  }
  
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
    //  log_number_(0),
    //  prev_log_number_(0),
      descriptor_file_(NULL),
      descriptor_log_(NULL),
      dummy_versions_(this),
      current_(NULL) {
      for(int i=0;i<config::kNumPartition;i++){
        //last_sequence_[i]=0;
          log_number_[i]=0;
          prev_log_number_[i]=0;
      }
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != NULL) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

 void VersionSet::initVersion(){
      for(int i=0;i<config::kNumPartition;i++){
       //versionPartition_Info[i].smallestCharKey=" ";
       versionPartition_Info[i].partitionID=i;
       versionPartition_Info[i].nextID=-1;
    }
}

int VersionSet::initFileMetadataAndprefetchPinterFile(int partitionNum){
  Version* current = current_;
    current->Ref();
    Iterator* iter;
    int emptyDB=1;
    printf("before initFileMetadataAndprefetchPinterFile\n");
    fprintf(stderr,"before initFileMetadataAndprefetchPinterFile\n");
    for(int p=0;p<partitionNum;p++){
      for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
        //std::vector<FileMetaData*>* files = &current->pfiles_[p][level];
        const std::vector<FileMetaData*>& files = current_->pfiles_[p][level];
        for (int i = 0; i < files.size(); i++) {
          emptyDB=0;
          FileMetaData* file =files[i];
          //table_cache_->SetFileMetaDataMap(file->number, file->file_size, file->smallest, file->largest);
          uint64_t file_number = file->number;
          uint64_t file_size = file->file_size;
          if(level==0){
             iter = table_cache_->NewIterator(ReadOptions(), file_number, file_size,false,true);//false,cache metadata for unsortedStore
          }else{
             iter = table_cache_->NewIterator(ReadOptions(), file_number, file_size,false,true); //cache metadata and prefetch pointer files
          }
        }
      }
    }
    current->Unref();
    printf("after initFileMetadataAndprefetchPinterFile\n");
    fprintf(stderr,"after initFileMetadataAndprefetchPinterFile\n");
    return emptyDB;
}

void VersionSet::InitializeTableCacheFileMetaData() {
	Version* current = current_;
	current->Ref();
for(int p=0;p<config::kNumPartition;p++){
	for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
    std::vector<FileMetaData*>* files = &current->pfiles_[p][level];
		for (int i = 0; i < files->size(); i++) {
			//FileMetaData* file =files[i];
			//table_cache_->SetFileMetaDataMap(file->number, file->file_size, file->smallest, file->largest);
		}
	}
}
	current->Unref();
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {//
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_[0]);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_[0],0);
  }
  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_[0],0);
  }
  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);
  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);//
    builder.SaveTo(v);
  }
  Finalize(v);
  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == NULL) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == NULL);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }
  // Unlock during expensive MANIFEST log write
  {
   // mu->Unlock();
    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }
    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }
   // mu->Lock();
  }
  // Install the new version
  if (s.ok()) {
    AppendVersion(v);
   // log_number_= edit->log_number_;
    //prev_log_number_= edit->prev_log_number_;
     for(int k=0;k<config::kNumPartition;k++){
        log_number_[k]= edit->log_number_[k];
        prev_log_number_[k]= edit->prev_log_number_[k];
      }
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = NULL;
      descriptor_file_ = NULL;
      env_->DeleteFile(new_manifest_file);
    }
  }
  return s;
}


Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu,int partition) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_[partition]);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_[partition],partition);
  }
  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_[partition],partition);
  }
  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);
  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit,partition);
    builder.SaveTo(v,partition);
  }
  Finalize(v,partition);//
  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == NULL) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == NULL);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }
  // Unlock during expensive MANIFEST log write
  {
    //mu->Unlock();
    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
        printf("MANIFEST write: %s failed !!",s.ToString().c_str());
      }
    }
    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }
    //mu->Lock();
  }
  // Install the new version
  if (s.ok()) {
    AppendVersion(v);
     log_number_[partition] = edit->log_number_[partition];
    prev_log_number_[partition] = edit->prev_log_number_[partition];
  } else {
    printf("delete version in partition: %d, s.ok():%d !!",partition,s.ok());
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = NULL;
      descriptor_file_ = NULL;
      env_->DeleteFile(new_manifest_file);
    }
  }
  return s;
}

Status VersionSet::Recover(bool *save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };
  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);
  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption(
            "CURRENT points to a non-existent file", s.ToString());
    }
    return s;
  }
  //bool have_log_number = false;
  bool have_log_number[config::kNumPartition]={ false};
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file =0;
  uint64_t last_sequence = 0;
  //uint64_t log_number= 0;
  //uint64_t prev_log_number =0;
  uint64_t log_number[config::kNumPartition]= {0};
  uint64_t prev_log_number[config::kNumPartition]={0};
  Builder builder(this, current_);
  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
     // std::cerr<<"before DecodeFrom, record:"<<record.ToString().c_str()<<"\n";
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }
      if (s.ok()) {
        for(int i=0;i<edit.partition_InfoVec.size();i++){
            strcpy(versionPartition_Info[i].smallestCharKey,edit.partition_InfoVec[i].smallestCharKey);
            versionPartition_Info[i].partitionID=edit.partition_InfoVec[i].partitionID;
            versionPartition_Info[i].nextID=edit.partition_InfoVec[i].nextID;
            printf("in recovery,size:%d, partitionID:%d,key:%s,nextID:%d\n",edit.partition_InfoVec.size(),versionPartition_Info[i].partitionID,versionPartition_Info[i].smallestCharKey,versionPartition_Info[i].nextID);
        
          }
          builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        for(int k=0;k<config::kNumPartition;k++){//
          log_number[k]= edit.log_number_[k];
          have_log_number[k]= true;
        }
      }
      if (edit.has_prev_log_number_) {
        for(int k=0;k<config::kNumPartition;k++){//
          prev_log_number[k]= edit.prev_log_number_[k];
        }
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file= edit.next_file_number_;
        have_next_file = true;
      }
      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
      //break;
    }
  }
  delete file;
  file = NULL;
  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }
    if (!have_prev_log_number) {
      for(int k=0;k<config::kNumPartition;k++){
          prev_log_number[k] = 0;
      }
     //memset( prev_log_number, 0,config::kNumPartition);
    }
    for(int k=0;k<config::kNumPartition;k++){
      MarkFileNumberUsed(prev_log_number[k]);
      MarkFileNumberUsed(log_number[k]);
    }
  }
  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
	  next_file_number_= next_file + 1;
    for(int k=0;k<config::kNumPartition;k++){
      //next_file_number_[k]= next_file[k] + 1;
      log_number_ [k]= log_number[k];
      prev_log_number_[k] = prev_log_number[k];
    }
    last_sequence_ = last_sequence;
    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    }else {
      *save_manifest = true;
    }
  }
  return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == NULL);
  assert(descriptor_log_ == NULL);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == NULL);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_<= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v,int partition) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;
  //for(int k=0;k<config::kNumPartition;k++){
  for (int level = 0; level < config::kNumLevels+config::kTempLevel-1; level++) {
      double score;
      if (level == 0) {
	// We treat level-0 specially by bounding the number of files
	// instead of number of bytes for two reasons:
	//
	// (1) With larger write-buffer sizes, it is nice not to do too
	// many level-0 compactions.
	//
	// (2) The files in level-0 are merged on every read and
	// therefore we wish to avoid too many files when the individual
	// file size is small (perhaps because of a small write-buffer
	// setting, or very high compression ratios, or lots of
	// overwrites/deletions).
	    score = v->pfiles_[partition][level].size() /
	    static_cast<double>(config::kL0_CompactionTrigger);
      //v->compaction_level_[partition] = level;
      //v->compaction_score_[partition] = score;
      } else {
          // Compute the ratio of current size to size limit.
          const uint64_t level_bytes = TotalFileSize(v->pfiles_[partition][level]);
          score = static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
      }

      if (score > best_score) {
        best_level = level;
        best_score = score;
      }
    }
    v->compaction_level_[partition] = best_level;
    v->compaction_score_[partition] = best_score;
  //}
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;
  for(int k=0;k<config::kNumPartition;k++){
    for (int level = 0; level < config::kNumLevels+config::kTempLevel-1; level++) {
      double score;
      if (level == 0) {
	// We treat level-0 specially by bounding the number of files
	// instead of number of bytes for two reasons:
	//
	// (1) With larger write-buffer sizes, it is nice not to do too
	// many level-0 compactions.
	//
	// (2) The files in level-0 are merged on every read and
	// therefore we wish to avoid too many files when the individual
	// file size is small (perhaps because of a small write-buffer
	// setting, or very high compression ratios, or lots of
	// overwrites/deletions).
	      score = v->pfiles_[k][level].size() / static_cast<double>(config::kL0_CompactionTrigger);
      } else {
        // Compute the ratio of current size to size limit.
        const uint64_t level_bytes = TotalFileSize(v->pfiles_[k][level]);
        score = static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
      }
      if (score > best_score) {
        best_level = level;
        best_score = score;
      }
    }
    v->compaction_level_[k] = best_level;
    v->compaction_score_[k] = best_score;
  }
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?
  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());
  // Save compaction pointers
  for(int k=0;k<config::kNumPartition;k++){
    for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
      if (!compact_pointer_[k][level].empty()) {
        InternalKey key;
        key.DecodeFrom(compact_pointer_[k][level]);
        edit.SetCompactPointer(level, key,k);
      }
    }
  }

  // Save files
  for(int k=0;k<config::kNumPartition;k++){
    for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
      const std::vector<FileMetaData*>& files = current_->pfiles_[k][level];
      for (size_t i = 0; i < files.size(); i++) {
        const FileMetaData* f = files[i];
        edit.AddFile(level, f->number,f->partition, f->file_size, f->smallest, f->largest);
      }
    }
  }
  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level,int partition) const {
  assert(level >= 0);
  assert(level < config::kNumLevels+config::kTempLevel);
  int numFiles=0;
  numFiles= current_->pfiles_[partition][level].size();
  return numFiles;
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  assert(config::kNumLevels+config::kTempLevel== 7);
  for(int k=0;k<config::kNumPartition;k++){
    snprintf(scratch->buffer, sizeof(scratch->buffer),
	    "files[ %d %d %d %d %d %d %d ]",
	    int(current_->pfiles_[k][0].size()),
	    int(current_->pfiles_[k][1].size()),
	    int(current_->pfiles_[k][2].size()),
	    int(current_->pfiles_[k][3].size()),
	    int(current_->pfiles_[k][4].size()),
	    int(current_->pfiles_[k][5].size()),
	    int(current_->pfiles_[k][6].size()));
  }
  return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  unsigned int kn=strtoul(ikey.user_key().ToString().c_str(),NULL,10);
  //int partition=kn%config::kNumPartition;
  int partition=kn/config::baseRange;
  for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
    const std::vector<FileMetaData*>& files = v->pfiles_[partition][level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, false,config::seekPrefetch,&tableptr);
        if (tableptr != NULL) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live,int partition) {
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
      for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
        const std::vector<FileMetaData*>& files = v->pfiles_[partition][level];
        for (size_t i = 0; i < files.size(); i++) {
          live->insert(files[i]->number);
        }
      }
  }
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for(int k=0;k<config::kNumPartition;k++){
      for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
          const std::vector<FileMetaData*>& files = v->pfiles_[k][level];
          for (size_t i = 0; i < files.size(); i++) {
            live->insert(files[i]->number);
          }
      }
    }
  }
}

void VersionSet::fileLocation(uint64_t number,int partitionNum, int* partition, int* fileLevel) {
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for(int k=0;k<partitionNum;k++){
      for (int level = 0; level < config::kNumLevels+config::kTempLevel; level++) {
        const std::vector<FileMetaData*>& files = v->pfiles_[k][level];
        for (size_t i = 0; i < files.size(); i++) {
          if(files[i]->number==number){
              *partition=k;
              *fileLevel=level;
              fprintf(stderr,"invalid number:%d in partition:%d,level:%d\n",number,*partition,*fileLevel);
          }         
        }
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level,int partition) const {
  assert(level >= 0);
  assert(level < config::kNumLevels+config::kTempLevel);
  return TotalFileSize(current_->pfiles_[partition][level]);
}

int64_t VersionSet::TotalPartitionBytes(int partition,int keySize, int valueSize) const {
  int64_t sum = 0;
  std::vector<FileMetaData*> &L0files=current_->pfiles_[partition][0];
  for (size_t i = 0; i < L0files.size(); i++) {
	  sum += L0files[i]->file_size;
  }
  std::vector<FileMetaData*> &L1files=current_->pfiles_[partition][1];
  float ratio=float(keySize+valueSize)/float(keySize+config::pointerSize);
  //fprintf(stderr,"ratio:%lf,keySize:%d,valueSize:%d\n",ratio,keySize,valueSize);
  for (size_t i = 0; i < L1files.size(); i++) {
	  sum += L1files[i]->file_size*ratio;
  }
  return sum;
  //return TotalFileSize(current_->pfiles_[partition][level]);
}

int64_t VersionSet::flushL0Bytes(int partition) const {
  int64_t sum = 0;
  std::vector<FileMetaData*> &L0files=current_->pfiles_[partition][0];
  for (size_t i = 0; i < L0files.size(); i++) {
	  sum += L0files[i]->file_size;
  }
  return sum;
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for(int k=0;k<config::kNumPartition;k++){
    for (int level = 1; level < config::kNumLevels+config::kTempLevel - 1; level++) {
      std::vector<FileMetaData*> &files=current_->pfiles_[k][level];/////////////////
      for (size_t i = 0; i < current_->pfiles_[k][level].size(); i++) {
          const FileMetaData* f = files[i];
          current_->GetOverlappingInputs(level+1, &f->smallest, &f->largest,
                      &overlaps,k);
          const int64_t sum = TotalFileSize(overlaps);
          if (sum > result) {
            result = sum;
          }
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest,
                          InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest,
                           InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeUnsortedStoreIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;
  //const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  const int space = c->inputs_[0].size()+1;
  Iterator** list = new Iterator*[space];
  int num = 0;
  const std::vector<FileMetaData*>& files = c->inputs_[0];
  for (size_t i = 0; i < files.size(); i++) {
      list[num++] = table_cache_->NewIterator(
      options, files[i]->number, files[i]->file_size,false,true);///////get Iter of each SSTable,read data from cache or disk
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

Iterator* VersionSet::MakeGCterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;
  //const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  const int space = 2;
  int level=1;
  Iterator** list = new Iterator*[space];
  int num = 0;
  // Create concatenating iterator for the files from this level
  list[num++] = NewTwoLevelIterator(
      new Version::LevelFileNumIterator(icmp_, &c->inputs_[level]),
      &GetFileIterator, table_cache_, options);//readData
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}


Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;
  int space;
  space =c->inputs_[0].size() +config::kNumLevels+config::kTempLevel-1;
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < config::kNumLevels+config::kTempLevel; which++) {
    if (!c->inputs_[which].empty()) {
      //fprintf(stderr,"which:%d,validFiles.size:%d,c->inputs_[which]:%d\n",which,validFiles[which].size(),c->inputs_[which].size());
      if (c->level() + which == 0) {
          const std::vector<FileMetaData*>& files = c->inputs_[which];
          for (size_t i = 0; i < files.size(); i++) {
              list[num++]=table_cache_->NewIterator(options, files[i]->number, files[i]->file_size,false,true);
          //list[num++]=table_cache_->NewIterator(options, files[i]->number, files[i]->file_size,HashIndex,myReduceIOInComp);    
          }
      }else {
        // Create concatenating iterator for the files from this level
           list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),&GetFileIterator, table_cache_, options);//readData
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

Compaction* VersionSet::PickSortedStoreForGC(int partition) {
  Compaction* c;
  int level=1;
  //c->inputs_[0].clear();
  //c->inputs_[1].clear();
  c = new Compaction(options_, level);
  std::vector<FileMetaData*> &files=current_->pfiles_[partition][level];//
  for (size_t i = 0; i < files.size(); i++) {
    FileMetaData* f = files[i];
    c->inputs_[level].push_back(f);//
  }
  c->input_version_ = current_;
  c->input_version_->Ref();
  return c;
}

int VersionSet::GetSortedStoreMiddleKey(std::string* middleKey,int partition){
  int level=1;
  std::vector<FileMetaData*> &files=current_->pfiles_[partition][level];
  if(files.size()>1){
    int middleFileID=files.size()/2;
    *middleKey=files[middleFileID]->smallest.user_key().ToString();
    return 0;
  }else{
    return 1;
  }
}


Compaction* VersionSet::PickAllCompaction(int partition) {
  Compaction* c;
  int level=0;
  const std::vector<FileMetaData*>& myfiles=current_->pfiles_[partition][level];
  current_->compaction_score_[partition]=myfiles.size() / static_cast<double>(config::kL0_CompactionTrigger);
  printf("in PickAllCompaction, partition:%d,L0 size:%d,L1 size:%d,L2:%d\n",partition,current_->pfiles_[partition][0].size(),current_->pfiles_[partition][1].size(),current_->pfiles_[partition][2].size());	    
  const bool size_compaction =true;// (current_->compaction_score_[partition]>= 1);
  const bool seek_compaction = (current_->file_to_compact_ != NULL);
  if (size_compaction) {
    level =0;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels+config::kTempLevel);
    c = new Compaction(options_, level);
    // Pick the first file that comes after compact_pointer_[level]
    std::vector<FileMetaData*> &files=current_->pfiles_[partition][level];//
    for (size_t i = 0; i < files.size(); i++) {
      FileMetaData* f = files[i];
      c->inputs_[0].push_back(f);
    }
  } else {
    return NULL;
  }
  c->input_version_ = current_;
  c->input_version_->Ref();
  SetupOtherInputs(c,partition,true);
  return c;
}

Compaction* VersionSet::PickAllL0Compaction(int partition) {
  Compaction* c;
  int level=0;
  const std::vector<FileMetaData*>& myfiles=current_->pfiles_[partition][level];
  current_->compaction_score_[partition]=myfiles.size() / static_cast<double>(config::kL0_CompactionTrigger);
  printf("in PickAllL0Compaction, partition:%d,L0 size:%d,L1 size:%d\n",partition,current_->pfiles_[partition][0].size(),current_->pfiles_[partition][1].size());	    
  const bool size_compaction =true;// (current_->compaction_score_[partition]>= 1);
  const bool seek_compaction = (current_->file_to_compact_ != NULL);
  if (size_compaction) {
    level =0;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels+config::kTempLevel);
    c = new Compaction(options_, level);
    // Pick the first file that comes after compact_pointer_[level]
    std::vector<FileMetaData*> &files=current_->pfiles_[partition][level];//
    for (size_t i = 0; i < files.size(); i++) {
      FileMetaData* f = files[i];
      c->inputs_[0].push_back(f);
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return NULL;
  }
  c->input_version_ = current_;
  c->input_version_->Ref();
  return c;
}

Compaction* VersionSet::PickSomeHashIndexFileCompaction(int partition) {
  Compaction* c;
  int level=0;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels+config::kTempLevel);
    c = new Compaction(options_, level);
    // Pick the first file that comes after compact_pointer_[level]
    std::vector<FileMetaData*> &files=current_->pfiles_[partition][level];
    int pickedNum=files.size();
    //int pickedNum=files.size()-config::triggerSizeBasedMerge;//-config::limitSacnFiles;
    for (size_t i = 0; i < pickedNum; i++) {
      FileMetaData* f = files[i];
      c->inputs_[0].push_back(f);
    }
  c->input_version_ = current_;
  c->input_version_->Ref();
  return c;
}

Compaction* VersionSet::PickCompaction(int partition) {
  Compaction* c;
  int level=config::kNumLevels+config::kTempLevel-2;
  const std::vector<FileMetaData*>& myfiles=current_->pfiles_[partition][level];
  current_->compaction_score_[partition]=myfiles.size() / static_cast<double>(config::kL0_CompactionTrigger);	
  const bool size_compaction =true;
  const bool seek_compaction = (current_->file_to_compact_ != NULL);
  if (size_compaction) {
    assert(level >= 0);
    assert(level+1 < config::kNumLevels+config::kTempLevel);
    c = new Compaction(options_, level);
    for(int curLevel=config::kNumLevels+config::kTempLevel-2;curLevel>0;curLevel--){
      std::vector<FileMetaData*> &files=current_->pfiles_[partition][curLevel];//
      for (size_t i = 0; i < files.size(); i++) {
        FileMetaData* f = files[i];
        c->inputs_[curLevel].push_back(f);
      }
    }
  } else {
    return NULL;
  }
  c->input_version_ = current_;
  c->input_version_->Ref();

  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    current_->GetOverlappingInputsL0(0, &smallest, &largest, &c->inputs_[0],partition);
    assert(!c->inputs_[0].empty());
  }
  SetupOtherInputs(c,partition,false);
  return c;
}

void VersionSet::SetupOtherInputs(Compaction* c,int partition,bool setupAll) {
  const int level = c->level();
  InternalKey smallest, largest;
  GetRange(c->inputs_[level], &smallest, &largest);//////smallest and largest key in L0 overlapped SStables
  current_->GetAllOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[level+1],partition);
  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[partition][level] = largest.Encode().ToString(); //设置新的compact_pointer
  c->edit_.SetCompactPointer(level, largest,partition);
}

Compaction* VersionSet::CompactRange(
    int level,
    const InternalKey* begin,
    const InternalKey* end,int partition) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs,partition);
  if (inputs.empty()) {
    return NULL;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c,partition,false);
  return c;
}


Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(NULL),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for(int k=0;k<config::kNumPartition;k++){
    for (int i = 0; i < config::kNumLevels+config::kTempLevel; i++) {
      level_ptrs_[k][i] = 0;
    }
  }
}

Compaction::~Compaction() {
  if (input_version_ != NULL) {
    input_version_->Unref();
  }
}

bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

int Compaction::GetSortedStoreBeginMiddleKey(char* middleSmallKey,char* beginSmallKey,int partition) {
    const std::vector<FileMetaData*>& myfiles = input_version_->pfiles_[partition][config::kNumLevels+config::kTempLevel-1];
    if(myfiles.size()>0){
      int middle=myfiles.size()/2;
      strcpy(beginSmallKey,myfiles[0]->smallest.user_key().ToString().c_str());
      strcpy(middleSmallKey,myfiles[middle]->smallest.user_key().ToString().c_str());
      printf("partition:%d,size:%d,middle smallest key:%s,beginSmallKey:%s\n",partition,myfiles.size(),myfiles[middle]->smallest.user_key().ToString().c_str(),myfiles[0]->smallest.user_key().ToString().c_str());
      return 0;
    }else{
      printf("SortedStore is empty\n");
      return 1;
    }
 }

void Compaction::updateEditMetaForLastLevel(VersionEdit* edit,int partition,int newPartition) {
    const std::vector<FileMetaData*>& myfiles = input_version_->pfiles_[partition][config::kNumLevels+config::kTempLevel-1];
    int middle=myfiles.size()/2;
    printf("last level size:%d,middle:%d\n",myfiles.size(),middle);
    for(int i=middle;i<myfiles.size();i++){
	    edit->AddFile(config::kNumLevels+config::kTempLevel-1,myfiles[i]->number,newPartition, myfiles[i]->file_size, myfiles[i]->smallest, myfiles[i]->largest);
	    edit->DeleteFile(config::kNumLevels+config::kTempLevel-1, partition,  myfiles[i]->number);////
    }
}

void Compaction::AddInputDeletions(VersionEdit* edit,int partition,bool split) {
    for (int which = 0; which <config::kNumLevels+config::kTempLevel; which++) {
        for (size_t i = 0; i < inputs_[which].size(); i++) {
          //edit->DeleteFile(level_ + which, partition, inputs_[which][i]->number);///////delete old SSTable File after compaction
			    edit->DeleteFile(which, partition, inputs_[which][i]->number);///////delete old SSTable File after compaction
          //printf("delete file meta,which:%d,partition:%d,fileID:%d\n",which, partition, inputs_[which][i]->number);
        }
    }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  unsigned int kn=strtoul(user_key.ToString().c_str(),NULL,10);
  //int partition=kn%config::kNumPartition;
  int partition=kn/config::baseRange;
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels+config::kTempLevel; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->pfiles_[partition][lvl];
    for (; level_ptrs_[partition][lvl] < files.size(); ) {
      FileMetaData* f = files[level_ptrs_[partition][lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[partition][lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
      icmp->Compare(internal_key,
                    grandparents_[grandparent_index_]->largest.Encode()) > 0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != NULL) {
    input_version_->Unref();
    input_version_ = NULL;
  }
}

}  // namespace leveldb
