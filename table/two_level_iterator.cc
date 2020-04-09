// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "unikv/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"
#include "db/hashfunc.h"
//#include "db/global.h"

namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator: public Iterator {
 public:
  TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options);
     
     
/*  TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options,uint64_t file_number,IndexEntry* myHashIndex,long int* myReduceIOInComp);
  
  TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options,uint64_t file_number,IndexEntry* myHashIndex,int *inValidKey,bool flag);*/

  virtual ~TwoLevelIterator();

  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();

  virtual bool Valid() const {
    return data_iter_.Valid();
  }
  virtual Slice key() const {
    assert(Valid());
    return data_iter_.key();
  }
  virtual Slice value() const {
    assert(Valid());
    return data_iter_.value();
  }
  virtual Status status() const {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != NULL && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  //void SkipInvalidDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  //int CountInValidKey();

  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  IteratorWrapper index_iter_;
  IteratorWrapper data_iter_; // May be NULL
  // If data_iter_ is non-NULL, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;
  uint64_t TLfile_number;
  //int* HashKtoTN;
//  IndexEntry* HashIndexEntry;
  long int* ReduceIOInComp;
  HashFunc hashFun;
  //long int* TotalIOInComp;
};
TwoLevelIterator::TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(NULL){
	//HashKtoTN=NULL;
//	HashIndexEntry=NULL;
}

/*TwoLevelIterator::TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options,uint64_t file_number,IndexEntry* myHashIndex,long int* myReduceIOInComp)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(NULL), 
      TLfile_number(file_number)
     {
       ReduceIOInComp=myReduceIOInComp;
     //  TotalIOInComp=myTotalIOInComp;
        //HashKtoTN=myHashKtoTN;
        HashIndexEntry=myHashIndex;
}

TwoLevelIterator::TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options,uint64_t file_number,IndexEntry* myHashIndex,int *inValidKey,bool flag)
      : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(NULL), 
      TLfile_number(file_number)
      {
	 HashIndexEntry=myHashIndex;
	 //*inValidKey=CountInValidKey();
}*/

TwoLevelIterator::~TwoLevelIterator() {
}



void TwoLevelIterator::Seek(const Slice& target) {///////////////////////////////////////////////////////////
  index_iter_.Seek(target);
  //////////////////////////////
   /*while(index_iter_.Valid() && index_iter_.value()==Slice("n")){
	    index_iter_.Next();
       // printf("index_iter_.key():%d !!\n",atoi(index_iter_.key().ToString().c_str()));
    }*/
  ///////////////////////////
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.Seek(target);
  
   SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
   // printf("index_iter_.key():%d !!\n",atoi(index_iter_.key().ToString().c_str()));
    /*while(index_iter_.Valid() && index_iter_.value()==Slice("n")){
	    index_iter_.Next();
       // printf("index_iter_.key():%d !!\n",atoi(index_iter_.key().ToString().c_str()));
    }*/
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
   SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
   SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}


void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Next();
  // fprintf(stderr,"key:%d\n",atoi(index_iter_.key().ToString().c_str()));
     /* if(HashIndexEntry!=NULL){   
	  int count=0,changeNum=0;
	  //byte* tableNumBytes=new byte[3];
	  byte* keyBytes=new byte[4];
	  while(index_iter_.Valid() && index_iter_.value()==Slice("n")){
	    count++;
	    int tableNum=-1;
	   // int IntKey=atoi(index_iter_.key().ToString().c_str());
	   unsigned int IntKey;
	    if(index_iter_.key().ToString().length()>20){
	      IntKey=hashFun.RSHash((char*)index_iter_.key().ToString().substr(0,config::kKeyLength).c_str());
	    }else{
	      IntKey=atoi(index_iter_.key().ToString().c_str());
	    }
	  // fprintf(stderr,"-key:%d,HashKtoTN[IntKey]:%d\n",IntKey,HashKtoTN[IntKey]);
		intTo4Byte(IntKey,keyBytes);    
		int bucketNumber=IntKey%config::bucketNum;
		if(HashIndexEntry[bucketNumber].KeyTag[0]==keyBytes[2] && HashIndexEntry[bucketNumber].KeyTag[1]==keyBytes[3] && HashIndexEntry[bucketNumber].TableNum[0]!=0){
		    //tableNumBytes[0]=HashIndexEntry[bucketNumber].TableNum[0];
		    //tableNumBytes[1]=HashIndexEntry[bucketNumber].TableNum[1];
		    //tableNumBytes[2]=HashIndexEntry[bucketNumber].TableNum[2];
		    //tableNumBytes[3]=HashIndexEntry[bucketNumber].TableNum[3];
		    tableNum=bytes3ToInt(HashIndexEntry[bucketNumber].TableNum);
		}else{
		  IndexEntry* nextIndexEntry=HashIndexEntry[bucketNumber].nextEntry;
		  while(nextIndexEntry!=NULL){
		     //fprintf(stderr,"in two_level_iterator!!!!!\n");
		     if(nextIndexEntry->KeyTag[0]==keyBytes[2] && nextIndexEntry->KeyTag[1]==keyBytes[3] && nextIndexEntry->TableNum[0]!=0){
			    //tableNumBytes[0]=nextIndexEntry->TableNum[0];
			    //tableNumBytes[1]=nextIndexEntry->TableNum[1];
			    //tableNumBytes[2]=nextIndexEntry->TableNum[2];
			   // tableNumBytes[3]=nextIndexEntry->TableNum[3];
			    tableNum=bytes3ToInt(nextIndexEntry->TableNum);
			    break;
			}
			nextIndexEntry=nextIndexEntry->nextEntry;
		  }
		} 
	    if(tableNum>0 && tableNum!=(int)TLfile_number){//HashKtoTN[IntKey]!=0 && HashKtoTN[IntKey]!=TLfile_number
		  changeNum++;
	    }
	  // fprintf(stderr,"key:%d,count:%d\n",IntKey,count);
	    index_iter_.Next();
	  }
	  delete []keyBytes;
	  //delete []tableNumBytes;
	  if(count>0 && changeNum==count && atoi(index_iter_.key().ToString().c_str())!=1){
	    //fprintf(stderr,"Don't read this block,key:%d\n",atoi(index_iter_.key().ToString().c_str()));
	 //    cout<<"Don't read this block,key:"<<atoi(index_iter_.key().ToString().c_str())<<endl;
	   //config::reduceIOInComp++;
	   *ReduceIOInComp=*ReduceIOInComp+1;
	    continue;
	  }
     }*/
    InitDataBlock();
    //*TotalIOInComp=*TotalIOInComp+1;
    if (data_iter_.iter() != NULL) {
      data_iter_.SeekToFirst();
    }
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != NULL) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(NULL);
  } else {
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != NULL && handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
       // fprintf(stderr,"In InitDataBlock,before block_function_\n");
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

/*Iterator* NewTwoLevelIterator2(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options,uint64_t file_number,IndexEntry* myHashIndex,long int* myReduceIOInComp) {
  return new TwoLevelIterator(index_iter, block_function, arg, options,file_number,myHashIndex,myReduceIOInComp);
}

Iterator* TwoLevelCountInValidKey( Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options,uint64_t file_number,IndexEntry* myHashIndex,int* countInvalid){
   return new TwoLevelIterator(index_iter, block_function, arg, options,file_number,myHashIndex,countInvalid,true);
}*/

}  // namespace leveldb
