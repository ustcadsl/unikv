// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#define __STDC_LIMIT_MACROS

#include "unikv/write_batch.h"

#include "unikv/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch()
  : rep_() {
  Clear();
}

WriteBatch::~WriteBatch() { }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}


//////////////////////////////////////////////////////
Status WriteBatch::GetKeyValue(Slice *key, Slice *value) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }
  input.remove_prefix(kHeader);
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];//
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input,key)
	         && GetLengthPrefixedSlice(&input,value)){
          //handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}
/////////////////////////////////////////////////////////////////

Status WriteBatch::GetKeyValueInQueue(WriteBatch* updates, TreeNode* treeRoot) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }
  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];//
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input,&key)
	         && GetLengthPrefixedSlice(&input,&value)){
          int partition= treeRoot->binaryTreeSereach((char*)key.ToString().c_str());
          updates->Put(key.ToString(), value.ToString());
          updates->putPartitionNumber(partition);
         
          
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }

  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }
  input.remove_prefix(kHeader);
  Slice key, value;
  uint32_t partition;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value) && GetVarint32(&input, &partition)) {
          //handler->Put(key, value);
          handler->PutWithPartition(key, value, partition);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key) && GetVarint32(&input, &partition)) {
          //handler->Delete(key);
          handler->DeleteWithPartition(key, partition);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

Status WriteBatch::IterateRecovery(Handler* handler, int newPartition) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  uint32_t partition;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value) && GetVarint32(&input, &partition)) {//
          if(newPartition!=(int)partition){
            handler->PutWithPartition(key, value, newPartition);
          }else{
            handler->PutWithPartition(key, value, partition);
          }
          
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key) && GetVarint32(&input, &partition)) {//
          //handler->Delete(key);
          handler->DeleteWithPartition(key, partition);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::putPartitionNumber(int partition){
  PutVarint32(&rep_, partition);
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  MemTableInserter()
    : sequence_(),
      mem_() {
  }
  SequenceNumber sequence_;
  //MemTable* mem_;
  MemTable** mem_;

  virtual void Put(const Slice& key, const Slice& value) {
    mem_[0]->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  virtual void PutWithPartition(const Slice& key, const Slice& value, uint32_t partition) {
    mem_[partition]->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }

  virtual void Delete(const Slice& key) {
    mem_[0]->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
  virtual void DeleteWithPartition(const Slice& key, uint32_t partition) {
    mem_[partition]->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }

 private:
  MemTableInserter(const MemTableInserter&);
  MemTableInserter& operator = (const MemTableInserter&);
};
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b,
                                      MemTable** memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

Status WriteBatchInternal::InsertIntoRecovery(const WriteBatch* b,
                                      MemTable** memtable, int partition) {//Insert MemTable
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->IterateRecovery(&inserter, partition);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
