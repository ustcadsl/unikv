// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/version_set.h"
#include "util/coding.h"

namespace leveldb {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
  kComparator           = 1,
  kLogNumber            = 2,
  kNextFileNumber       = 3,
  kLastSequence         = 4,
  kCompactPointer       = 5,
  kDeletedFile          = 6,
  kNewFile              = 7,
  // 8 was used for large value refs
  kPrevLogNumber        = 9,
   kPartitionInfo	=10,
  kReplayNumber	=11
};

void VersionEdit::Clear() {
  comparator_.clear();
  //log_number_ = 0;
  //prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  //deleted_files_.clear();
  //new_files_.clear();
  for(int k=0;k<config::kNumPartition;k++){
    //prev_log_number_[k] = 0;
    //log_number_[k] = 0;
    //next_file_number_[k]= 0;
     prev_log_number_[k] = 0;
    //next_file_number_[k] = 0;
     log_number_[k] = 0;
     deleted_files_[k].clear();
     new_files_[k].clear();
  }
}

void VersionEdit::EncodeTo(std::string* dst) const {
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);
    //PutVarint64(dst, log_number_);
    for(int k=0;k<config::kNumPartition;k++){////
	    PutVarint64(dst, log_number_[k]);
    }
  }
  if (has_prev_log_number_) {
    PutVarint32(dst, kPrevLogNumber);
    //PutVarint64(dst, prev_log_number_);
     for(int k=0;k<config::kNumPartition;k++){////
	      PutVarint64(dst, prev_log_number_[k]);
    }
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }

   for(int k=0;k<config::kNumPartition;k++){////
    for (size_t i = 0; i < compact_pointers_[k].size(); i++) {
      PutVarint32(dst, kCompactPointer);
      std::pair<int, InternalKey> cPointers=compact_pointers_[k][i];
      PutVarint32(dst,cPointers.first);  // level
      PutLengthPrefixedSlice(dst, cPointers.second.Encode());
    }
  }
  for(int k=0;k<config::kNumPartition;k++){
    for (DeletedFileSet::const_iterator iter = deleted_files_[k].begin();
	iter != deleted_files_[k].end();
	++iter) {
       //const FileMetaData& f = iter->second;
      PutVarint32(dst, kDeletedFile);
      PutVarint32(dst, iter->first);   // level
      PutVarint32(dst, k);   // partition
      PutVarint64(dst, iter->second);  // file number
    }
  }
  for(int k=0;k<config::kNumPartition;k++){
    for (size_t i = 0; i < new_files_[k].size(); i++) {
      const FileMetaData& f = new_files_[k][i].second;
      PutVarint32(dst, kNewFile);
      PutVarint32(dst, new_files_[k][i].first);  // level
      PutVarint32(dst, f.partition);////
      PutVarint64(dst, f.number);
      PutVarint64(dst, f.file_size);
      PutLengthPrefixedSlice(dst, f.smallest.Encode());
      PutLengthPrefixedSlice(dst, f.largest.Encode());
    }
  }
  
  for(int k=0;k<partition_InfoVec.size();k++){
    printf("size:%d,key:%s,ID:%d,nextID:%d\n",(int)partition_InfoVec.size(),partition_InfoVec[k].smallestCharKey,partition_InfoVec[k].partitionID,partition_InfoVec[k].nextID);
      PutVarint32(dst, kPartitionInfo);
      PutLengthPrefixedSlice(dst, (Slice)partition_InfoVec[k].smallestCharKey);
      PutVarint32(dst, partition_InfoVec[k].partitionID);
      PutVarint32(dst, partition_InfoVec[k].nextID);
  }
  
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return true;
  } else {
    return false;
  }
}

static bool GetLevel(Slice* input, int* level) {
  uint32_t v;
  if (GetVarint32(input, &v) &&
      v < config::kNumLevels+config::kTempLevel) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

Status VersionEdit::DecodeFrom(const Slice& src) {
  Clear();
  Slice input = src;
  const char* msg = NULL;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  uint64_t number;
  FileMetaData f;
  struct partitionInfo curPartitionInfo;
  Slice str, chrKey;
  InternalKey key;

  while (msg == NULL && GetVarint32(&input, &tag)) {
    switch (tag) {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        for(int k=0;k<config::kNumPartition;k++){////
            if (GetVarint64(&input, &log_number_[k])) {
              has_log_number_ = true;
            } else {
              msg = "log number";
            }
        }
        break;

      case kPrevLogNumber:
        for(int k=0;k<config::kNumPartition;k++){////
              if (GetVarint64(&input, &prev_log_number_[k])) {
                has_prev_log_number_ = true;
              } else {
                msg = "previous log number";
              }
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactPointer:
        if (GetLevel(&input, &level) &&
            GetInternalKey(&input, &key)) {
          int kn=atoi(key.user_key().ToString().c_str());
          int partition=0;
          compact_pointers_[partition].push_back(std::make_pair(level, key));
        } else {
          msg = "compaction pointer";
        }
        break;

      case kDeletedFile:{
	      uint32_t partition=0;
        if (GetLevel(&input, &level) &&
	    GetVarint32(&input, &partition) &&
            GetVarint64(&input, &number)) {
	          deleted_files_[partition].insert(std::make_pair(level, number));
        } else {
          msg = "deleted file";
        }
        break;
      }
      case kNewFile:{
        if (GetLevel(&input, &level) &&
	          GetVarint32(&input, &f.partition)&& ////
            GetVarint64(&input, &f.number) &&
            GetVarint64(&input, &f.file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          new_files_[(int)f.partition].push_back(std::make_pair(level, f));
        } else {
          msg = "new-file entry";
        }
        break;
      }
      case kPartitionInfo:
        if(GetLengthPrefixedSlice(&input, &chrKey) &&
          GetVarint32(&input, &curPartitionInfo.partitionID)&&
          GetVarint32(&input, &curPartitionInfo.nextID)){
          strcpy(curPartitionInfo.smallestCharKey,chrKey.ToString().c_str());
          printf(" partitionID:%d,key:%s,fileID:%d,nextID:%d\n",curPartitionInfo.partitionID,curPartitionInfo.smallestCharKey,curPartitionInfo.partitionID,curPartitionInfo.nextID);
          partition_InfoVec.push_back(curPartitionInfo);
          }
        break;
      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == NULL && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != NULL) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString() const {
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  
for(int k=0;k<config::kNumPartition;k++){
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_[k]);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_[k]);
  }
}

  if (has_next_file_number_) {
    r.append("\n  NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }

  for(int k=0;k<config::kNumPartition;k++){
    for (size_t i = 0; i < compact_pointers_[k].size(); i++) {
      r.append("\n  CompactPointer: ");
      AppendNumberTo(&r, compact_pointers_[k][i].first);
      r.append(" ");
      r.append(compact_pointers_[k][i].second.DebugString());
    }
 
    for (DeletedFileSet::const_iterator iter = deleted_files_[k].begin();
	iter != deleted_files_[k].end();
	++iter) {
      r.append("\n  DeleteFile: ");
      AppendNumberTo(&r, iter->first);
      r.append(" ");
      AppendNumberTo(&r, iter->second);
    }
  }
  for(int k=0;k<config::kNumPartition;k++){
    for (size_t i = 0; i < new_files_[k].size(); i++) {
      const FileMetaData& f = new_files_[k][i].second;
      r.append("\n  AddFile: ");
      AppendNumberTo(&r, new_files_[k][i].first);
      r.append(" ");
      AppendNumberTo(&r, f.number);
      r.append(" ");
      AppendNumberTo(&r, f.file_size);
      r.append(" ");
      r.append(f.smallest.DebugString());
      r.append(" .. ");
      r.append(f.largest.DebugString());
    }
  }
  r.append("\n}\n");
  return r;
}

}  // namespace leveldb
