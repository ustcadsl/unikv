// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "db/dbformat.h"
#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

//int to byte
void  intTo4Byte(unsigned int i,byte *bytes)
{
    memset(bytes,0,sizeof(byte) * 4);
    bytes[0] = (byte) (0xff & i);
    bytes[1] = (byte) ((0xff00 & i) >> 8);
    bytes[2] = (byte) ((0xff0000 & i) >> 16);
    bytes[3] = (byte) ((0xff000000 & i) >> 24);
 }
 
 void  intTo3Byte(unsigned int i,byte *bytes)
{
    memset(bytes,0,sizeof(byte) * 3);
    bytes[0] = (byte) (0xff & i);
    bytes[1] = (byte) ((0xff00 & i) >> 8);
    bytes[2] = (byte) ((0xff0000 & i) >> 16);
 }
 
void  intTo2Byte(unsigned int i,byte *bytes)
{
    memset(bytes,0,sizeof(byte) * 2);
    bytes[0] = (byte) (0xff & i);
    bytes[1] = (byte) ((0xff00 & i) >> 8);
 }
 
 void  intToByte(unsigned int i,byte *bytes)
{
    memset(bytes,0,sizeof(byte) * 1);
    bytes[0] = (byte) (0xff & i);
 }

//byte to int
 unsigned int bytesToInt(byte* bytes) 
{
    unsigned int addr = bytes[0];
    return addr;
 }
 
 unsigned int bytes2ToInt(byte* bytes) 
{
    unsigned int addr = bytes[0];
    addr |= (bytes[1] << 8);
    return addr;
 }
 
unsigned int bytes3ToInt(byte* bytes) 
{
    unsigned int addr = bytes[0];
    addr |= (bytes[1] << 8);
    addr |= (bytes[2] << 16);
    return addr;
 }
 
 unsigned int bytes4ToInt(byte* bytes) 
{
    unsigned int addr = bytes[0];
    addr |= (bytes[1] << 8);
    addr |= (bytes[2] << 16);
    addr |= (bytes[3] << 24);
    return addr;
 }
 
 int myStrlen(unsigned char* str){
  int nLen=0;
  unsigned char* p=str;
  while(*p!=0){
    nLen++;
    p++;
  }
  return nLen;
}
  
static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

std::string ParsedInternalKey::DebugString() const {
  char buf[50];
  snprintf(buf, sizeof(buf), "' @ %llu : %d",
           (unsigned long long) sequence,
           int(type));
  std::string result = "'";
  result += EscapeString(user_key.ToString());
  result += buf;
  return result;
}

std::string InternalKey::DebugString() const {
  std::string result;
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    result = parsed.DebugString();
  } else {
    result = "(bad)";
    result.append(EscapeString(rep_));
  }
  return result;
}

const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator";
}

int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

void InternalKeyComparator::FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

const char* InternalFilterPolicy::Name() const {
  return user_policy_->Name();
}

void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]);
    // TODO(sanjay): Suppress dups?
  }
  user_policy_->CreateFilter(keys, n, dst);
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

LookupKey::LookupKey(const Slice& ukey, SequenceNumber s)
  : start_(),
    kstart_(),
    end_() {
  size_t usize = ukey.size();
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  dst = EncodeVarint32(dst, usize + 8);
  kstart_ = dst;
  memcpy(dst, ukey.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

}  // namespace leveldb
