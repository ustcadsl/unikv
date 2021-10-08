// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
#define STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_

#include <string>
#include "unikv/status.h"
#include "unikv/db.h"
#include <queue>
#include "db/b_tree.h"

namespace leveldb {

class Slice;

class WriteBatch {
 public:
  WriteBatch();
  ~WriteBatch();

  static void* recordToBatch(void *paraData);

  // Store the mapping "key->value" in the database.
  void Put(const Slice& key, const Slice& value);

  void putPartitionNumber(int partition);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  void Delete(const Slice& key);

  // Clear all updates buffered in this batch.
  void Clear();

  // Support for iterating over the contents of a batch.
  class Handler {
   public:
    virtual ~Handler();
    virtual void Put(const Slice& key, const Slice& value) = 0;
    virtual void PutWithPartition(const Slice& key, const Slice& value, uint32_t partition) = 0;
    virtual void Delete(const Slice& key) = 0;
    virtual void DeleteWithPartition(const Slice& key, uint32_t partition) = 0;
  };
  Status Iterate(Handler* handler) const;
  
  Status IterateRecovery(Handler* handler, int newPartition) const;
  
  Status GetKeyValue(Slice *key, Slice *value) const;

  Status GetKeyValueInQueue(WriteBatch* updates, TreeNode* treeRoot) const;

  //WriteOptions options;

 private:
  friend class WriteBatchInternal;

  std::string rep_;  // See comment in write_batch.cc for the format of rep_

  // Intentionally copyable
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
