// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "unikv/db.h"
#include "unikv/env.h"
#include "unikv/table_builder.h"
#include "unikv/iterator.h"
#include "db/hashfunc.h"

namespace leveldb {

Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();
  std::string fname = TableFileName(dbname, meta->number);//
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }
    TableBuilder* builder = new TableBuilder(options, file,false,0);
    meta->smallest.DecodeFrom(iter->key());//smallest key in SSTable
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      builder->Add(key, iter->value());//add key-value to SSTable file
    }
    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();//
      assert(meta->file_size > 0);
    }
    delete builder;
    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;
    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size,false,false);
      s = it->status();
      delete it;
    }
  }
  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }
  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    //fprintf(stderr,"s.ok():%d, meta->file_size:%d,fname:%s\n",s.ok(), meta->file_size,fname.c_str());
    //env->DeleteFile(fname);
  }
  return s;
}

/*Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();
  std::string fname = TableFileName(dbname, meta->number);//
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }
    TableBuilder* builder = new TableBuilder(options, file,false,0);
    meta->smallest.DecodeFrom(iter->key());///////////////////////smallest key in SSTable
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      //meta->tableKeys[count++]=key;
      meta->largest.DecodeFrom(key);
      builder->Add(key, iter->value());//add key-value to SSTable file//
      //meta->tableKeys[keyNum++]=atoi(key.ToString().c_str());              
      //fprintf(stderr,"meta.number:%d,tableNumBytes:%d\n",(int)meta.number,bytes2ToInt(tableNumBytes));   
    }
    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();//
      assert(meta->file_size > 0);
    }
    delete builder;
    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;
    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size,false,false);
      s = it->status();
      delete it;
    }
  }
  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }
  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}*/

}  // namespace leveldb
