// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "unikv/options.h"

#include "unikv/comparator.h"
#include "unikv/env.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
      //write_buffer_size(4<<20),
      write_buffer_size(67108864),
      max_open_files(10000),////1000//30000
      block_cache(NULL),
      block_size(4096),
      block_restart_interval(16),
      max_file_size(2<<20),
      compression(kSnappyCompression),
      reuse_logs(false),
      bloom_bits(0),
      filter_policy(NULL) {
}

}  // namespace leveldb
