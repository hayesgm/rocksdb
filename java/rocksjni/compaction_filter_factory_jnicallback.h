// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Comparator and rocksdb::DirectComparator.

#ifndef JAVA_ROCKSJNI_COMPACTION_FILTER_FACTORY_JNICALLBACK_H_
#define JAVA_ROCKSJNI_COMPACTION_FILTER_FACTORY_JNICALLBACK_H_

#include <jni.h>
#include <memory>

#include "rocksdb/compaction_filter.h"
#include "rocksjni/jnicallback.h"

namespace rocksdb {

class CompactionFilterFactoryJniCallback : public JniCallback, public CompactionFilterFactory {
 public:
    CompactionFilterFactoryJniCallback(
        JNIEnv* env, jobject jCompactionFilterFactory);
    virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context);
    virtual const char* Name() const;

 private:
    std::unique_ptr<const char[]> m_name;
    jmethodID m_jCreateCompactionFilterMethodId;
};

}  //namespace rocksdb

#endif  // JAVA_ROCKSJNI_COMPACTION_FILTER_FACTORY_JNICALLBACK_H_
