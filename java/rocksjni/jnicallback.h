// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the callback "bridge" between Java and C++ for
// JNI Callbacks from C++ to sub-classes or org.rocksdb.RocksCallbackObject

#ifndef JAVA_ROCKSJNI_JNICALLBACK_H_
#define JAVA_ROCKSJNI_JNICALLBACK_H_

#include <jni.h>

namespace rocksdb {
  class JniCallback {
   public:
    JniCallback(JNIEnv* env, jobject jcallback_obj);
    virtual ~JniCallback();

   protected:
    JavaVM* m_jvm;
    jobject m_jcallback_obj;
    JNIEnv* getJniEnv() const;
    void releaseJniEnv() const;
  };
}

#endif  // JAVA_ROCKSJNI_JNICALLBACK_H_