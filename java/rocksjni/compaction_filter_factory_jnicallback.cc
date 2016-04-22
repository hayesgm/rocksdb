// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Comparator.

#include "rocksjni/compaction_filter_factory_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
CompactionFilterFactoryJniCallback::CompactionFilterFactoryJniCallback(
    JNIEnv* env, jobject jCompactionFilterFactory)
    : JniCallback(env, jCompactionFilterFactory) {
  
  // Note: The name of a CompactionFilterFactory will not change during
  // it's lifetime, so we cache it in a global var
  jmethodID jNameMethodId = AbstractCompactionFilterFactoryJni::getNameMethodId(env);
  if(jNameMethodId == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  jstring jsName = (jstring)env->CallObjectMethod(m_jcallback_obj, jNameMethodId);
  if(env->ExceptionCheck()) {
    // exception thrown
    return;
  }
  jboolean has_exception = JNI_FALSE;
  m_name = JniUtil::copyString(env, jsName, &has_exception);  // also releases jsName
  if (has_exception == JNI_TRUE) {
    // exception thrown
    return;
  }

  m_jCreateCompactionFilterMethodId = AbstractCompactionFilterFactoryJni::getCreateCompactionFilterMethodId(env);
  if(m_jCreateCompactionFilterMethodId == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }
}

const char* CompactionFilterFactoryJniCallback::Name() const {
  return m_name.get();
}

std::unique_ptr<CompactionFilter> CompactionFilterFactoryJniCallback::CreateCompactionFilter(
    const CompactionFilter::Context& context) {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jlong addr_compaction_filter = env->CallLongMethod(m_jcallback_obj,
      m_jCreateCompactionFilterMethodId,
      static_cast<jboolean>(context.is_full_compaction),
      static_cast<jboolean>(context.is_manual_compaction));

  if(env->ExceptionCheck()) {
    // exception thrown from CallLongMethod
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return nullptr;
  }

  auto* cff = reinterpret_cast<CompactionFilter*>(addr_compaction_filter);

  releaseJniEnv(attached_thread);

  return std::unique_ptr<CompactionFilter>(cff);
}

}  // namespace rocksdb
