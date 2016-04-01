// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Comparator.

#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
BaseComparatorJniCallback::BaseComparatorJniCallback(
    JNIEnv* env, jobject jComparator,
    const ComparatorJniCallbackOptions* copt)
    : JniCallback(env, jComparator),
    m_mtx_compare(copt->use_adaptive_mutex),
    m_mtx_findShortestSeparator(copt->use_adaptive_mutex),
    m_jSliceA(nullptr), m_jSliceB(nullptr), m_jSliceLimit(nullptr) {

  // Note: The name of a Comparator will not change during it's lifetime,
  // so we cache it in a global var
  jmethodID jNameMethodId = AbstractComparatorJni::getNameMethodId(env);
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
  m_name = JniUtil::copyString(env, jsName,
      &has_exception);  // also releases jsName
  if (has_exception == JNI_TRUE) {
    // exception thrown
    return;
  }

  m_jCompareMethodId = AbstractComparatorJni::getCompareMethodId(env);
  if(m_jCompareMethodId == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  m_jFindShortestSeparatorMethodId =
    AbstractComparatorJni::getFindShortestSeparatorMethodId(env);
  if(m_jFindShortestSeparatorMethodId == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  m_jFindShortSuccessorMethodId =
    AbstractComparatorJni::getFindShortSuccessorMethodId(env);
  if(m_jFindShortSuccessorMethodId == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }
}

const char* BaseComparatorJniCallback::Name() const {
  return m_name.get();
}

int BaseComparatorJniCallback::Compare(const Slice& a, const Slice& b) const {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  MaybeAcquireMutex(&m_mtx_compare);

  jobject jSliceA = GetSliceA(env);
  jobject jSliceB = GetSliceB(env);

  bool pending_exception =
      AbstractSliceJni::setHandle(env, jSliceA, &a, JNI_FALSE);
  if(pending_exception) {
    MaybeReleaseMutex(&m_mtx_compare);
    if(env->ExceptionCheck()) {
      // exception thrown from setHandle or descendant
      env->ExceptionDescribe(); // print out exception to stderr
    }
    releaseJniEnv(attached_thread);
    return 0;
  }

  pending_exception =
      AbstractSliceJni::setHandle(env, jSliceB, &b, JNI_FALSE);
  if(pending_exception) {
    MaybeReleaseMutex(&m_mtx_compare);
    if(env->ExceptionCheck()) {
      // exception thrown from setHandle or descendant
      env->ExceptionDescribe(); // print out exception to stderr
    }
    releaseJniEnv(attached_thread);
    return 0;
  }

  jint result =
    env->CallIntMethod(m_jcallback_obj, m_jCompareMethodId, jSliceA,
      jSliceB);

  MaybeReleaseMutex(&m_mtx_compare);

  if(env->ExceptionCheck()) {
    // exception thrown from CallIntMethod
    env->ExceptionDescribe(); // print out exception to stderr
    result = 0; // we could not get a result from java callback so use 0
  }

  releaseJniEnv(attached_thread);

  return result;
}

void BaseComparatorJniCallback::FindShortestSeparator(
    std::string* start, const Slice& limit) const {
  if (start == nullptr) {
    return;
  }

  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  const char* startUtf = start->c_str();
  jstring jsStart = env->NewStringUTF(startUtf);
  if(jsStart == nullptr) {
    // unable to construct string
    if(env->ExceptionCheck()) {
      env->ExceptionDescribe(); // print out exception to stderr
    }
    releaseJniEnv(attached_thread);
    return;
  }
  if(env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    env->ExceptionDescribe(); // print out exception to stderr
    env->DeleteLocalRef(jsStart);
    releaseJniEnv(attached_thread);
    return;
  }

  MaybeAcquireMutex(&m_mtx_findShortestSeparator);

  jobject jSliceLimit = GetSliceLimit(env);

  bool pending_exception =
      AbstractSliceJni::setHandle(env, jSliceLimit, &limit, JNI_FALSE);
  if(pending_exception) {
    MaybeReleaseMutex(&m_mtx_findShortestSeparator);
    if(env->ExceptionCheck()) {
      // exception thrown from setHandle or descendant
      env->ExceptionDescribe(); // print out exception to stderr
    }
    if(jsStart != nullptr) {
      env->DeleteLocalRef(jsStart);
    }
    releaseJniEnv(attached_thread);
    return;
  }


  jstring jsResultStart =
    (jstring)env->CallObjectMethod(m_jcallback_obj,
      m_jFindShortestSeparatorMethodId, jsStart, jSliceLimit);

  MaybeReleaseMutex(&m_mtx_findShortestSeparator);

  env->DeleteLocalRef(jsStart);

  if(env->ExceptionCheck()) {
    // exception thrown from CallObjectMethod
    env->ExceptionDescribe();  // print out exception to stderr
    env->DeleteLocalRef(jsStart);
    releaseJniEnv(attached_thread);
    return;
  }

  env->DeleteLocalRef(jsStart);

  if (jsResultStart != nullptr) {
    // update start with result
    jboolean has_exception = JNI_FALSE;
    std::unique_ptr<const char[]> result_start = JniUtil::copyString(env, jsResultStart,
        &has_exception);  // also releases jsResultStart
    if (has_exception == JNI_TRUE) {
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();  // print out exception to stderr
      }
      releaseJniEnv(attached_thread);
      return;
    }

    start->assign(result_start.get());
  }
  releaseJniEnv(attached_thread);
}

void BaseComparatorJniCallback::FindShortSuccessor(
    std::string* key) const {
  if (key == nullptr) {
    return;
  }

  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  const char* keyUtf = key->c_str();
  jstring jsKey = env->NewStringUTF(keyUtf);
  if(jsKey == nullptr) {
    // unable to construct string
    if(env->ExceptionCheck()) {
      env->ExceptionDescribe(); // print out exception to stderr
    }
    releaseJniEnv(attached_thread);
    return;
  } else if(env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    env->ExceptionDescribe(); // print out exception to stderr
    env->DeleteLocalRef(jsKey);
    releaseJniEnv(attached_thread);
    return;
  }

  jstring jsResultKey =
    (jstring)env->CallObjectMethod(m_jcallback_obj,
      m_jFindShortSuccessorMethodId, jsKey);

  if(env->ExceptionCheck()) {
    // exception thrown from CallObjectMethod
    env->ExceptionDescribe(); // print out exception to stderr
    env->DeleteLocalRef(jsKey);
    releaseJniEnv(attached_thread);
    return;
  }

  env->DeleteLocalRef(jsKey);

  if (jsResultKey != nullptr) {
    // updates key with result, also releases jsResultKey.
    jboolean has_exception = JNI_FALSE;
    std::unique_ptr<const char[]> result_key = JniUtil::copyString(env, jsResultKey,
        &has_exception);    // also releases jsResultKey
    if (has_exception == JNI_TRUE) {
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();  // print out exception to stderr
      }
      releaseJniEnv(attached_thread);
      return;
    }

    key->assign(result_key.get());
  }

  releaseJniEnv(attached_thread);
}

void BaseComparatorJniCallback::MaybeAcquireMutex(InstrumentedMutex* mtx) const {
#if (!thread_local_supported)
  mtx->Lock();
#endif
}

void BaseComparatorJniCallback::MaybeReleaseMutex(InstrumentedMutex* mtx) const {
#if (!thread_local_supported)
  mtx->Unlock();
#endif
}

jobject BaseComparatorJniCallback::GetSliceA(JNIEnv* env) const {
  return GetSlice(env, m_jSliceA, &m_mtx_compare);
}

jobject BaseComparatorJniCallback::GetSliceB(JNIEnv* env) const {
  return GetSlice(env, m_jSliceB, &m_mtx_compare);
}

jobject BaseComparatorJniCallback::GetSliceLimit(JNIEnv* env) const {
  return GetSlice(env, m_jSliceLimit, &m_mtx_findShortestSeparator);
}

jobject BaseComparatorJniCallback::GetSlice(JNIEnv* env, jobject &maybeReusedSlice, InstrumentedMutex* reusedSliceMutex) const {
#if (thread_local_supported)
  static thread_local JObjectCleanupWrapper sliceWrapper(m_jvm,
      env->NewGlobalRef(CreateSlice(env)));
  return *sliceWrapper.get();
#else
   reusedSliceMutex->AssertHeld();
   if (maybeReusedSlice == nullptr) {
     maybeReusedSlice = env->NewGlobalRef(CreateSlice(env));
   }
  return maybeReusedSlice;
#endif
}

ComparatorJniCallback::ComparatorJniCallback(
    JNIEnv* env, jobject jComparator,
    const ComparatorJniCallbackOptions* copt) :
    BaseComparatorJniCallback(env, jComparator, copt) {}

jobject ComparatorJniCallback::CreateSlice(JNIEnv* env) const {
  return SliceJni::construct0(env);
}

ComparatorJniCallback::~ComparatorJniCallback() {
#if (!thread_local_supported)
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  if(m_jSliceA != nullptr) {
    env->DeleteGlobalRef(m_jSliceA);
  }

  if(m_jSliceB != nullptr) {
    env->DeleteGlobalRef(m_jSliceB);
  }

  if(m_jSliceLimit != nullptr) {
    env->DeleteGlobalRef(m_jSliceLimit);
  }

  releaseJniEnv(attached_thread);
#endif
}

DirectComparatorJniCallback::DirectComparatorJniCallback(
    JNIEnv* env, jobject jComparator,
    const ComparatorJniCallbackOptions* copt) :
    BaseComparatorJniCallback(env, jComparator, copt) {}

jobject DirectComparatorJniCallback::CreateSlice(JNIEnv* env) const {
  return DirectSliceJni::construct0(env);
}

DirectComparatorJniCallback::~DirectComparatorJniCallback() {
#if (!thread_local_supported)
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  if(m_jSliceA != nullptr) {
    env->DeleteGlobalRef(m_jSliceA);
  }

  if(m_jSliceB != nullptr) {
    env->DeleteGlobalRef(m_jSliceB);
  }

  if(m_jSliceLimit != nullptr) {
    env->DeleteGlobalRef(m_jSliceLimit);
  }

  releaseJniEnv(attached_thread);
#endif
}
}  // namespace rocksdb
