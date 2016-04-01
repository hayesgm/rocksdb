// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Comparator and rocksdb::DirectComparator.

#ifndef JAVA_ROCKSJNI_COMPARATORJNICALLBACK_H_
#define JAVA_ROCKSJNI_COMPARATORJNICALLBACK_H_

#include <jni.h>
#include <memory>
#include <string>
#include "rocksjni/jnicallback.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "port/port.h"
#include "monitoring/instrumented_mutex.h"

namespace rocksdb {

struct ComparatorJniCallbackOptions {
  // Use adaptive mutex, which spins in the user space before resorting
  // to kernel. This could reduce context switch when the mutex is not
  // heavily contended. However, if the mutex is hot, we could end up
  // wasting spin time.
  // Default: false
  bool use_adaptive_mutex;

  ComparatorJniCallbackOptions() : use_adaptive_mutex(false) {
  }
};

/**
 * This class is used to cleanup the Global
 * reference of a jobject at destruction
 * time
 */
class JObjectCleanupWrapper {
 public:
  JObjectCleanupWrapper(JavaVM* jvm, const jobject&& jobj) : m_jvm(jvm), m_jobj(jobj) {}

  ~JObjectCleanupWrapper() {
    JNIEnv* env = nullptr;
    jint rs __attribute__((unused)) =
        m_jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    assert(rs == JNI_OK);
    env->DeleteGlobalRef(m_jobj);
    m_jvm->DetachCurrentThread();
  }

  jobject* get() {
    return &m_jobj;
  }

 private:
  JavaVM* m_jvm;
  jobject m_jobj;
};

  
/**
 * This class acts as a bridge between C++
 * and Java. The methods in this class will be
 * called back from the RocksDB storage engine (C++)
 * we then callback to the appropriate Java method
 * this enables Comparators to be implemented in Java.
 *
 * The design of this Comparator caches the Java Slice
 * objects that are used in the compare and findShortestSeparator
 * method callbacks. Instead of creating new objects for each callback
 * of those functions, by reuse via setHandle we are a lot
 * faster; We use C++ 11 thread_local to achieve this caching where
 * supported, unfortunately for the compilers that don't support this,
 * we fall back to independent locking in regions of each of those methods
 * via the mutexs m_mtx_compare and m_mtx_findShortestSeparator
 * respectively.
 */
class BaseComparatorJniCallback : public JniCallback, public Comparator {
 public:
    BaseComparatorJniCallback(
      JNIEnv* env, jobject jComparator,
      const ComparatorJniCallbackOptions* copt);
    virtual const char* Name() const;
    virtual int Compare(const Slice& a, const Slice& b) const;
    virtual void FindShortestSeparator(
      std::string* start, const Slice& limit) const;
    virtual void FindShortSuccessor(std::string* key) const;

 private:
    // used for synchronisation in compare method
    mutable InstrumentedMutex m_mtx_compare;
    // used for synchronisation in findShortestSeparator method
    mutable InstrumentedMutex m_mtx_findShortestSeparator;
    std::unique_ptr<const char[]> m_name;
    jmethodID m_jCompareMethodId;
    jmethodID m_jFindShortestSeparatorMethodId;
    jmethodID m_jFindShortSuccessorMethodId;
    jobject GetSliceA(JNIEnv* env) const;
    jobject GetSliceB(JNIEnv* env) const;
    jobject GetSliceLimit(JNIEnv* env) const;
    jobject GetSlice(JNIEnv* env, jobject &maybeReusedSlice,
        InstrumentedMutex* reusedSliceMutex) const;
    void MaybeAcquireMutex(InstrumentedMutex* mtx) const;
    void MaybeReleaseMutex(InstrumentedMutex* mtx) const;

 protected:
    virtual jobject CreateSlice(JNIEnv* env) const = 0;
    mutable jobject m_jSliceA;
    mutable jobject m_jSliceB;
    mutable jobject m_jSliceLimit;
};

class ComparatorJniCallback : public BaseComparatorJniCallback {
 public:
      ComparatorJniCallback(
        JNIEnv* env, jobject jComparator,
        const ComparatorJniCallbackOptions* copt);
      ~ComparatorJniCallback();

  protected:
      virtual jobject CreateSlice(JNIEnv* env) const override;
};

class DirectComparatorJniCallback : public BaseComparatorJniCallback {
 public:
      DirectComparatorJniCallback(
        JNIEnv* env, jobject jComparator,
        const ComparatorJniCallbackOptions* copt);
      ~DirectComparatorJniCallback();

  protected:
      virtual jobject CreateSlice(JNIEnv* env) const override;
};
}  // namespace rocksdb

#endif  // JAVA_ROCKSJNI_COMPARATORJNICALLBACK_H_
