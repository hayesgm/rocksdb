//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/write_batch_with_index.h"

#include <memory>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "memory/arena.h"
#include "memtable/skiplist.h"
#include "options/db_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "util/cast_util.h"
#include "util/string_util.h"
#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

namespace ROCKSDB_NAMESPACE {

// when direction == forward
// * current_at_base_ <=> base_iterator > delta_iterator
// when direction == backwards
// * current_at_base_ <=> base_iterator < delta_iterator
// always:
// * equal_keys_ <=> base_iterator == delta_iterator
class BaseDeltaIterator : public Iterator {
 public:
  BaseDeltaIterator(DB* db, ColumnFamilyHandle* column_family,
                    Iterator* base_iterator, WBWIIteratorImpl* delta_iterator,
                    const Comparator* comparator,
                    const ReadOptions* read_options = nullptr)
      : wbwii_(db, column_family),
        forward_(true),
        current_at_base_(true),
        equal_keys_(false),
        status_(Status::OK()),
        base_iterator_(base_iterator),
        delta_iterator_(delta_iterator),
        comparator_(comparator),
        iterate_upper_bound_(read_options ? read_options->iterate_upper_bound
                                          : nullptr) {}

  ~BaseDeltaIterator() override {}

  bool Valid() const override {
    return current_at_base_ ? BaseValid() : DeltaValid();
  }

  void SeekToFirst() override {
    forward_ = true;
    base_iterator_->SeekToFirst();
    delta_iterator_->SeekToFirst();
    UpdateCurrent();
  }

  void SeekToLast() override {
    forward_ = false;
    base_iterator_->SeekToLast();
    delta_iterator_->SeekToLast();
    UpdateCurrent();
  }

  void Seek(const Slice& k) override {
    forward_ = true;
    base_iterator_->Seek(k);
    delta_iterator_->Seek(k);
    UpdateCurrent();
  }

  void SeekForPrev(const Slice& k) override {
    forward_ = false;
    base_iterator_->SeekForPrev(k);
    delta_iterator_->SeekForPrev(k);
    UpdateCurrent();
  }

  void Next() override {
    if (!Valid()) {
      status_ = Status::NotSupported("Next() on invalid iterator");
      return;
    }

    if (!forward_) {
      // Need to change direction
      // if our direction was backward and we're not equal, we have two states:
      // * both iterators are valid: we're already in a good state (current
      // shows to smaller)
      // * only one iterator is valid: we need to advance that iterator
      forward_ = true;
      equal_keys_ = false;
      if (!BaseValid()) {
        assert(DeltaValid());
        base_iterator_->SeekToFirst();
      } else if (!DeltaValid()) {
        delta_iterator_->SeekToFirst();
      } else if (current_at_base_) {
        // Change delta from larger than base to smaller
        AdvanceDelta();
      } else {
        // Change base from larger than delta to smaller
        AdvanceBase();
      }
      if (DeltaValid() && BaseValid()) {
        if (comparator_->Equal(delta_iterator_->Entry().key,
                               base_iterator_->key())) {
          equal_keys_ = true;
        }
      }
    }
    Advance();
  }

  void Prev() override {
    if (!Valid()) {
      status_ = Status::NotSupported("Prev() on invalid iterator");
      return;
    }

    if (forward_) {
      // Need to change direction
      // if our direction was backward and we're not equal, we have two states:
      // * both iterators are valid: we're already in a good state (current
      // shows to smaller)
      // * only one iterator is valid: we need to advance that iterator
      forward_ = false;
      equal_keys_ = false;
      if (!BaseValid()) {
        assert(DeltaValid());
        base_iterator_->SeekToLast();
      } else if (!DeltaValid()) {
        delta_iterator_->SeekToLast();
      } else if (current_at_base_) {
        // Change delta from less advanced than base to more advanced
        AdvanceDelta();
      } else {
        // Change base from less advanced than delta to more advanced
        AdvanceBase();
      }
      if (DeltaValid() && BaseValid()) {
        if (comparator_->Equal(delta_iterator_->Entry().key,
                               base_iterator_->key())) {
          equal_keys_ = true;
        }
      }
    }

    Advance();
  }

  Slice key() const override {
    return current_at_base_ ? base_iterator_->key()
                            : delta_iterator_->Entry().key;
  }

  Slice value() const override {
    if (current_at_base_) {
      return base_iterator_->value();
    } else {
      WriteEntry delta_entry = delta_iterator_->Entry();
      if (wbwii_.GetNumOperands() == 0) {
        return delta_entry.value;
      } else if (wbwii_.GetNumOperands() == 1) {
        // Special case one operand if we can avoid a merge
        if (delta_entry.type == kDeleteRecord ||
            delta_entry.type == kSingleDeleteRecord) {
          // A delete with a single merge is the merge result
          return wbwii_.GetOperand(0);
        } else if (delta_entry.type == kMergeRecord && !equal_keys_) {
          // A single merge against an empty origin is the merge
          return wbwii_.GetOperand(0);
        }
      }
      if (delta_entry.type == kDeleteRecord ||
          delta_entry.type == kSingleDeleteRecord) {
        status_ =
            wbwii_.MergeKey(delta_entry.key, nullptr, merge_result_.GetSelf());
      } else if (delta_entry.type == kPutRecord) {
        status_ = wbwii_.MergeKey(delta_entry.key, &delta_entry.value,
                                  merge_result_.GetSelf());
      } else if (delta_entry.type == kMergeRecord) {
        if (equal_keys_) {
          Slice base_value = base_iterator_->value();
          status_ = wbwii_.MergeKey(delta_entry.key, &base_value,
                                    merge_result_.GetSelf());
        } else {
          status_ = wbwii_.MergeKey(delta_entry.key, nullptr,
                                    merge_result_.GetSelf());
        }
      }
      merge_result_.PinSelf();
      return merge_result_;
    }
  }

  Status status() const override {
    if (!status_.ok()) {
      return status_;
    }
    if (!base_iterator_->status().ok()) {
      return base_iterator_->status();
    }
    return delta_iterator_->status();
  }

 private:
  void AssertInvariants() {
#ifndef NDEBUG
    bool not_ok = false;
    if (!base_iterator_->status().ok()) {
      assert(!base_iterator_->Valid());
      not_ok = true;
    }
    if (!delta_iterator_->status().ok()) {
      assert(!delta_iterator_->Valid());
      not_ok = true;
    }
    if (not_ok) {
      assert(!Valid());
      assert(!status().ok());
      return;
    }

    if (!Valid()) {
      return;
    }
    if (!BaseValid()) {
      assert(!current_at_base_ && delta_iterator_->Valid());
      return;
    }
    if (!DeltaValid()) {
      assert(current_at_base_ && base_iterator_->Valid());
      return;
    }
    // we don't support those yet
    assert(delta_iterator_->Entry().type != kMergeRecord &&
           delta_iterator_->Entry().type != kLogDataRecord);
    int compare = comparator_->Compare(delta_iterator_->Entry().key,
                                       base_iterator_->key());
    if (forward_) {
      // current_at_base -> compare < 0
      assert(!current_at_base_ || compare < 0);
      // !current_at_base -> compare <= 0
      assert(current_at_base_ && compare >= 0);
    } else {
      // current_at_base -> compare > 0
      assert(!current_at_base_ || compare > 0);
      // !current_at_base -> compare <= 0
      assert(current_at_base_ && compare <= 0);
    }
    // equal_keys_ <=> compare == 0
    assert((equal_keys_ || compare != 0) && (!equal_keys_ || compare == 0));
#endif
  }

  void Advance() {
    if (equal_keys_) {
      assert(BaseValid() && DeltaValid());
      AdvanceBase();
      AdvanceDelta();
    } else {
      if (current_at_base_) {
        assert(BaseValid());
        AdvanceBase();
      } else {
        assert(DeltaValid());
        AdvanceDelta();
      }
    }
    UpdateCurrent();
  }

  void AdvanceDelta() {
    if (forward_) {
      delta_iterator_->NextKey();
    } else {
      delta_iterator_->PrevKey();
    }
  }
  void AdvanceBase() {
    if (forward_) {
      base_iterator_->Next();
    } else {
      base_iterator_->Prev();
    }
  }
  bool BaseValid() const { return base_iterator_->Valid(); }
  bool DeltaValid() const { return delta_iterator_->Valid(); }
  void UpdateCurrent() {
// Suppress false positive clang analyzer warnings.
#ifndef __clang_analyzer__
    status_ = Status::OK();
    while (true) {
      auto delta_result = WBWIIteratorImpl::kNotFound;
      WriteEntry delta_entry;
      if (DeltaValid()) {
        assert(delta_iterator_->status().ok());
        delta_result =
            delta_iterator_->FindLatestUpdate(wbwii_.GetMergeContext());
        delta_entry = delta_iterator_->Entry();
      } else if (!delta_iterator_->status().ok()) {
        // Expose the error status and stop.
        current_at_base_ = false;
        return;
      }
      equal_keys_ = false;
      if (!BaseValid()) {
        if (!base_iterator_->status().ok()) {
          // Expose the error status and stop.
          current_at_base_ = true;
          return;
        }

        // Base has finished.
        if (!DeltaValid()) {
          // Finished
          return;
        }
        if (iterate_upper_bound_) {
          if (comparator_->Compare(delta_entry.key, *iterate_upper_bound_) >=
              0) {
            // out of upper bound -> finished.
            return;
          }
        }
        if (delta_result == WBWIIteratorImpl::kDeleted &&
            wbwii_.GetNumOperands() == 0) {
          AdvanceDelta();
        } else {
          current_at_base_ = false;
          return;
        }
      } else if (!DeltaValid()) {
        // Delta has finished.
        current_at_base_ = true;
        return;
      } else {
        int compare =
            (forward_ ? 1 : -1) *
            comparator_->Compare(delta_entry.key, base_iterator_->key());
        if (compare <= 0) {  // delta bigger or equal
          if (compare == 0) {
            equal_keys_ = true;
          }
          if (delta_result != WBWIIteratorImpl::kDeleted ||
              wbwii_.GetNumOperands() > 0) {
            current_at_base_ = false;
            return;
          }
          // Delta is less advanced and is delete.
          AdvanceDelta();
          if (equal_keys_) {
            AdvanceBase();
          }
        } else {
          current_at_base_ = true;
          return;
        }
      }
    }

    AssertInvariants();
#endif  // __clang_analyzer__
  }

  WriteBatchWithIndexInternal wbwii_;
  bool forward_;
  bool current_at_base_;
  bool equal_keys_;
  mutable Status status_;
  std::unique_ptr<Iterator> base_iterator_;
  std::unique_ptr<WBWIIteratorImpl> delta_iterator_;
  const Comparator* comparator_;  // not owned
  const Slice* iterate_upper_bound_;
  mutable PinnableSlice merge_result_;
};

struct WriteBatchWithIndex::Rep {
  explicit Rep(const Comparator* index_comparator, size_t reserved_bytes = 0,
               size_t max_bytes = 0, bool _overwrite_key = false)
      : write_batch(reserved_bytes, max_bytes),
        comparator(index_comparator, &write_batch),
        skip_list(comparator, &arena),
        overwrite_key(_overwrite_key),
        last_entry_offset(0),
        last_sub_batch_offset(0),
        sub_batch_cnt(1) {}
  ReadableWriteBatch write_batch;
  WriteBatchEntryComparator comparator;
  Arena arena;
  WriteBatchEntrySkipList skip_list;
  bool overwrite_key;
  size_t last_entry_offset;
  // The starting offset of the last sub-batch. A sub-batch starts right before
  // inserting a key that is a duplicate of a key in the last sub-batch. Zero,
  // the default, means that no duplicate key is detected so far.
  size_t last_sub_batch_offset;
  // Total number of sub-batches in the write batch. Default is 1.
  size_t sub_batch_cnt;

  // Remember current offset of internal write batch, which is used as
  // the starting offset of the next record.
  void SetLastEntryOffset() { last_entry_offset = write_batch.GetDataSize(); }

  // In overwrite mode, find the existing entry for the same key and update it
  // to point to the current entry.
  // Return true if the key is found and updated.
  bool UpdateExistingEntry(ColumnFamilyHandle* column_family, const Slice& key,
                           WriteType type);
  bool UpdateExistingEntryWithCfId(uint32_t column_family_id, const Slice& key,
                                   WriteType type);

  // Add the recent entry to the update.
  // In overwrite mode, if key already exists in the index, update it.
  void AddOrUpdateIndex(ColumnFamilyHandle* column_family, const Slice& key,
                        WriteType type);
  void AddOrUpdateIndex(const Slice& key, WriteType type);

  // Allocate an index entry pointing to the last entry in the write batch and
  // put it to skip list.
  void AddNewEntry(uint32_t column_family_id);

  // Clear all updates buffered in this batch.
  void Clear();
  void ClearIndex();

  // Rebuild index by reading all records from the batch.
  // Returns non-ok status on corruption.
  Status ReBuildIndex();
};

bool WriteBatchWithIndex::Rep::UpdateExistingEntry(
    ColumnFamilyHandle* column_family, const Slice& key, WriteType type) {
  uint32_t cf_id = GetColumnFamilyID(column_family);
  return UpdateExistingEntryWithCfId(cf_id, key, type);
}

bool WriteBatchWithIndex::Rep::UpdateExistingEntryWithCfId(
    uint32_t column_family_id, const Slice& key, WriteType type) {
  if (!overwrite_key) {
    return false;
  }

  WBWIIteratorImpl iter(column_family_id, &skip_list, &write_batch,
                        &comparator);
  iter.Seek(key);
  if (!iter.Valid()) {
    return false;
  } else if (!iter.MatchesKey(column_family_id, key)) {
    return false;
  } else {
    // Move to the end of this key (NextKey-Prev)
    iter.NextKey();  // Move to the next key
    if (iter.Valid()) {
      iter.Prev();  // Move back one entry
    } else {
      iter.SeekToLast();
    }
  }

  WriteBatchIndexEntry* non_const_entry =
      const_cast<WriteBatchIndexEntry*>(iter.GetRawEntry());
  if (LIKELY(last_sub_batch_offset <= non_const_entry->offset)) {
    last_sub_batch_offset = last_entry_offset;
    sub_batch_cnt++;
  }
  if (type == kMergeRecord) {
    return false;
  } else {
    non_const_entry->offset = last_entry_offset;
    return true;
  }
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndex(
    ColumnFamilyHandle* column_family, const Slice& key, WriteType type) {
  if (!UpdateExistingEntry(column_family, key, type)) {
    uint32_t cf_id = GetColumnFamilyID(column_family);
    const auto* cf_cmp = GetColumnFamilyUserComparator(column_family);
    if (cf_cmp != nullptr) {
      comparator.SetComparatorForCF(cf_id, cf_cmp);
    }
    AddNewEntry(cf_id);
  }
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndex(const Slice& key,
                                                WriteType type) {
  if (!UpdateExistingEntryWithCfId(0, key, type)) {
    AddNewEntry(0);
  }
}

void WriteBatchWithIndex::Rep::AddNewEntry(uint32_t column_family_id) {
  const std::string& wb_data = write_batch.Data();
  Slice entry_ptr = Slice(wb_data.data() + last_entry_offset,
                          wb_data.size() - last_entry_offset);
  // Extract key
  Slice key;
  bool success __attribute__((__unused__));
  success =
      ReadKeyFromWriteBatchEntry(&entry_ptr, &key, column_family_id != 0);
  assert(success);

  auto* mem = arena.Allocate(sizeof(WriteBatchIndexEntry));
  auto* index_entry =
      new (mem) WriteBatchIndexEntry(last_entry_offset, column_family_id,
                                      key.data() - wb_data.data(), key.size());
  skip_list.Insert(index_entry);
}

void WriteBatchWithIndex::Rep::Clear() {
  write_batch.Clear();
  ClearIndex();
}

void WriteBatchWithIndex::Rep::ClearIndex() {
  skip_list.~WriteBatchEntrySkipList();
  arena.~Arena();
  new (&arena) Arena();
  new (&skip_list) WriteBatchEntrySkipList(comparator, &arena);
  last_entry_offset = 0;
  last_sub_batch_offset = 0;
  sub_batch_cnt = 1;
}

Status WriteBatchWithIndex::Rep::ReBuildIndex() {
  Status s;

  ClearIndex();

  if (write_batch.Count() == 0) {
    // Nothing to re-index
    return s;
  }

  size_t offset = WriteBatchInternal::GetFirstOffset(&write_batch);

  Slice input(write_batch.Data());
  input.remove_prefix(offset);

  // Loop through all entries in Rep and add each one to the index
  uint32_t found = 0;
  while (s.ok() && !input.empty()) {
    Slice key, value, blob, xid;
    uint32_t column_family_id = 0;  // default
    char tag = 0;

    // set offset of current entry for call to AddNewEntry()
    last_entry_offset = input.data() - write_batch.Data().data();

    s = ReadRecordFromWriteBatch(&input, &tag, &column_family_id, &key,
                                  &value, &blob, &xid);
    if (!s.ok()) {
      break;
    }

    switch (tag) {
      case kTypeColumnFamilyValue:
      case kTypeValue:
        found++;
        if (!UpdateExistingEntryWithCfId(column_family_id, key, kPutRecord)) {
          AddNewEntry(column_family_id);
        }
        break;
      case kTypeColumnFamilyDeletion:
      case kTypeDeletion:
        found++;
        if (!UpdateExistingEntryWithCfId(column_family_id, key,
                                         kDeleteRecord)) {
          AddNewEntry(column_family_id);
        }
        break;
      case kTypeColumnFamilySingleDeletion:
      case kTypeSingleDeletion:
        found++;
        if (!UpdateExistingEntryWithCfId(column_family_id, key,
                                         kSingleDeleteRecord)) {
          AddNewEntry(column_family_id);
        }
        break;
      case kTypeColumnFamilyMerge:
      case kTypeMerge:
        found++;
        if (!UpdateExistingEntryWithCfId(column_family_id, key, kMergeRecord)) {
          AddNewEntry(column_family_id);
        }
        break;
      case kTypeLogData:
      case kTypeBeginPrepareXID:
      case kTypeBeginPersistedPrepareXID:
      case kTypeBeginUnprepareXID:
      case kTypeEndPrepareXID:
      case kTypeCommitXID:
      case kTypeRollbackXID:
      case kTypeNoop:
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag in ReBuildIndex",
                                  ToString(static_cast<unsigned int>(tag)));
    }
  }

  if (s.ok() && found != write_batch.Count()) {
    s = Status::Corruption("WriteBatch has wrong count");
  }

  return s;
}

WriteBatchWithIndex::WriteBatchWithIndex(
    const Comparator* default_index_comparator, size_t reserved_bytes,
    bool overwrite_key, size_t max_bytes)
    : rep(new Rep(default_index_comparator, reserved_bytes, max_bytes,
                  overwrite_key)) {}

WriteBatchWithIndex::~WriteBatchWithIndex() {}

WriteBatchWithIndex::WriteBatchWithIndex(WriteBatchWithIndex&&) = default;

WriteBatchWithIndex& WriteBatchWithIndex::operator=(WriteBatchWithIndex&&) =
    default;

WriteBatch* WriteBatchWithIndex::GetWriteBatch() { return &rep->write_batch; }

size_t WriteBatchWithIndex::SubBatchCnt() { return rep->sub_batch_cnt; }

WBWIIterator* WriteBatchWithIndex::NewIterator() {
  return new WBWIIteratorImpl(0, &(rep->skip_list), &rep->write_batch,
                              &(rep->comparator));
}

WBWIIterator* WriteBatchWithIndex::NewIterator(
    ColumnFamilyHandle* column_family) {
  return new WBWIIteratorImpl(GetColumnFamilyID(column_family),
                              &(rep->skip_list), &rep->write_batch,
                              &(rep->comparator));
}

Iterator* WriteBatchWithIndex::NewIteratorWithBase(DB* db,
                                                   Iterator* base_iterator,
                                                   const ReadOptions* opts) {
  return NewIteratorWithBase(db, db->DefaultColumnFamily(), base_iterator,
                             opts);
}

Iterator* WriteBatchWithIndex::NewIteratorWithBase(
    ColumnFamilyHandle* column_family, Iterator* base_iterator,
    const ReadOptions* opts) {
  return NewIteratorWithBase(nullptr, column_family, base_iterator, opts);
}

Iterator* WriteBatchWithIndex::NewIteratorWithBase(
    DB* db, ColumnFamilyHandle* column_family, Iterator* base_iterator,
    const ReadOptions* opts) {
  auto wbwiii =
      new WBWIIteratorImpl(GetColumnFamilyID(column_family), &(rep->skip_list),
                           &rep->write_batch, &rep->comparator);
  return new BaseDeltaIterator(db, column_family, base_iterator, wbwiii,
                               GetColumnFamilyUserComparator(column_family),
                               opts);
}
Iterator* WriteBatchWithIndex::NewIteratorWithBase(Iterator* base_iterator) {
  // default column family's comparator
  auto wbwiii = new WBWIIteratorImpl(0, &(rep->skip_list), &rep->write_batch,
                                     &rep->comparator);
  return new BaseDeltaIterator(nullptr, nullptr, base_iterator, wbwiii,
                               rep->comparator.default_comparator());
}

Status WriteBatchWithIndex::Put(ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Put(column_family, key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, key, kPutRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Put(const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Put(key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(key, kPutRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Delete(ColumnFamilyHandle* column_family,
                                   const Slice& key) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Delete(column_family, key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, key, kDeleteRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Delete(const Slice& key) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Delete(key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(key, kDeleteRecord);
  }
  return s;
}

Status WriteBatchWithIndex::SingleDelete(ColumnFamilyHandle* column_family,
                                         const Slice& key) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.SingleDelete(column_family, key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, key, kSingleDeleteRecord);
  }
  return s;
}

Status WriteBatchWithIndex::SingleDelete(const Slice& key) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.SingleDelete(key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(key, kSingleDeleteRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Merge(ColumnFamilyHandle* column_family,
                                  const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Merge(column_family, key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, key, kMergeRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Merge(const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Merge(key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(key, kMergeRecord);
  }
  return s;
}

Status WriteBatchWithIndex::PutLogData(const Slice& blob) {
  return rep->write_batch.PutLogData(blob);
}

void WriteBatchWithIndex::Clear() { rep->Clear(); }

Status WriteBatchWithIndex::GetFromBatch(ColumnFamilyHandle* column_family,
                                         const DBOptions& options,
                                         const Slice& key, std::string* value) {
  Status s;
  WriteBatchWithIndexInternal wbwii(&options, column_family);
  auto result = wbwii.GetFromBatch(this, key, value, &s);
  switch (result) {
    case WBWIIteratorImpl::kFound:
    case WBWIIteratorImpl::kError:
      // use returned status
      break;
    case WBWIIteratorImpl::kDeleted:
    case WBWIIteratorImpl::kNotFound:
      s = Status::NotFound();
      break;
    case WBWIIteratorImpl::kMergeInProgress:
      s = Status::MergeInProgress();
      break;
    default:
      assert(false);
  }

  return s;
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              const Slice& key,
                                              std::string* value) {
  assert(value != nullptr);
  PinnableSlice pinnable_val(value);
  assert(!pinnable_val.IsPinned());
  auto s = GetFromBatchAndDB(db, read_options, db->DefaultColumnFamily(), key,
                             &pinnable_val);
  if (s.ok() && pinnable_val.IsPinned()) {
    value->assign(pinnable_val.data(), pinnable_val.size());
  }  // else value is already assigned
  return s;
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              const Slice& key,
                                              PinnableSlice* pinnable_val) {
  return GetFromBatchAndDB(db, read_options, db->DefaultColumnFamily(), key,
                           pinnable_val);
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key,
                                              std::string* value) {
  assert(value != nullptr);
  PinnableSlice pinnable_val(value);
  assert(!pinnable_val.IsPinned());
  auto s =
      GetFromBatchAndDB(db, read_options, column_family, key, &pinnable_val);
  if (s.ok() && pinnable_val.IsPinned()) {
    value->assign(pinnable_val.data(), pinnable_val.size());
  }  // else value is already assigned
  return s;
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key,
                                              PinnableSlice* pinnable_val) {
  return GetFromBatchAndDB(db, read_options, column_family, key, pinnable_val,
                           nullptr);
}

Status WriteBatchWithIndex::GetFromBatchAndDB(
    DB* db, const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const Slice& key, PinnableSlice* pinnable_val, ReadCallback* callback) {
  Status s;
  WriteBatchWithIndexInternal wbwii(db, column_family);

  // Since the lifetime of the WriteBatch is the same as that of the transaction
  // we cannot pin it as otherwise the returned value will not be available
  // after the transaction finishes.
  std::string& batch_value = *pinnable_val->GetSelf();
  auto result = wbwii.GetFromBatch(this, key, &batch_value, &s);

  if (result == WBWIIteratorImpl::kFound) {
    pinnable_val->PinSelf();
    return s;
  }
  if (result == WBWIIteratorImpl::kDeleted) {
    return Status::NotFound();
  }
  if (result == WBWIIteratorImpl::kError) {
    return s;
  }
  assert(result == WBWIIteratorImpl::kMergeInProgress ||
         result == WBWIIteratorImpl::kNotFound);

  // Did not find key in batch OR could not resolve Merges.  Try DB.
  if (!callback) {
    s = db->Get(read_options, column_family, key, pinnable_val);
  } else {
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = column_family;
    get_impl_options.value = pinnable_val;
    get_impl_options.callback = callback;
    s = static_cast_with_check<DBImpl>(db->GetRootDB())
            ->GetImpl(read_options, key, get_impl_options);
  }

  if (s.ok() || s.IsNotFound()) {  // DB Get Succeeded
    if (result == WBWIIteratorImpl::kMergeInProgress) {
      // Merge result from DB with merges in Batch
      std::string merge_result;
      if (s.ok()) {
        s = wbwii.MergeKey(key, pinnable_val, &merge_result);
      } else {  // Key not present in db (s.IsNotFound())
        s = wbwii.MergeKey(key, nullptr, &merge_result);
      }
      if (s.ok()) {
        pinnable_val->Reset();
        *pinnable_val->GetSelf() = std::move(merge_result);
        pinnable_val->PinSelf();
      }
    }
  }

  return s;
}

void WriteBatchWithIndex::MultiGetFromBatchAndDB(
    DB* db, const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const size_t num_keys, const Slice* keys, PinnableSlice* values,
    Status* statuses, bool sorted_input) {
  MultiGetFromBatchAndDB(db, read_options, column_family, num_keys, keys,
                         values, statuses, sorted_input, nullptr);
}

void WriteBatchWithIndex::MultiGetFromBatchAndDB(
    DB* db, const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const size_t num_keys, const Slice* keys, PinnableSlice* values,
    Status* statuses, bool sorted_input, ReadCallback* callback) {
  WriteBatchWithIndexInternal wbwii(db, column_family);
  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  // To hold merges from the write batch
  autovector<std::pair<WBWIIteratorImpl::Result, MergeContext>,
             MultiGetContext::MAX_BATCH_SIZE>
      merges;
  // Since the lifetime of the WriteBatch is the same as that of the transaction
  // we cannot pin it as otherwise the returned value will not be available
  // after the transaction finishes.
  for (size_t i = 0; i < num_keys; ++i) {
    MergeContext merge_context;
    PinnableSlice* pinnable_val = &values[i];
    std::string& batch_value = *pinnable_val->GetSelf();
    Status* s = &statuses[i];
    auto result =
        wbwii.GetFromBatch(this, keys[i], &merge_context, &batch_value, s);

    if (result == WBWIIteratorImpl::kFound) {
      pinnable_val->PinSelf();
      continue;
    }
    if (result == WBWIIteratorImpl::kDeleted) {
      *s = Status::NotFound();
      continue;
    }
    if (result == WBWIIteratorImpl::kError) {
      continue;
    }
    assert(result == WBWIIteratorImpl::kMergeInProgress ||
           result == WBWIIteratorImpl::kNotFound);
    key_context.emplace_back(column_family, keys[i], &values[i],
                             /*timestamp*/ nullptr, &statuses[i]);
    merges.emplace_back(result, std::move(merge_context));
  }

  for (KeyContext& key : key_context) {
    sorted_keys.emplace_back(&key);
  }

  // Did not find key in batch OR could not resolve Merges.  Try DB.
  static_cast_with_check<DBImpl>(db->GetRootDB())
      ->PrepareMultiGetKeys(key_context.size(), sorted_input, &sorted_keys);
  static_cast_with_check<DBImpl>(db->GetRootDB())
      ->MultiGetWithCallback(read_options, column_family, callback,
                             &sorted_keys);

  for (auto iter = key_context.begin(); iter != key_context.end(); ++iter) {
    KeyContext& key = *iter;
    if (key.s->ok() || key.s->IsNotFound()) {  // DB Get Succeeded
      size_t index = iter - key_context.begin();
      std::pair<WBWIIteratorImpl::Result, MergeContext>& merge_result =
          merges[index];
      if (merge_result.first == WBWIIteratorImpl::kMergeInProgress) {
        // Merge result from DB with merges in Batch
        Slice* merge_data;
        if (key.s->ok()) {
          merge_data = iter->value;
        } else {  // Key not present in db (s.IsNotFound())
          merge_data = nullptr;
        }
        *key.s = wbwii.MergeKey(*key.key, merge_data, merge_result.second,
                                key.value->GetSelf());
        key.value->PinSelf();
      }
    }
  }
}

void WriteBatchWithIndex::SetSavePoint() { rep->write_batch.SetSavePoint(); }

Status WriteBatchWithIndex::RollbackToSavePoint() {
  Status s = rep->write_batch.RollbackToSavePoint();

  if (s.ok()) {
    rep->sub_batch_cnt = 1;
    rep->last_sub_batch_offset = 0;
    s = rep->ReBuildIndex();
  }

  return s;
}

Status WriteBatchWithIndex::PopSavePoint() {
  return rep->write_batch.PopSavePoint();
}

void WriteBatchWithIndex::SetMaxBytes(size_t max_bytes) {
  rep->write_batch.SetMaxBytes(max_bytes);
}

size_t WriteBatchWithIndex::GetDataSize() const {
  return rep->write_batch.GetDataSize();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
