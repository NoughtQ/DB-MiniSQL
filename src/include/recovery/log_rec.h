#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
  kInvalid,
  kInsert,
  kDelete,
  kUpdate,
  kBegin,
  kCommit,
  kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 * insert: new_rec_
 * delete: old_rec_
 * update: all
 */
struct LogRec {
  LogRec() = default;

  LogRecType type_{LogRecType::kInvalid};
  lsn_t lsn_{INVALID_LSN};
  lsn_t prev_lsn_{INVALID_LSN};
  txn_id_t txn_id_;
  std::pair<KeyType, ValType> old_rec_;
  std::pair<KeyType, ValType> new_rec_;

  /* used for testing only */
  static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
  static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  auto prev = LogRec::prev_lsn_map_.find(txn_id);
  if (prev == LogRec::prev_lsn_map_.end()) {
    throw "Invalid Txn id";
    return nullptr;
  }
  LogRecPtr ptr = std::make_shared<LogRec>();
  ptr->type_ = LogRecType::kInsert;
  ptr->prev_lsn_ = prev->second;
  ptr->lsn_ = LogRec::next_lsn_++;
  ptr->txn_id_ = txn_id;
  LogRec::prev_lsn_map_[txn_id] = ptr->lsn_;

  ptr->new_rec_ = make_pair(ins_key, ins_val);

  return ptr;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  auto prev = LogRec::prev_lsn_map_.find(txn_id);
  if (prev == LogRec::prev_lsn_map_.end()) {
    throw "Invalid Txn id";
    return nullptr;
  }
  LogRecPtr ptr = std::make_shared<LogRec>();
  ptr->type_ = LogRecType::kDelete;
  ptr->prev_lsn_ = prev->second;
  ptr->lsn_ = LogRec::next_lsn_++;
  ptr->txn_id_ = txn_id;
  LogRec::prev_lsn_map_[txn_id] = ptr->lsn_;

  ptr->old_rec_ = make_pair(del_key, del_val);

  return ptr;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
  auto prev = LogRec::prev_lsn_map_.find(txn_id);
  if (prev == LogRec::prev_lsn_map_.end()) {
    throw "Invalid Txn id";
    return nullptr;
  }
  LogRecPtr ptr = std::make_shared<LogRec>();
  ptr->type_ = LogRecType::kUpdate;
  ptr->prev_lsn_ = prev->second;
  ptr->lsn_ = LogRec::next_lsn_++;
  ptr->txn_id_ = txn_id;
  LogRec::prev_lsn_map_[txn_id] = ptr->lsn_;

  ptr->old_rec_ = make_pair(old_key, old_val);
  ptr->new_rec_ = make_pair(new_key, new_val);

  return ptr;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
  auto prev = LogRec::prev_lsn_map_.find(txn_id);
  if (prev != LogRec::prev_lsn_map_.end()) {
    throw "Invalid Txn id";
    return nullptr;
  }
  LogRecPtr ptr = std::make_shared<LogRec>();
  ptr->type_ = LogRecType::kBegin;
  ptr->prev_lsn_ = INVALID_LSN;
  ptr->lsn_ = LogRec::next_lsn_++;
  ptr->txn_id_ = txn_id;
  LogRec::prev_lsn_map_[txn_id] = ptr->lsn_;

  return ptr;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  auto prev = LogRec::prev_lsn_map_.find(txn_id);
  if (prev == LogRec::prev_lsn_map_.end()) {
    throw "Invalid Txn id";
    return nullptr;
  }
  LogRecPtr ptr = std::make_shared<LogRec>();
  ptr->type_ = LogRecType::kCommit;
  ptr->prev_lsn_ = prev->second;
  ptr->lsn_ = LogRec::next_lsn_++;
  ptr->txn_id_ = txn_id;
  LogRec::prev_lsn_map_.erase(txn_id);

  return ptr;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  auto prev = LogRec::prev_lsn_map_.find(txn_id);
  if (prev == LogRec::prev_lsn_map_.end()) {
    throw "Invalid Txn id";
    return nullptr;
  }

  LogRecPtr ptr = std::make_shared<LogRec>();
  ptr->type_ = LogRecType::kAbort;
  ptr->prev_lsn_ = prev->second;
  ptr->lsn_ = LogRec::next_lsn_++;
  ptr->txn_id_ = txn_id;
  LogRec::prev_lsn_map_.erase(txn_id);

  return ptr;
}

#endif  // MINISQL_LOG_REC_H
