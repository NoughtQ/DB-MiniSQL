#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
  lsn_t checkpoint_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase persist_data_{};

  inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

  inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
 public:
  /**
   * TODO: Student Implement
   */
  void Init(CheckPoint &last_checkpoint) {
    persist_lsn_ = last_checkpoint.checkpoint_lsn_;
    active_txns_ = last_checkpoint.active_txns_;
    data_ = last_checkpoint.persist_data_;
  }

  /**
   * TODO: Student Implement
   */
  void RedoPhase() {
    auto it = log_recs_.find(persist_lsn_);

    while (it != log_recs_.end()) {
      const auto &rec = it->second;

      switch (rec->type_) {
        case LogRecType::kInsert:
          data_[rec->new_rec_.first] = rec->new_rec_.second;
          active_txns_[rec->txn_id_] = rec->lsn_;
          break;
        case LogRecType::kUpdate:
          if (rec->new_rec_.first == rec->old_rec_.first)
            data_[rec->new_rec_.first] = rec->new_rec_.second;
          else {
            data_[rec->new_rec_.first] = rec->new_rec_.second;
            data_.erase(rec->old_rec_.first);
          }
          active_txns_[rec->txn_id_] = rec->lsn_;
          break;
        case LogRecType::kDelete:
          data_.erase(rec->old_rec_.first);
          active_txns_[rec->txn_id_] = rec->lsn_;
          break;
        case LogRecType::kCommit:
          active_txns_.erase(rec->txn_id_);
          break;
        case LogRecType::kBegin:
          active_txns_[rec->txn_id_] = rec->lsn_;
          break;
        case LogRecType::kAbort:
          UndoTxn(rec->txn_id_);
          active_txns_.erase(rec->txn_id_);
        default:
          break;
      }

      ++it;
    }
  }

  /**
   * TODO: Student Implement
   */
  void UndoPhase() {
    for (const auto &active_txn : active_txns_) {
      UndoTxn(active_txn.first);
    }
    active_txns_.clear();
  }

  // used for test only
  void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

  // used for test only
  inline KvDatabase &GetDatabase() { return data_; }

 private:
  void UndoTxn(txn_id_t txn_id) {
    auto active_txn = active_txns_.find(txn_id);
    if (active_txn == active_txns_.end()) return;
    auto it = log_recs_.find(active_txn->second);

    while (it != log_recs_.end()) {
      const auto &rec = it->second;
      switch (rec->type_) {
        case LogRecType::kInsert:
          data_.erase(rec->new_rec_.first);
          break;
        case LogRecType::kUpdate:
          if (rec->new_rec_.first == rec->old_rec_.first) {
            data_[rec->new_rec_.first] = rec->old_rec_.second;
          } else {
            data_[rec->old_rec_.first] = rec->old_rec_.second;
            data_.erase(rec->new_rec_.first);
          }
          break;
        case LogRecType::kDelete:
          data_[rec->old_rec_.first] = rec->old_rec_.second;
          break;
        default:
          break;
      }
      it = log_recs_.find(rec->prev_lsn_);
    }
  }

  std::map<lsn_t, LogRecPtr> log_recs_{};
  lsn_t persist_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
