#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"
#include <vector>
#include <algorithm>
#include <thread>

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
    return false;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
    return false;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
    return false;
}

/**
 * TODO: Student Implement
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
    return false;
}

/**
 * TODO: Student Implement
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) {}

/**
 * TODO: Student Implement
 */
void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
}

/**
 * TODO: Student Implement
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].insert(t2); }

/**
 * TODO: Student Implement
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

/**
 * TODO: Student Implement
 */
bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
    auto& tid = newest_tid_in_cycle;                // avoid using lengthy name

    // find the cycle!
    if (visited_set_.count(tid) > 0) {
        auto youngest_tid = tid;
        for (const auto& elem : visited_set_) {
            if (elem < youngest_tid) {
                youngest_tid = elem;
            }
        }
        tid = youngest_tid;    // set the youngest tid
        return true;
    }

    // tid is visited
    visited_set_.insert(tid);
    visited_path_.push(tid);

    // dfs
    for (const auto& next_tid : waits_for_[tid]) {
        txn_id_t temp_tid = next_tid;  // Create a non-const copy
        if (HasCycle(temp_tid)) {
            newest_tid_in_cycle = temp_tid;
            return true;
        }
    }

    // backtrack
    visited_path_.pop();
    visited_set_.erase(tid);

    return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
    waits_for_.erase(txn_id);

    auto *txn = txn_mgr_->GetTransaction(txn_id);

    for (const auto &row_id: txn->GetSharedLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }

    for (const auto &row_id: txn->GetExclusiveLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::RunCycleDetection() {
    while (enable_cycle_detection_) {
        std::this_thread::sleep_for(cycle_detection_interval_);
        
        // clear previous detection state
        visited_set_.clear();
        while (!visited_path_.empty()) {
            visited_path_.pop();
        }
        
        // check each transaction for cycles
        std::vector<txn_id_t> txn_ids;
        for (const auto& pair : waits_for_) {
            txn_ids.push_back(pair.first);
        }
        std::sort(txn_ids.begin(), txn_ids.end());
        
        for (const auto& tid : txn_ids) {
            txn_id_t newest_tid = tid;
            if (HasCycle(newest_tid)) {
                // abort the youngest transaction in the cycle
                auto* txn = txn_mgr_->GetTransaction(newest_tid);
                if (txn != nullptr) {
                    txn_mgr_->Abort(txn);
                    DeleteNode(newest_tid);
                }
                break;
            }
        }
    }
}

/**
 * TODO: Student Implement
 */
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
    std::vector<std::pair<txn_id_t, txn_id_t>> result;
    for (const auto& elem : waits_for_) {
        for (const auto& node : elem.second) {
            result.push_back({elem.first, node});
        }
    }
    return result;
}
