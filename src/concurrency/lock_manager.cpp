#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
    // 如果隔离级别是 READ_UNCOMMITTED，不允许加共享锁
    if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
    }

    std::unique_lock<std::mutex> lk(latch_);
    LockPrepare(txn, rid);
    auto &req_queue = lock_table_.at(rid);

    // 检查事务是否已经持有该记录的锁
    auto iter = req_queue.req_list_iter_map_.find(txn->GetTxnId());
    if (iter != req_queue.req_list_iter_map_.end()) {
        if (iter->second->granted_ != LockMode::kNone) {
            return true;  // 已经持有锁，直接返回
        }
    } else {
        // 添加新的锁请求
        req_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kShared);
        iter = req_queue.req_list_iter_map_.find(txn->GetTxnId());
    }

    // 等待直到可以获取共享锁
    while (true) {
        CheckAbort(txn, req_queue);
        
        // 如果没有写锁且没有正在进行的锁升级，可以获取共享锁
        if (!req_queue.is_writing_ && !req_queue.is_upgrading_) {
            iter->second->granted_ = LockMode::kShared;
            req_queue.sharing_cnt_++;
            txn->GetSharedLockSet().insert(rid);
            return true;
        }

        // 等待其他事务释放锁
        req_queue.cv_.wait(lk);
    }

    return false;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> lk(latch_);
    LockPrepare(txn, rid);
    auto &req_queue = lock_table_.at(rid);

    // 检查事务是否已经持有该记录的锁
    auto iter = req_queue.req_list_iter_map_.find(txn->GetTxnId());
    if (iter != req_queue.req_list_iter_map_.end()) {
        if (iter->second->granted_ == LockMode::kExclusive) {
            return true;  // 已经持有独占锁，直接返回
        }
    } else {
        // 添加新的锁请求
        req_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kExclusive);
        iter = req_queue.req_list_iter_map_.find(txn->GetTxnId());
    }

    // 等待直到可以获取独占锁
    while (true) {
        CheckAbort(txn, req_queue);
        
        // 如果没有其他锁，可以获取独占锁
        if (!req_queue.is_writing_ && req_queue.sharing_cnt_ == 0) {
            iter->second->granted_ = LockMode::kExclusive;
            req_queue.is_writing_ = true;
            txn->GetExclusiveLockSet().insert(rid);
            return true;
        }

        // 等待其他事务释放锁
        req_queue.cv_.wait(lk);
    }

    return false;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> lk(latch_);
    auto &req_queue = lock_table_.at(rid);

    // 检查是否已经有其他事务在进行锁升级
    if (req_queue.is_upgrading_) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kUpgradeConflict);
    }

    // 检查事务是否持有共享锁
    auto iter = req_queue.req_list_iter_map_.find(txn->GetTxnId());
    if (iter == req_queue.req_list_iter_map_.end() || iter->second->granted_ != LockMode::kShared) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
    }

    // 标记正在进行锁升级
    req_queue.is_upgrading_ = true;
    iter->second->lock_mode_ = LockMode::kExclusive;

    // 等待直到可以获取独占锁
    while (true) {
        CheckAbort(txn, req_queue);
        
        // 如果只剩下当前事务持有共享锁，可以升级为独占锁
        if (!req_queue.is_writing_ && req_queue.sharing_cnt_ == 1) {
            iter->second->granted_ = LockMode::kExclusive;
            req_queue.is_writing_ = true;
            req_queue.is_upgrading_ = false;
            req_queue.sharing_cnt_--;
            
            // 更新事务的锁集合
            txn->GetSharedLockSet().erase(rid);
            txn->GetExclusiveLockSet().insert(rid);
            
            return true;
        }

        // 等待其他事务释放锁
        req_queue.cv_.wait(lk);
    }

    return false;
}

/**
 * TODO: Student Implement
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> lk(latch_);

    // 如果事务状态是 GROWING，则转换为 SHRINKING
    if (txn->GetState() == TxnState::kGrowing) {
        txn->SetState(TxnState::kShrinking);
    } else if (txn->GetState() == TxnState::kShrinking) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kUnlockOnShrinking);
    }

    auto &req_queue = lock_table_.at(rid);
    auto iter = req_queue.req_list_iter_map_.find(txn->GetTxnId());
    if (iter == req_queue.req_list_iter_map_.end()) {
        return false;  // 事务没有持有该记录的锁
    }

    // 根据锁类型更新计数器和标志
    if (iter->second->granted_ == LockMode::kShared) {
        req_queue.sharing_cnt_--;
        txn->GetSharedLockSet().erase(rid);
    } else if (iter->second->granted_ == LockMode::kExclusive) {
        req_queue.is_writing_ = false;
        txn->GetExclusiveLockSet().erase(rid);
    }

    // 从请求队列中移除该事务的请求
    req_queue.EraseLockRequest(txn->GetTxnId());

    // 通知其他等待的事务
    req_queue.cv_.notify_all();

    return true;
}

/**
 * TODO: Student Implement
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
    // 检查事务状态，如果是 SHRINKING，则抛出异常
    if (txn->GetState() == TxnState::kShrinking) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
    }
    
    // 在 lock_table_ 中创建 rid 对应的队列（如果不存在）
    if (lock_table_.find(rid) == lock_table_.end()) {
        lock_table_.try_emplace(rid);
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
    // 检查事务状态是否为 ABORTED
    if (txn->GetState() == TxnState::kAborted) {
        // 从请求队列中删除该事务的请求
        req_queue.EraseLockRequest(txn->GetTxnId());
        // 通知其他等待的事务
        req_queue.cv_.notify_all();
        // 抛出异常
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

/**
 * TODO: Student Implement
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

/**
 * TODO: Student Implement
 */
bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {}

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
void LockManager::RunCycleDetection() {}

/**
 * TODO: Student Implement
 */
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
    std::vector<std::pair<txn_id_t, txn_id_t>> result;
    return result;
}
