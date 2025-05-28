#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
    if (first_page_id_ == INVALID_PAGE_ID) {
        LOG(ERROR) << "Failed to insert tuple: table is empty" << std::endl;
    }
    if (row.GetSerializedSize(schema_) > TablePage::SIZE_MAX_ROW) {
        LOG(ERROR) << "Failed to insert tuple: tuple is too large" << std::endl;
        return false;
    }
    // 寻找合适的页面
    uint32_t size = row.GetSerializedSize(schema_) + 8;
    page_id_t page_id = INVALID_PAGE_ID;
    auto it = free_space_.begin();
    for (; it!= free_space_.end(); ++it) {
        if (it->second >= size) {
            page_id = it->first;
            break;
        }
    }
    if (page_id != INVALID_PAGE_ID) {
        // 在找到的页面中插入元组
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if (page == nullptr) {
            LOG(ERROR) << "Failed to fetch page when insert" << std::endl;
            return false;
        }
        page->WLatch();
        // 插入并更新 free space
        bool insert_success = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
        it->second = page->GetFreeSpaceRemaining();
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
        if (!insert_success) {
            assert(false);
            LOG(ERROR) << "Unexpected error while insert" << std::endl;
        }
        return insert_success;
    } else {
        // 如果所有现有页面都已满，创建新页面
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id_));
        if (page == nullptr) {
            LOG(ERROR) << "Failed to fetch page when insert" << std::endl;
            return false;
        }
        // 确保是最后一页
        auto next_page_id = page->GetNextPageId();
        if (next_page_id!= INVALID_PAGE_ID) {
            buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
            LOG(ERROR) << "Unexpected error while insert" << std::endl;
            return false;
        }
        // 创建新页面
        page_id_t new_page_id;
        auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
        if (new_page == nullptr) {
            LOG(ERROR) << "Failed to create new page while insert" << std::endl;
            return false;
        }
        new_page->Init(new_page_id, page->GetTablePageId(), log_manager_, txn);
        new_page->SetNextPageId(INVALID_PAGE_ID);
        page->SetNextPageId(new_page_id);
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);

        new_page->WLatch();
        bool insert_success = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
        new_page->WUnlatch();
        if (!insert_success) {
            LOG(ERROR) << "Unexpected error while insert" << std::endl;
        }
        free_space_.emplace(new_page_id, new_page->GetFreeSpaceRemaining());
        last_page_id_ = new_page_id;

        buffer_pool_manager_->UnpinPage(new_page_id, true);
        return insert_success;
    }
    // // 获取第一个页面
    // auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
    // // LOG(INFO) << "Insert: fetch first page: " << first_page_id_ << std::endl;
    // if (page == nullptr) {
    //     LOG(ERROR) << "Failed to fetch page " << first_page_id_ << std::endl;
    //     return false;
    // }

    // // 尝试在现有页面中插入
    // page->WLatch();
    // bool insert_success = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    // page->WUnlatch();

    // // 如果当前页面插入失败，尝试在后续页面中插入
    // while (!insert_success && page->GetNextPageId() != INVALID_PAGE_ID) {
    //     page_id_t next_page_id = page->GetNextPageId();
    //     buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    //     page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    //     if (page == nullptr) {
    //         LOG(ERROR) << "Failed to fetch page " << next_page_id << std::endl;
    //         return false;
    //     }
    //     page->WLatch();
    //     insert_success = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    //     page->WUnlatch();
    // }

    // if (insert_success) {
    //     buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    // } else {
    //     // 如果所有现有页面都已满，创建新页面
    //     page_id_t new_page_id;
    //     auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
    //     if (new_page == nullptr) {
    //         buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    //         LOG(ERROR) << "Failed to create new page while insert" << std::endl;
    //         return false;
    //     }
    //     new_page->Init(new_page_id, page->GetTablePageId(), log_manager_, txn);
    //     page->SetNextPageId(new_page_id);
    //     buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);

    //     new_page->WLatch();
    //     insert_success = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    //     new_page->WUnlatch();
    //     if (!insert_success) {
    //         LOG(ERROR) << "Unexpected error while insert" << std::endl;
    //     }

    //     buffer_pool_manager_->UnpinPage(new_page_id, true);
    // }

    // return insert_success;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    LOG(ERROR) << "Failed to fetch page " << rid.GetPageId() << std::endl;
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  bool success = page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  if (!success) {
    LOG(ERROR) << "Unexpected behavior of MarkDelete" << std::endl;
  }
  return success;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
    // 获取包含要更新元组的页面
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        LOG(ERROR) << "Failed to fetch page " << rid.GetPageId() << std::endl;
        return false;
    }

    // 尝试在现有页面中更新
    row.SetRowId(rid);
    Row old_row(rid);
    page->WLatch();
    bool valid = false;
    bool update_success = page->UpdateTuple(row, &old_row, schema_, valid, txn, lock_manager_, log_manager_);
    page->WUnlatch();

    // 如果更新失败且有效（空间不足），需要删除旧元组并插入新元组
    if (!update_success && valid) {
        // 标记删除旧元组
        if (MarkDelete(rid, txn)) {
            // 尝试插入新元组
            if (InsertTuple(row, txn)) {
                ApplyDelete(rid, txn);
                buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
                return true;
            }
            // 插入新元组失败，需要回滚删除操作
            RollbackDelete(rid, txn);
            LOG(ERROR) << "Failed to insert new tuple" << std::endl;
        } else {
            LOG(ERROR) << "Failed to mark delete old tuple" << std::endl;
        }
    } else if (!update_success) {
        LOG(ERROR) << "Invalid args while updating" << std::endl;
    }

    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return update_success;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
    // Step1: Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        LOG(ERROR) << "Failed to fetch page when ApplyDelete: " << rid.GetPageId() << std::endl;
        return;
    }

    // Step2: Delete the tuple from the page.
    page->WLatch();
    page->ApplyDelete(rid, txn, log_manager_);
    auto it = free_space_.find(rid.GetPageId());
    if (it!= free_space_.end()) {
        it->second = page->GetFreeSpaceRemaining();
    } else {
        LOG(ERROR) << "Unexpected error while ApplyDelete" << std::endl;
    }
    page->WUnlatch();
    // 当一个 table page 中所有元组都被删除了，需要删除这个 page。这里改了之后 iterator 里也要优化
    RowId temp;
    if (page->GetFirstTupleRid(&temp) == false) {
        // 在双向链表中删除该页面
        page_id_t next_page_id_ = page->GetNextPageId();
        page_id_t prev_page_id_ = page->GetPrevPageId();
        if (next_page_id_!= INVALID_PAGE_ID) {
            auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id_));
            next_page->WLatch();
            next_page->SetPrevPageId(prev_page_id_);
            next_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(next_page_id_, true);
        }
        if (prev_page_id_!= INVALID_PAGE_ID) {
            auto prev_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(prev_page_id_));
            prev_page->WLatch();
            prev_page->SetNextPageId(next_page_id_);
            prev_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(prev_page_id_, true);
        }
        if (page->GetTablePageId() == first_page_id_ && next_page_id_ != INVALID_PAGE_ID) {
            first_page_id_ = next_page_id_;
            buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
            buffer_pool_manager_->DeletePage(page->GetTablePageId());
            free_space_.erase(page->GetTablePageId());
        } else if (page->GetTablePageId() != first_page_id_) {
            buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
            buffer_pool_manager_->DeletePage(page->GetTablePageId());
            if (page->GetTablePageId() == last_page_id_) last_page_id_ = prev_page_id_;
            free_space_.erase(page->GetTablePageId());
        } else {
            buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
        }
        return;
    }
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
    // 获取包含元组的页面
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
    if (page == nullptr) {
        LOG(ERROR) << "Failed to fetch page when GetTuple" << row->GetRowId().GetPageId() << std::endl;
        return false;
    }

    // 获取元组数据
    page->RLatch();
    bool get_success = page->GetTuple(row, schema_, txn, lock_manager_);
    page->RUnlatch();

    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return get_success;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
    // 获取第一个页面
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
    // LOG(INFO) << "In Begin: first_page_id_: " << first_page_id_ << std::endl;
    if (page == nullptr) {
        LOG(ERROR) << "Failed to fetch page when get Begin iterator(a): " << first_page_id_ << std::endl;
        return End();
    }

    // 初始化迭代器
    page->RLatch();
    RowId rid;
    // 获取页面中第一个有效的元组
    if (page->GetFirstTupleRid(&rid)) {
        // LOG(INFO) << "In Begin 0: rid: " << rid.GetPageId() << " " << rid.GetSlotNum() << std::endl;
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
        return TableIterator(this, rid, txn);
    }
    // LOG(INFO) << "In Begin 1: rid: " << rid.GetPageId() << " " << rid.GetSlotNum() << std::endl;

    // 如果当前页面没有有效元组，尝试在后续页面中查找
    while (page->GetNextPageId() != INVALID_PAGE_ID) {
        page_id_t next_page_id = page->GetNextPageId();
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
        page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
        if (page == nullptr) {
            LOG(ERROR) << "Failed to fetch page when get Begin iterator(b): " << next_page_id << std::endl;
            return End();
        }
        page->RLatch();
        if (page->GetFirstTupleRid(&rid)) {
            // LOG(INFO) << "In Begin 2: rid: " << rid.GetPageId() << " " << rid.GetSlotNum() << "pageid: " << next_page_id << std::endl;
            page->RUnlatch();
            buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
            return TableIterator(this, rid, txn);
        }
    }

    // 如果没有找到有效元组，返回End迭代器
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    //LOG(WARNING) << "Failed to find a valid tuple when get Begin iterator" << std::endl;
    return End();
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { return TableIterator(this, RowId(INVALID_PAGE_ID, 0), nullptr); }
