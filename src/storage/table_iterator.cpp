#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) : table_heap_(table_heap), rid_(rid), txn_(txn) {
    row_ = new Row(rid);
    if (rid_.GetPageId() != INVALID_PAGE_ID) {
        // LOG(INFO) << "TableIterator initialized with rid: " << rid_.GetPageId() << " " << rid_.GetSlotNum() << std::endl;
        table_heap_->GetTuple(row_, txn_);
        // LOG(INFO) << "Row info: " << row_->GetFieldCount() << std::endl;
    }
}

TableIterator::TableIterator(const TableIterator &other) {
    table_heap_ = other.table_heap_;
    rid_ = other.rid_;
    txn_ = other.txn_;
    row_ = new Row(*other.row_);
}

TableIterator::~TableIterator() {
    delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
    return rid_ == itr.rid_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
    return !(*this == itr);
}

const Row &TableIterator::operator*() {
    return *row_;
}

Row *TableIterator::operator->() {
    return row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
    if (this != &itr) {
        table_heap_ = itr.table_heap_;
        rid_ = itr.rid_;
        txn_ = itr.txn_;
        delete row_;
        row_ = new Row(*itr.row_);
    }
    return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
    auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
    RowId next_rid;
    if (page != nullptr) {
        if (page->GetNextTupleRid(rid_, &next_rid)) { // 当前页面还有下一个元组
            rid_ = next_rid;
            row_->SetRowId(rid_);
        } else { // 需要跨页
            while (page->GetNextPageId() != INVALID_PAGE_ID) { // 有可能一个页中的所有元组都被删除了
                auto next_page_id = page->GetNextPageId();
                table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
                page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
                if (page->GetFirstTupleRid(&next_rid)) {
                    rid_ = next_rid;
                    row_->SetRowId(rid_);
                    break;
                }
            }
            if (page->GetNextPageId() == INVALID_PAGE_ID) { // 找不到下一个元组，即 tabel_heap_->end()
                rid_.Set(INVALID_PAGE_ID, 0);
            }
        }
        if (rid_.GetPageId() != INVALID_PAGE_ID) { // 读取下一个元组
            row_->destroy();
            table_heap_->GetTuple(row_, txn_);
        }
    } else {
        LOG(ERROR) << "Failed to fetch page when incrementing TableIterator: " << rid_.GetPageId() << std::endl;
    }
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
    TableIterator temp(*this);
    ++(*this);
    return TableIterator(temp);
}
