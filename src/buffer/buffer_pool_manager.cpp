#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // 1.1
    Page *page = &pages_[it->second]; // pages_[frame_id]
    if (page->pin_count_ == 0) {
      replacer_->Pin(it->second);
    }
    page->pin_count_++;
    return page;
  } else {
    // 1.2
    frame_id_t frame_id;
    if (!free_list_.empty()) { // 内存池未满
      // 1.2.1
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else { // 使用 lruReplace
      // 1.2.2
      if (!replacer_->Victim(&frame_id)) {
        return nullptr;
      }
    }
    Page *page = &pages_[frame_id]; // 老的 page
    // 2
    if (page->is_dirty_) {
      FlushPage(page->page_id_);
    }
    // 3
    page_table_.erase(page->page_id_);
    page_table_.insert({page_id, frame_id});
    // 4
    page->page_id_ = page_id;
    page->is_dirty_ = false;
    page->pin_count_ = 1;
    disk_manager_->ReadPage(page_id, page->data_);
    return page;
  }
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t frame_id;
  Page *page;
  if(free_list_.empty()) { // 内存池已满
    if(!replacer_->Victim(&frame_id)) {
      page_id = INVALID_PAGE_ID;
      return nullptr;
    }
    // else the victim page is found
    page = &pages_[frame_id];
    if(page->is_dirty_) { // 有可能是脏页
      FlushPage(page->page_id_);
    }
    page_table_.erase(page->page_id_); // 善后
    page->ResetMemory();
  } else {  // 内存还空着
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  }
  page_id = AllocatePage(); // 分配新的 page_id
  page_table_.insert({page_id, frame_id});
  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  return page;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  auto it = page_table_.find(page_id);
  if(it == page_table_.end()) {
    DeallocatePage(page_id);  // 说明已经被替换掉，那就直接删除
    return true;
  }
  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];
  if(page->pin_count_ != 0) {
    LOG(ERROR) << "Unable to delete page " << page_id << ": pin count = " << page->pin_count_ << endl;
    return false;
  }
  replacer_->Pin(frame_id);  // 需要将其从 replacer 中删除
  page_table_.erase(page_id);  // 删除元信息
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  free_list_.push_back(frame_id);  // 释放内存
  DeallocatePage(page_id);
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    auto it = page_table_.find(page_id);
    if(it == page_table_.end()) {
        return false;
    }
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];
    // LOG(INFO) << "Unpin page: " << page_id << ", pin count: " << page->pin_count_ << endl;
    if(page->pin_count_ == 0) {
        LOG(ERROR) << "Unable to unpin page " << page_id << ": pin count = " << page->pin_count_ << endl;
        return false;
    }
    page->pin_count_--;
    if(page->pin_count_ == 0) {
        replacer_->Unpin(frame_id);
    }
    page->is_dirty_ |= is_dirty;
    return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
    auto it = page_table_.find(page_id);
    if(it == page_table_.end()) {
        LOG(ERROR) << "Cannot flush page " << page_id << ": not found" << endl;
        return false;
    }
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];
    if(page->is_dirty_) {
        disk_manager_->WritePage(page_id, page->data_);
        page->is_dirty_ = false;
    }
    return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}