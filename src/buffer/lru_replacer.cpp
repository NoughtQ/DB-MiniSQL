#include "buffer/lru_replacer.h"
#include "glog/logging.h"
#include <ostream>

LRUReplacer::LRUReplacer(size_t num_pages):
  max_size_(num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // 返回双向链表的最后一个元素
  if(lru_list_.empty()){
    LOG(INFO) << "LRUReplacer is empty" << std::endl;
    frame_id = nullptr;
    return false;
  }
  *frame_id = lru_list_.back();
  lru_map_.erase(*frame_id);
  lru_list_.pop_back();
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  auto it = lru_map_.find(frame_id);
  if (it == lru_map_.end()){
    LOG(WARNING) << "Pin frame_id not found: " << frame_id << std::endl;
    return;
  }
  lru_list_.erase(it->second); // 从链表中删除
  lru_map_.erase(it); // 从哈希表中删除
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  auto it = lru_map_.find(frame_id);
  if(it != lru_map_.end()) {
    LOG(WARNING) << "frame_id already exists: " << frame_id << std::endl;
    return;
  }
  lru_list_.push_front(frame_id); // 插入链表头部
  lru_map_[frame_id] = lru_list_.begin(); // 插入哈希表
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list_.size(); // 返回链表的大小
}