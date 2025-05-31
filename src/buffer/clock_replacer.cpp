#include "buffer/clock_replacer.h"
#include "glog/logging.h"
#include <ostream>

CLOCKReplacer::CLOCKReplacer(size_t num_pages):
  capacity(0), clock_list_(num_pages, make_pair(false,false)), clock_hand_(0){}

CLOCKReplacer::~CLOCKReplacer() = default;

/**
 * TODO: Student Implement
 */
bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
    // 空表，直接返回
    if (capacity == 0) {
        LOG(INFO) << "CLOCKReplacer is empty" << std::endl;
        return false;
    }

    // 确保时钟指针有效（若无效则重置到数组头部）
    if (clock_hand_ == clock_list_.size()) {
        clock_hand_ = 0;
    }

    // 循环查找可替换的页帧
    while (true) {
        auto &it = clock_list_[clock_hand_];
        if (!it.second) {
            // if not valid
            clock_hand_++;
        } else if (it.first){
            // valid and ref bit = 1
            it.first = false;
            clock_hand_++;
        } else {
            *frame_id = clock_map_[clock_hand_];
            // 删除该页帧，但不必移动指针
            it.second = false;
            clock_map_.erase(clock_hand_);
            capacity--;
            return true;
        }
        if (clock_hand_ == clock_list_.size()) {
                clock_hand_ = 0;
        }
    }
}

/**
 * TODO: Student Implement
 */
void CLOCKReplacer::Pin(frame_id_t frame_id) {
    for(auto &it : clock_map_){
        if(it.second == frame_id){
            clock_list_[it.first] = make_pair(false, false);
            clock_map_.erase(it.first);
            capacity--;
            break;
        }
    }
}

/**
 * TODO: Student Implement
 */
void CLOCKReplacer::Unpin(frame_id_t frame_id) {
    // 若页帧已在Clock中
    for(auto &it : clock_map_){
        if(it.second == frame_id){
            // 按照算法：unpin ---> ref bit <= true
            // 但应该不会这样用（
            clock_list_[it.first].first = true;
            return;
        }
    }

    // 若list已满
    if (capacity >= clock_list_.size()) {
        LOG(ERROR) << "CLOCKReplacer is full" << std::endl;
        frame_id_t* t;
        Victim(t);
    }

    // 添加新页帧到一个原先invalid的位置，并移动指针
    while(clock_list_[clock_hand_].first){
        clock_hand_++;
        if(clock_hand_ == clock_list_.size()) clock_hand_=0;
    }
    clock_list_[clock_hand_] = make_pair(false, true);
    clock_map_[clock_hand_] = frame_id;
    clock_hand_++;
    capacity++;

}

/**
 * TODO: Student Implement
 */
size_t CLOCKReplacer::Size() {
  return capacity; // 返回链表的大小
}