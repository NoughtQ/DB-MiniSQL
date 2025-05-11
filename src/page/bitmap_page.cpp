#include "page/bitmap_page.h"

#include "glog/logging.h"

static constexpr uint32_t INVALID_PAGE = 0xFFFFFFFF;  // 使用最大的32位无符号整数

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (next_free_page_ == INVALID_PAGE) {
    return false;
  }
  page_offset = next_free_page_; // 分配下一个可用的页面

  /* 更新位图 */
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  bytes[byte_index] |= (1 << bit_index);

  /* 更新下一个可用的页面 */
  uint32_t start = next_free_page_;
  next_free_page_ = (next_free_page_ + 1) % GetMaxSupportedSize();  // 循环使用位图 (circular bitmap)
  while(next_free_page_ != start){
    if(IsPageFree(next_free_page_)){
      break;
    }
    next_free_page_ = (next_free_page_ + 1) % GetMaxSupportedSize();  // 循环使用位图 (circular bitmap)
  }
  if(next_free_page_ == start){
    next_free_page_ = INVALID_PAGE; // 没有可用的页面
  }
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  /* 检查页面偏移是否有效 */
  if (page_offset >= GetMaxSupportedSize()) {
    return false;
  }

  /* 检查页面是否已经被释放 */
  if (IsPageFree(page_offset)) {
    return false;
  }

  /* 更新位图 */
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  bytes[byte_index] &= ~(1 << bit_index);

  /* 更新下一个可用的页面 */
  if(next_free_page_ == INVALID_PAGE){
    next_free_page_ = page_offset;
  }
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  return !(bytes[byte_index] & (1 << bit_index)); // 检查对应位是否为1，为1则表示页面已被分配，为0则表示页面未被分配
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;