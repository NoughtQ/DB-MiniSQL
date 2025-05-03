#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
// auua:next_page_id在调用init之前已经给了INVALID_PAGE_ID
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  page_type_ = IndexPageType::LEAF_PAGE;
  page_id_ = page_id;
  parent_page_id_ = parent_id;
  size_ = 0;
  max_size_ = max_size;
  key_size_ = key_size;
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const { return next_page_id_; }

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int left = 0;
  int right = GetSize() - 1;

  // Early check if all keys are smaller than target
  if (KM.CompareKeys(KeyAt(right), key) < 0) {
    return -1;
  }
  // Early check if first key is >= target
  if (KM.CompareKeys(KeyAt(left), key) >= 0) {
    return left;
  }

  // Binary search for the first key >= target
  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = KM.CompareKeys(KeyAt(mid), key);

    if (cmp < 0) {
      left = mid + 1;
    } else if (cmp > 0) {
      right = mid - 1;
    } else {
      return mid;
    }
  }

  return left;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) { return KeyAt(index); }

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
// auua: 额外规定，如果插入的key已经存在则返回-1
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int current_size = GetSize();
  int index = KeyIndex(key, KM);
  if (KM.CompareKeys(KeyAt(index), key) == 0) return -1;

  // insert to the tail has no need to memmove
  if (index < 0) {
    index = current_size;
  } else {
    memmove(PairPtrAt(index + 1), PairPtrAt(index), (current_size - index) * pair_size);
  }

  // Insert new key-value pair
  SetKeyAt(index, key);
  SetValueAt(index, value);

  // Update size and return new size
  SetSize(current_size + 1);
  return current_size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
// auua: 很奇怪，我觉得recipient应该和该节点比较一下大小，区分应该移动前一半还是后一半...
// auua: 另外，移动一半，这里选择移出去的部分大于等于剩下的
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int current_size = GetSize();
  if (current_size <= 1) return;

  int start_index = current_size / 2;
  int num_items = current_size - start_index;

  // Copy items to recipient
  recipient->CopyNFrom(PairPtrAt(start_index), num_items);

  // Update size (recipient->size change in CopyNfrom())
  SetSize(current_size - num_items);
}

// auua:直接插入到了最后面，不过是不是应该看是插入到前面还是后面（
/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  ASSERT(GetSize() + size <= GetMaxSize(), "Exceeds page capacity");

  void *dest = PairPtrAt(GetSize());
  PairCopy(dest, src, size);
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if (index >= 0 && KM.CompareKeys(KeyAt(index), key) == 0) {
    value = ValueAt(index);
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  // if find the key
  if (index >= 0 && KM.CompareKeys(KeyAt(index), key) == 0) {
    int move_num = GetSize() - index - 1;
    // if not the last one
    if (move_num > 0) {
      memmove(PairPtrAt(index), PairPtrAt(index + 1), move_num * pair_size);
    }
    IncreaseSize(-1);
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
// auua: 将本节点的键值对移动到recipient的后面
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  int size = GetSize();
  // If nothing to move, just update the next page pointer
  if (size == 0) {
    recipient->SetNextPageId(GetNextPageId());
    return;
  }

  // Copy all items to the end of recipient
  int recipient_size = recipient->GetSize();
  memcpy(recipient->PairPtrAt(recipient_size), PairPtrAt(0), size * pair_size);

  // Update sizes and pointers
  recipient->IncreaseSize(size);
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  if (GetSize() == 0) return;

  // insert
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0));

  // Shift remaining items left
  if (GetSize() > 1) {
    memmove(PairPtrAt(0), PairPtrAt(1), (GetSize() - 1) * pair_size);
  }
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  SetKeyAt(GetSize(), key);
  SetValueAt(GetSize(), value);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  if (GetSize() == 0) return;

  // Copy last item to recipient's front
  recipient->CopyFirstFrom(KeyAt(GetSize() - 1), ValueAt(GetSize() - 1));
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  if (GetSize() > 0) {
    memmove(PairPtrAt(1), PairPtrAt(0), GetSize() * pair_size);
  }
  SetKeyAt(0, key);
  SetValueAt(0, value);
  IncreaseSize(1);
}
