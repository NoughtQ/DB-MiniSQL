#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetKeySize(key_size);
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value) return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) { return KeyAt(index); }

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
// auua:index begin from 0 这里返回的是第一个大于key的pair的前一组的page_id
 /*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int size = GetSize();
  if (size <= 1) {
    return ValueAt(0);  // Only invalid key exists, return leftmost child
  }

  // Check if key is less than first valid key
  if (KM.CompareKeys(KeyAt(1), key) > 0) {
    return ValueAt(0);
  }

  // Check if key is greater than last key
  if (KM.CompareKeys(KeyAt(size - 1), key) <= 0) {
    return ValueAt(size - 1);
  }

  // Binary search for the key
  int left = 1;
  int right = size - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = KM.CompareKeys(KeyAt(mid),key);

    if (cmp == 0) {
      return ValueAt(mid);  // Exact match found
    } else if (cmp > 0) {
      right = mid - 1;  // Search left half
    } else {
      left = mid + 1;   // Search right half
    }
  }

  // If not exact match found, return the child pointer where key would be
  return ValueAt(left - 1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
// auua:貌似是已经创建好了节点，然后往里面填内容；又由于是新的根节点，那只会有两个元素
 /*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  // Set first entry (left child)
  SetValueAt(0, old_value);

  // Set second entry (right child)
  SetKeyAt(1, new_key);
  SetValueAt(1, new_value);

  // Update size to reflect the two entries
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int index = ValueIndex(old_value);
  if (index == -1) return GetSize();  // old_value not found

  // Shift elements after the insertion point
  if (index < GetSize() - 1) {
    void *dest = PairPtrAt(index + 2);
    void *src = PairPtrAt(index + 1);
    int bytes_to_move = (GetSize() - index - 1) * pair_size;
    memmove(dest, src, bytes_to_move);
  }

  // Insert new pair
  SetKeyAt(index + 1, new_key);
  SetValueAt(index + 1, new_value);
  IncreaseSize(1);
  return GetSize();
}


/*****************************************************************************
 * SPLIT
 *****************************************************************************/
// auua: 移动后一半
 /*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int current_size = GetSize();
  if (current_size <= 1) return;

  // Calculate split point (round down)
  int split_index = current_size / 2;
  int move_count = current_size - split_index;

  // Copy the latter half of entries to recipient
  recipient->CopyNFrom(PairPtrAt(split_index), move_count, buffer_pool_manager);

  SetSize(split_index);
}

//auua:和leaf一样，也是直接放到了最后，后面有问题再改吧（
/* 
 * Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes
 * to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with 
 * BufferPoolManger
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  if (size <= 0) return;
  ASSERT(GetSize() + size <= GetMaxSize(), "Exceeds page capacity");
  
  int dest_index = GetSize();
  memcpy(PairPtrAt(dest_index), src, size * pair_size);

  // Update parent pointers for all moved child pages
  for (int i = 0; i < size; i++) {
    page_id_t child_page_id = ValueAt(dest_index + i);
    auto *child_page = buffer_pool_manager->FetchPage(child_page_id);

    if (child_page != nullptr) {
      auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_node->SetParentPageId(GetPageId());
      //unpin
      buffer_pool_manager->UnpinPage(child_page_id, true);
    }
  }

  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  //if not the last one
  if(index < GetSize()-1)
    memmove(PairPtrAt(index), PairPtrAt(index+1), (GetSize()-index-1)*pair_size);
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() { 
  IncreaseSize(-1);
  return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
// auua: 和叶子节点不同，内部节点合并是右边的并到左边的尾部
 /*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  if(GetSize()<=0) return;

  // copy
  int move_num = GetSize();
  int dest_index = recipient->GetSize();
  memcpy(recipient->PairPtrAt(dest_index), PairPtrAt(0), move_num*pair_size);
  recipient->SetKeyAt(dest_index, middle_key);
  recipient->IncreaseSize(move_num);

  // change parent id
  page_id_t parent_id = recipient->GetPageId();
  for(int i=0; i<move_num; i++)
  {
    page_id_t child_page_id = ValueAt(i);
    auto *child_page = buffer_pool_manager->FetchPage(child_page_id);

    if (child_page != nullptr) {
      auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_node->SetParentPageId(parent_id);
      
      //unpin
      buffer_pool_manager->UnpinPage(child_page_id, true);
    }
  }

  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  int current_size = GetSize();
  if(current_size<=0) return;

  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);

  // Shift remaining elements left by one position
  if (current_size > 1) {
    memmove(PairPtrAt(0), PairPtrAt(1), (current_size - 1) * pair_size);
  }

  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int dest_index = GetSize();
  SetKeyAt(dest_index, key);
  SetValueAt(dest_index, value);

  auto *child_page = buffer_pool_manager->FetchPage(value);
  if(child_page!=nullptr)
  {
    auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
  }

  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  if(GetSize()<=0) return;

  recipient->CopyFirstFrom(middle_key, ValueAt(GetSize()-1), buffer_pool_manager);

  IncreaseSize(-1);
}

//auua:iii 这里我增加了一个key的接口，这样更对称...
/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(GenericKey *middle_key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  memmove(PairPtrAt(1), PairPtrAt(0), GetSize()*pair_size);
  SetValueAt(0, value);
  SetKeyAt(1, middle_key);

  auto *child_page = buffer_pool_manager->FetchPage(value);
  if(child_page != nullptr)
  {
    auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value,true);
  }

  IncreaseSize(1);
}