#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  int key_size = KM.GetKeySize();
  if (leaf_max_size == UNDEFINED_SIZE) {
    int pair_size = key_size + sizeof(RowId);
    leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / pair_size;
  }
  if (internal_max_size_ == UNDEFINED_SIZE) {
    int pair_size = key_size + sizeof(page_id_t);
    internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / pair_size;
  }
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  if (current_page_id == INVALID_PAGE_ID) {
    return;
  }

  // Fetch the current page from buffer pool
  Page *page = buffer_pool_manager_->FetchPage(current_page_id);
  if (page == nullptr) {
    LOG(ERROR) << "b_plus_tree::Destroy: page which need deleted can't fetch" << endl;
    return;  // Page not found or error occurred
  }

  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (!node->IsLeafPage()) {
    // If it's an internal page, recursively destroy all child pages
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    for (int i = 0; i < internal_node->GetSize(); ++i) {
      Destroy(internal_node->ValueAt(i));
    }
  }

  // Clean up the current page
  buffer_pool_manager_->UnpinPage(current_page_id, false);
  buffer_pool_manager_->DeletePage(current_page_id);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (root_page == nullptr) {
    return true;
  }

  BPlusTreePage *root_page_data = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
  bool is_empty = (root_page_data->GetSize() == 0);
  buffer_pool_manager_->UnpinPage(root_page_id_, false);

  return is_empty;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
// auua:result不能清空，因为测试是每次检查最后一个...
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if (IsEmpty()) return false;

  page_id_t page_id = root_page_id_;
  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) {
      return false;  // Failed to fetch page
    }
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      LeafPage *leaf = reinterpret_cast<LeafPage *>(node);
      RowId row_id;
      bool found = leaf->Lookup(key, row_id, processor_);

      buffer_pool_manager_->UnpinPage(page_id, false);

      if (found) {
        //result.clear();  // Ensure vector is clean before adding result
        result.push_back(row_id);
        return true;
      }

      return false;
    } else {
      InternalPage *internal = reinterpret_cast<InternalPage *>(node);
      page_id_t next_page_id = internal->Lookup(key, processor_);

      buffer_pool_manager_->UnpinPage(page_id, false);

      page_id = next_page_id;
    }
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  } else {
    return InsertIntoLeaf(key, value, transaction);
  }
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  // Allocate new page for root
  Page *new_root_page = buffer_pool_manager_->NewPage(root_page_id_);
  if (new_root_page == nullptr) {
    throw std::runtime_error("out of memory");
  }

  // Initialize new leaf page as root
  LeafPage *new_root_node = reinterpret_cast<LeafPage *>(new_root_page->GetData());
  new_root_node->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);

  // 不用insert方法，因为这样更快
  new_root_node->SetKeyAt(0, key);
  new_root_node->SetValueAt(0, value);
  new_root_node->SetSize(1);

  // Unpin the page after modifications
  new_root_node->SetNextPageId(INVALID_PAGE_ID);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  UpdateRootPageId(true);
}

// auua: 这里说实话违背了资源谁申请，谁释放的原则...因为findleafpage这么要求，那就只能这样了...
/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  // Find the appropriate leaf page
  Page *leaf_page = FindLeafPage(key, root_page_id_, false);

  // Found the leaf page - proceed with insertion
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // Check for duplicate key
  RowId existing_value;
  // if key is exist, return false
  if (leaf_node->Lookup(key, existing_value, processor_)) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return false;
  }

  // Insert the key-value pair into the leaf node
  if (leaf_node->GetSize() == leaf_node->GetMaxSize()) {
    // Split the leaf node if full
    LeafPage *new_leaf = Split(leaf_node, transaction);
    GenericKey *new_key = new_leaf->KeyAt(0);

    // Determine which leaf should contain the new key
    bool insert_in_new = (processor_.CompareKeys(key, new_key) >= 0);
    (insert_in_new ? new_leaf : leaf_node)->Insert(key, value, processor_);

    // Update parent pointers
    InsertIntoParent(leaf_node, new_key, new_leaf, transaction);
    buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  } else {
    leaf_node->Insert(key, value, processor_);
  }

  // Unpin the leaf node page
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  // Allocate new page from buffer pool
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);  // NewPage will new new_page_id
  if (new_page == nullptr) {
    throw std::runtime_error("out of memory");
  }

  // Initialize new internal page
  InternalPage *new_internal = reinterpret_cast<InternalPage *>(new_page->GetData());
  new_internal->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);

  // Split
  node->MoveHalfTo(new_internal, buffer_pool_manager_);

  return new_internal;
}

// auua:新的leafnode直接插入到node链表位置的下一个
BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  // Allocate new page from buffer pool
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    throw std::runtime_error("out of memory");
  }

  // Initialize new leaf page
  LeafPage *new_leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_leaf->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);

  // Calculate split point (move half the entries to new page)
  node->MoveHalfTo(new_leaf);

  // Update sibling pointers
  new_leaf->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_page_id);

  //buffer_pool_manager_->UnpinPage(new_page_id, true);
  return new_leaf;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  page_id_t parent_id = old_node->GetParentPageId();

  // Case 1: old_node was root, need to create new root
  if (old_node->IsRootPage()) {
    Page *root_page = buffer_pool_manager_->NewPage(root_page_id_);
    if (root_page == nullptr) {
      throw std::runtime_error("Out of memory while creating new root");
    }

    InternalPage *new_root = reinterpret_cast<InternalPage *>(root_page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);

    // Insert old and new nodes into the new root
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    // Update parent pointers
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    // Update root in buffer pool and header
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId(true);
    return;
  }

  // Case 2: Normal case - insert into existing parent
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  if (parent_page == nullptr) {
    throw std::runtime_error("Failed to fetch parent page");
  }

  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // Insert new key and node into parent
  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  new_node->SetParentPageId(parent_id);

  // Check if parent needs to be split
  if (parent_node->GetSize() > parent_node->GetMaxSize()) {
    InternalPage *new_parent_node = Split(parent_node, transaction);

    // Recursively insert into parent's parent
    InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction);

    buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (IsEmpty()) return;
  
  // Find the leaf page containing the key
  Page *leaf_page = FindLeafPage(key, root_page_id_, false);
  if (leaf_page == nullptr) return;

  LeafPage *leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());

  // Try to remove the key - if not found, just unpin and return
  int size = leaf_node->GetSize();
  if (size == leaf_node->RemoveAndDeleteRecord(key, processor_)) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return;
  }

  // Handle potential coalesce or redistribute
  bool node_deleted = CoalesceOrRedistribute(leaf_node, transaction);

  // If node wasn't deleted by CoalesceOrRedistribute, unpin it
  if (!node_deleted) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  }
}

// auua:资源申请原则：谁申请，谁释放
// auua:这是删除的善后处理工具，我改变了返回的逻辑（反正现在是返回了true，就是parent已经被删了，不用再释放了）
/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  // 如果满足B+树要求，不需要更改
  if (node->GetSize() >= node->GetMinSize()) return false;

  // If node is root, handle specially
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }

  // Get parent page
  page_id_t parent_id = node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  if (parent_page == nullptr) {
    throw std::runtime_error("Failed to fetch parent page");
  }
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // Find node's index in parent
  int node_index = parent->ValueIndex(node->GetPageId());
  int sibling_index = (node_index == 0) ? 1 : node_index - 1;
  page_id_t sibling_id = parent->ValueAt(sibling_index);

  // Fetch sibling page
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
  if (sibling_page == nullptr) {
    buffer_pool_manager_->UnpinPage(parent_id, false);
    throw std::runtime_error("Failed to fetch sibling page");
  }
  N *sibling = reinterpret_cast<N *>(sibling_page->GetData());

  // Decide whether to coalesce or redistribute
  bool finish_delete_node = false;
  bool parent_needs_Unpin = false;

  if (node->GetSize() + sibling->GetSize() <= node->GetMaxSize()) {
    // Coalesce the nodes
    bool parent_deleted = Coalesce(sibling, node, parent, node_index, transaction);

    // Handle parent page if it wasn't deleted
    if (!parent_deleted) {
      // Special case: parent is root with only one child
      if (parent->IsRootPage() && parent->GetSize() == 1) {
        AdjustInternalRoot(parent, node_index == 0 ? node : sibling);
      }
      parent_needs_Unpin = true;
    }

    // Mark node for deletion if it's the right sibling (index != 0)
    finish_delete_node = (node_index != 0);
  } else {
    // Redistribute entries between nodes
    Redistribute(sibling, node, node_index, parent);
    parent_needs_Unpin = true;
  }

  // Clean up resources
  if (parent_needs_Unpin) {
    buffer_pool_manager_->UnpinPage(parent_id, true);
  }
  // node被删，那么sibling就没被删，需要释放
  if (finish_delete_node) buffer_pool_manager_->UnpinPage(sibling_id, true);

  return finish_delete_node;
}

// auua: Coalesce是“合并”的意思
// auua: 除了node是第一个节点，其他情况都是右边的并到左边中
/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node has been deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  bool node_need_deleted = (index != 0);
  LeafPage *live_node = node_need_deleted ? neighbor_node : node;
  LeafPage *deleted_node = node_need_deleted ? node : neighbor_node;

  // Move all entries from right node to left node
  deleted_node->MoveAllTo(live_node);

  // Remove the right node from parent and delete it
  page_id_t old_page_id = deleted_node->GetPageId();
  parent->Remove(node_need_deleted ? index - 1 : index);
  buffer_pool_manager_->UnpinPage(old_page_id, false);
  buffer_pool_manager_->DeletePage(old_page_id);

  // Check if parent needs further adjustment
  return CoalesceOrRedistribute(parent, transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  // Determine which node is on the left/right
  bool node_need_deleted = (index != 0);
  InternalPage *live_node = node_need_deleted ? neighbor_node : node;
  InternalPage *deleted_node = node_need_deleted ? node : neighbor_node;

  // Get the separator key from parent
  GenericKey *separator_key = parent->KeyAt(node_need_deleted ? index : index+1);

  // Move all entries from right node to left node, including the separator key
  deleted_node->MoveAllTo(live_node, separator_key, buffer_pool_manager_);

  // Remove the right node from parent and delete it
  page_id_t old_page_id = deleted_node->GetPageId();
  parent->Remove(node_need_deleted ? index : index+1);
  buffer_pool_manager_->UnpinPage(old_page_id, false);
  buffer_pool_manager_->DeletePage(old_page_id);

  // Check if parent needs further adjustment
  return CoalesceOrRedistribute(parent, transaction);
}

// auua: 这里加了一个parent的接口，不然Redistribute涉及到的parent中的更改没法完成...
/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index, InternalPage *parent) {
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node);
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node);
    parent->SetKeyAt(index, node->KeyAt(0));
  }
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index, InternalPage *parent) {
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, parent->KeyAt(1),buffer_pool_manager_);
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node, parent->KeyAt(index),buffer_pool_manager_);
    parent->SetKeyAt(index, node->KeyAt(0));
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page has been deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  // Case 2: Empty tree - delete the root
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    page_id_t old_page_id = old_root_node->GetPageId();
    root_page_id_ = INVALID_PAGE_ID;
    buffer_pool_manager_->UnpinPage(old_page_id,false);
    buffer_pool_manager_->DeletePage(old_page_id);
    UpdateRootPageId(true);
    return true;
  }
  // Case 1: Root with only one child - promote the child
  // auua: 这种情况不应该在这里处理，因为child也需要修改，而child的资源不是在这里申请的
  else {
    return false;
  }
}

// auua: 这是我新增的辅助函数，用处就是让root被删，让node成为新的root
void BPlusTree::AdjustInternalRoot(InternalPage *root, BPlusTreePage *node) {
  root_page_id_ = node->GetPageId();
  page_id_t old_page_id = root->GetPageId();
  buffer_pool_manager_->UnpinPage(old_page_id, false);
  buffer_pool_manager_->DeletePage(old_page_id);
  node->SetParentPageId(INVALID_PAGE_ID);
  UpdateRootPageId(true);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() { 
  if (IsEmpty()) return IndexIterator();
  Page *leaf_page = FindLeafPage(nullptr, root_page_id_, true);
  LeafPage *leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  page_id_t t_page_id = leaf_node->GetPageId();
  buffer_pool_manager_->UnpinPage(t_page_id, true);
  return IndexIterator(t_page_id, buffer_pool_manager_, 0); 
}

// auua: 'low key' 似乎是范围查询的下界，所以找到对应leaf_node，然后通过KeyIndex()返回了第一个大于等于它的index
/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) { 
  if (IsEmpty()) return IndexIterator();
  Page *leaf_page = FindLeafPage(key, root_page_id_, false);
  LeafPage *leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  page_id_t t_page_id = leaf_node->GetPageId();
  int index = leaf_node->KeyIndex(key, processor_);
  buffer_pool_manager_->UnpinPage(t_page_id, true);
  return IndexIterator(t_page_id, buffer_pool_manager_, index); 
}

// auua: 我感觉这个设计很不合常理，end()不应该是个空值吗...
/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() { 
  if (IsEmpty()) return IndexIterator();
  Page *leaf_page = FindLeafPage(nullptr, root_page_id_, false, true);
  LeafPage *leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  page_id_t t_page_id = leaf_node->GetPageId();
  int index = leaf_node->GetSize()-1;
  buffer_pool_manager_->UnpinPage(t_page_id, true);
  return IndexIterator(t_page_id, buffer_pool_manager_, index); 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
// auua: 不能复用rightMost实在是有些可惜，所以补一下
 /*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost, bool rightMost) {
  // Find the appropriate leaf page
  page_id_t current_page_id = page_id;
  Page *current_page = nullptr;
  BPlusTreePage *current_node = nullptr;

  while (true) {
    current_page = buffer_pool_manager_->FetchPage(current_page_id);
    if (current_page == nullptr) {
      throw std::runtime_error("Failed to fetch page");
    }

    current_node = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
    if (current_node->IsLeafPage()) {
      return current_page;
    } else {
      // Internal node - continue searching
      InternalPage *internal_node = reinterpret_cast<InternalPage *>(current_node);

      page_id_t next_page_id;
      if (!(leftMost|rightMost))
        next_page_id = internal_node->Lookup(key, processor_);
      else if(leftMost)
        next_page_id = internal_node->ValueAt(0);
      else
        next_page_id = internal_node->ValueAt(internal_node->GetSize() - 1);

      buffer_pool_manager_->UnpinPage(current_page_id, false);
      current_page_id = next_page_id;
    }
  }
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page is defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  // Fetch the header page
  Page *header_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  if (header_page == nullptr) {
    throw std::runtime_error("Failed to fetch header page");
  }

  // Get the header page data
  IndexRootsPage *header = reinterpret_cast<IndexRootsPage *>(header_page->GetData());

  if (insert_record) {
    // Insert new record if it doesn't exist
    if (!header->Insert(index_id_, root_page_id_)) {
      // If insert fails (record exists), update instead
      header->Update(index_id_, root_page_id_);
    }
  } else {
    // Update existing record
    header->Update(index_id_, root_page_id_);
  }

  // Unpin the header page with dirty flag set
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}