#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t serialized_size = 12;
  uint32_t table_meta_pages_size = table_meta_pages_.size();
  uint32_t index_meta_pages_size = index_meta_pages_.size();

  serialized_size += 8 * (table_meta_pages_size + index_meta_pages_size);

  return serialized_size;
}

CatalogMeta::CatalogMeta(): table_meta_pages_(), index_meta_pages_() {
  table_meta_pages_.clear();
  index_meta_pages_.clear();
}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  table_names_.clear();
  tables_.clear();
  index_names_.clear();
  indexes_.clear();
  Page* page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
  char* buf = page->GetData();

  if (init) {
    catalog_meta_ = CatalogMeta::NewInstance();
    catalog_meta_->SerializeTo(buf);
  } else {
    catalog_meta_ = catalog_meta_->DeserializeFrom(buf);
    for (const auto& table_meta_page: catalog_meta_->table_meta_pages_)
      LoadTable(table_meta_page.first, table_meta_page.second);
    for (const auto& index_meta_page: catalog_meta_->index_meta_pages_)
      LoadIndex(index_meta_page.first, index_meta_page.second);
  }

  buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, true);
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  try {
    if (table_names_.count(table_name) == 1)
      return DB_TABLE_ALREADY_EXIST;

    table_id_t table_id = catalog_meta_->GetNextTableId();
    TableMetadata *table_meta;
    TableHeap *table_heap;
    TableSchema *table_schema = table_schema->DeepCopySchema(schema);
    page_id_t table_page_id = 0, page_id = 0;
    Page *table_page, *page1, *page2;
    char *buf;

    table_page = buffer_pool_manager_->NewPage(table_page_id);
    page1 = buffer_pool_manager_->NewPage(page_id);
    
    // Initialize table info
    table_meta = table_meta->Create(table_id, table_name, table_page_id, table_schema);
    table_meta->SerializeTo(page1->GetData());
    table_heap = table_heap->Create(buffer_pool_manager_, table_schema, txn, log_manager_, lock_manager_);
    table_info = table_info->Create();
    table_info->Init(table_meta, table_heap);

    // Update catalog manager
    table_names_[table_name] = table_id;
    tables_[table_id] = table_info;

    // Initialize catalog metadata
    catalog_meta_->table_meta_pages_[table_id] = page_id;
    page2 = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    buf = page2->GetData();
    catalog_meta_->SerializeTo(buf);
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

    return DB_SUCCESS;
  } catch (exception e) {
    return DB_FAILED;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.count(table_name) == 0)
    return DB_TABLE_NOT_EXIST;
  
  table_id_t table_id = table_names_[table_name];
  table_info = tables_[table_id];

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if (tables.size() == 0)
    return DB_FAILED;

  tables.resize(tables_.size());

  for (const auto& table : tables_)
    tables.push_back(table.second);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  try {
    if (table_names_.count(table_name) == 0)
      return DB_TABLE_NOT_EXIST;
    
    if (index_names_.count(table_name) == 1) {
      auto index_list = index_names_[table_name];
      if (index_list.count(index_name) == 1) 
        return DB_INDEX_ALREADY_EXIST;
    } else {
      return DB_FAILED;
    }

    table_id_t table_id = table_names_[table_name];
    TableInfo *table_info = tables_[table_id];
    TableMetadata *table_meta ;
    TableHeap *table_heap;
    TableSchema *table_schema = table_info->GetSchema();

    std::unordered_map<std::string, index_id_t> index_name_to_index;
    std::vector<uint32_t> key_map;
    index_id_t index_id = catalog_meta_->GetNextIndexId();
    IndexMetadata *index_meta;
    IndexSchema *key_schema;

    page_id_t index_page_id = 0, table_page_id = 0, page_id = 0;
    Page *index_page, *page1, *page2;
    char *buf;

    index_page = buffer_pool_manager_->NewPage(index_page_id);
    page1 = buffer_pool_manager_->NewPage(page_id);
    
    // Obtain key_map
    uint32_t col_index;
    for (const auto& col_name : index_keys) {
      if (table_schema->GetColumnIndex(col_name, col_index) == DB_COLUMN_NAME_NOT_EXIST)
        return DB_COLUMN_NAME_NOT_EXIST;
      key_map.push_back(col_index);
    }

    // Initialize index info
    index_meta = index_meta->Create(index_id, index_name, table_id, key_map);
    index_meta->SerializeTo(page1->GetData());
    index_info = index_info->Create();
    index_info->Init(index_meta, table_info, buffer_pool_manager_);

    // Update catalog manager
    index_name_to_index[index_name] = index_id;
    index_names_[table_name] = index_name_to_index;
    indexes_[index_id] = index_info;

    // Initialize catalog metadata
    catalog_meta_->index_meta_pages_[table_id] = page_id;
    page2 = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    buf = page2->GetData();
    catalog_meta_->SerializeTo(buf);
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

    return DB_SUCCESS;
  } catch (exception e) {
    return DB_FAILED;
  }  
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if (table_names_.count(table_name) == 0)
    return DB_TABLE_NOT_EXIST;
  
  if (index_names_.count(table_name) == 1) {
    auto index_list = index_names_.at(table_name);
    if (index_list.count(index_name) == 0) 
      return DB_INDEX_NOT_FOUND;
  } else {
    return DB_FAILED;
  }
  
  index_id_t index_id = index_names_.at(table_name).at(index_name);
  index_info = indexes_.at(index_id);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if (indexes.size() == 0)
    return DB_FAILED;

  if (table_names_.count(table_name) == 0)
    return DB_TABLE_NOT_EXIST;

  indexes.resize(indexes_.size());

  auto index_list = index_names_.at(table_name);
  for (const auto& index : index_list)
    indexes.push_back(indexes_.at(index.second));

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.count(table_name) == 0)
    return DB_TABLE_NOT_EXIST;

  table_id_t table_id = table_names_[table_name];
  table_names_.erase(table_name);

  if (tables_.count(table_id) == 0)
    return DB_FAILED;

  TableInfo* table = tables_[table_id];

  tables_.erase(table_id);
  table->~TableInfo();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (table_names_.count(table_name) == 0)
    return DB_TABLE_NOT_EXIST;
  
  if (index_names_.count(table_name) == 1) {
    auto index_list = index_names_.at(table_name);
    if (index_list.count(index_name) == 0) 
      return DB_INDEX_NOT_FOUND;
  } else {
    return DB_FAILED;
  }

  index_id_t index_id = index_names_[table_name][index_name];
  index_names_[table_name].erase(index_name);

  if (indexes_.count(index_id) == 0)
    return DB_FAILED;

  IndexInfo *index_info = indexes_[index_id];

  indexes_.erase(index_id);
  index_info->~IndexInfo();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  if (buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID))
    return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  try {
    if (tables_.count(table_id) == 0)
      return DB_TABLE_NOT_EXIST;

    TableMetadata *table_meta;
    TableInfo *table_info;
    TableHeap *table_heap;
    TableSchema *table_schema;
    page_id_t table_page_id;
    Page *page;
    char *buf;

    page = buffer_pool_manager_->FetchPage(page_id);
    buf = page->GetData();
    
    // Obtain table metadata
    table_meta->DeserializeFrom(buf, table_meta);
    // Create table info
    table_page_id = table_meta->GetFirstPageId();
    table_schema = table_meta->GetSchema();
    table_heap = table_heap->Create(buffer_pool_manager_, page_id, table_schema, log_manager_, lock_manager_);
    table_info = table_info->Create();
    table_info->Init(table_meta, table_heap);

    // Update catalog manager
    std::string table_name = table_meta->GetTableName();
    table_names_[table_name] = table_id;
    tables_[table_id] = table_info;

    buffer_pool_manager_->UnpinPage(page_id, false);

    return DB_SUCCESS;
  } catch (exception e) {
    return DB_FAILED;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  try {
    if (indexes_.count(index_id) == 0)
      return DB_INDEX_NOT_FOUND;

    table_id_t table_id;
    TableMetadata *table_meta;
    TableInfo *table_info;

    index_id_t index_id;
    IndexMetadata *index_meta;
    IndexInfo *index_info;

    page_id_t index_page_id = 0, table_page_id = 0, page_id = 0;
    Page *page;
    char *buf;

    page = buffer_pool_manager_->FetchPage(page_id);
    buf = page->GetData();
    
    // Obtain index metadata
    index_meta->DeserializeFrom(buf, index_meta);
    // Create index info
    table_id = index_meta->GetTableId();
    table_info = tables_[table_id];
    index_info = index_info->Create();
    index_info->Init(index_meta, table_info, buffer_pool_manager_);

    // Update catalog manager
    std::string index_name = index_meta->GetIndexName();
    std::string table_name = table_info->GetTableName();
    index_names_[table_name][index_name] = index_id;
    indexes_[index_id] = index_info;

    return DB_SUCCESS;
  } catch (exception e) {
    return DB_FAILED;
  }  
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if (tables_.count(table_id) == 0)
    return DB_TABLE_NOT_EXIST;

  table_info = tables_[table_id];

  return DB_SUCCESS;
}