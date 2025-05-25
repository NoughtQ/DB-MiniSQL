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
  // total size = magic num(4) + size of table meta pages(4) + size of index meta pages(4)
  //              + size of table meta pages * (table_id(4) + table_heap_page_id(4))
  //              + size of index meta pages * (index_id(4) + index_heap_page_id(4))
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

  if (init) {
    // initialize(create) the catalog meta
    catalog_meta_ = CatalogMeta::NewInstance();
    // catalog_meta_->SerializeTo(buf);
  } else {
    // fetch catalog meta page from the disk
    Page* page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    char* buf = page->GetData();
    // load the catalog meta from the buffer pool
    catalog_meta_ = catalog_meta_->DeserializeFrom(buf);
    // load table meta pages
    for (const auto& table_meta_page: catalog_meta_->table_meta_pages_) {
      LoadTable(table_meta_page.first, table_meta_page.second);
    }
    // load index meta pages
    for (const auto& index_meta_page: catalog_meta_->index_meta_pages_) {
      LoadIndex(index_meta_page.first, index_meta_page.second);
    }
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);
  }
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
    // check if the table with same name exists
    if (table_names_.count(table_name) == 1)
      return DB_TABLE_ALREADY_EXIST;

    table_id_t table_id = catalog_meta_->GetNextTableId();    // get new table id
    TableMetadata *table_meta = nullptr;
    TableHeap *table_heap = nullptr;
    TableSchema *table_schema = nullptr;
    page_id_t table_page_id, page_id;
    Page *table_page = nullptr, *table_meta_page = nullptr, *catalog_meta_page = nullptr;
    char *buf;

    // get new pages from buffer pool for table data
    table_page = buffer_pool_manager_->NewPage(table_page_id);
    table_meta_page = buffer_pool_manager_->NewPage(page_id);
    
    // deep copy from passed schema
    table_schema = table_schema->DeepCopySchema(schema);
  
    // create and initialize new table info
    table_meta = table_meta->Create(table_id, table_name, table_page_id, table_schema);
    table_meta->SerializeTo(table_meta_page->GetData());
    buffer_pool_manager_->UnpinPage(page_id, true);
    table_heap = table_heap->Create(buffer_pool_manager_, table_schema, txn, log_manager_, lock_manager_);
    table_info = table_info->Create();
    table_info->Init(table_meta, table_heap);

    // update catalog manager
    table_names_[table_name] = table_id;
    tables_[table_id] = table_info;

    // update catalog metadata
    catalog_meta_->table_meta_pages_[table_id] = page_id;
    catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    buf = catalog_meta_page->GetData();
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
  // check if the specified table exists
  if (table_names_.count(table_name) == 0)
    return DB_TABLE_NOT_EXIST;
  
  // get table info from catalog manager
  table_id_t table_id = table_names_[table_name];
  table_info = tables_[table_id];

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // if (tables.size() == 0)
  //   return DB_FAILED;

  // ensure that variable tables has enough space for all tables in db
  tables.resize(tables_.size());
  tables.clear();
  // get all table info
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
    // check if the specified table exists
    if (table_names_.count(table_name) == 0)
      return DB_TABLE_NOT_EXIST;
    
    // check if the index with same name exists
    if (index_names_.count(table_name) == 1) {
      auto index_list = index_names_[table_name];
      if (index_list.count(index_name) == 1) 
        return DB_INDEX_ALREADY_EXIST;
    }

    table_id_t table_id = table_names_[table_name];
    TableInfo *table_info = tables_[table_id];
    TableMetadata *table_meta = nullptr;
    TableHeap *table_heap = nullptr;
    TableSchema *table_schema = table_info->GetSchema();

    std::unordered_map<std::string, index_id_t> index_name_to_index;
    std::vector<uint32_t> key_map;
    index_id_t index_id = catalog_meta_->GetNextIndexId();
    IndexMetadata *index_meta = nullptr;
    IndexSchema *key_schema = nullptr;

    page_id_t index_page_id, page_id;
    Page *index_page, *index_meta_page, *catalog_meta_page;
    char *buf;

    // get new pages from buffer pool for index data
    // index_page = buffer_pool_manager_->NewPage(index_page_id);
    index_meta_page = buffer_pool_manager_->NewPage(page_id);
    
    // obtain key_map
    uint32_t col_index;
    for (const auto& col_name : index_keys) {
      if (table_schema->GetColumnIndex(col_name, col_index) == DB_COLUMN_NAME_NOT_EXIST)
        return DB_COLUMN_NAME_NOT_EXIST;
      key_map.push_back(col_index);
    }

    // create and initialize index info
    index_meta = index_meta->Create(index_id, index_name, table_id, key_map);
    index_meta->SerializeTo(index_meta_page->GetData());
    buffer_pool_manager_->UnpinPage(page_id, true);
    index_info = index_info->Create();
    index_info->Init(index_meta, table_info, buffer_pool_manager_);

    // update catalog manager
    // If the table does not have any indexes yet, create a new map for it
    if (index_names_.find(table_name) == index_names_.end()) {
      index_names_[table_name] = std::unordered_map<std::string, index_id_t>();
    }
    index_names_[table_name][index_name] = index_id;
    indexes_[index_id] = index_info;

    // update catalog metadata
    catalog_meta_->index_meta_pages_[index_id] = page_id;
    catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    buf = catalog_meta_page->GetData();
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
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // check if the specified table exists
  if (index_names_.count(table_name) == 0)
    return DB_TABLE_NOT_EXIST;
  
  // check if the specified index exists
  auto index_list = index_names_.at(table_name);
  if (index_list.count(index_name) == 0) 
    return DB_INDEX_NOT_FOUND;

  // get index info from catalog manager
  index_id_t index_id = index_names_.at(table_name).at(index_name);
  index_info = indexes_.at(index_id);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // check if the specified table exists
  if (index_names_.count(table_name) == 0)
    return DB_TABLE_NOT_EXIST;

  // ensure that variable indexes has enough space for all indexes in the table
  indexes.resize(indexes_.size());
  indexes.clear();
  // get all index info in the table
  auto index_list = index_names_.at(table_name);
  for (const auto& index : index_list)
    indexes.push_back(indexes_.at(index.second));

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // cannot delete non-existent table
  if (table_names_.count(table_name) == 0)
    return DB_TABLE_NOT_EXIST;

  // delete the entry from table_names_
  table_id_t table_id = table_names_[table_name];
  table_names_.erase(table_name);

  // exception
  if (tables_.count(table_id) == 0)
    return DB_FAILED;

  // delete and deallocate table info
  // page_id_t page_id = catalog_meta_->table_meta_pages_[table_id];
  // buffer_pool_manager_->UnpinPage(page_id, true);
  // buffer_pool_manager_->DeletePage(page_id);

  TableInfo* table = tables_[table_id];
  tables_.erase(table_id);
  catalog_meta_->table_meta_pages_.erase(table_id);
  
  delete table;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // cannot delete non-existent table and non-existent index
  if (table_names_.count(table_name) == 0)
    return DB_TABLE_NOT_EXIST;
  
  if (index_names_.count(table_name) == 1) {
    auto index_list = index_names_.at(table_name);
    if (index_list.count(index_name) == 0)
      return DB_INDEX_NOT_FOUND;
  } else {
    return DB_FAILED;
  }

  // delete the entry from index_names_
  index_id_t index_id = index_names_[table_name][index_name];
  index_names_[table_name].erase(index_name);

  // exception
  if (indexes_.count(index_id) == 0)
    return DB_FAILED;

  // delete and deallocate index info
  // page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
  // buffer_pool_manager_->UnpinPage(page_id, true);
  // buffer_pool_manager_->DeletePage(page_id);

  IndexInfo *index_info = indexes_[index_id];
  indexes_.erase(index_id);
  catalog_meta_->index_meta_pages_.erase(index_id);

  delete index_info;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // do as the method name indicates
  if (buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID))
    return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  try {
    // cannot load non-existent table
    if (tables_.count(table_id) != 0)
      return DB_TABLE_ALREADY_EXIST;

    TableMetadata *table_meta = nullptr;
    TableInfo *table_info = nullptr;
    TableHeap *table_heap = nullptr;
    TableSchema *table_schema = nullptr;
    page_id_t table_page_id;
    Page *page;
    char *buf;

    // load page from disk
    page = buffer_pool_manager_->FetchPage(page_id);
    buf = page->GetData();
    
    // obtain table metadata
    table_meta->DeserializeFrom(buf, table_meta);

    // create table info
    table_page_id = table_meta->GetFirstPageId();
    table_schema = table_meta->GetSchema();
    table_heap = table_heap->Create(buffer_pool_manager_, table_page_id, table_schema, log_manager_, lock_manager_);
    table_info = table_info->Create();
    table_info->Init(table_meta, table_heap);
    
    // update catalog manager
    std::string table_name = table_meta->GetTableName();
    table_names_[table_name] = table_id;
    tables_[table_id] = table_info;

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
    // cannot load non-existent index
    if (indexes_.count(index_id) != 0)
      return DB_INDEX_ALREADY_EXIST;

    table_id_t table_id;
    TableMetadata *table_meta = nullptr;
    TableInfo *table_info = nullptr;

    IndexMetadata *index_meta = nullptr;
    IndexInfo *index_info = nullptr;

    page_id_t index_page_id, table_page_id;
    Page *page;
    char *buf;

    // load page from disk
    page = buffer_pool_manager_->FetchPage(page_id);
    buf = page->GetData();
    
    // obtain index metadata
    index_meta->DeserializeFrom(buf, index_meta);

    // create index info
    table_id = index_meta->GetTableId();
    table_info = tables_[table_id];
    index_info = index_info->Create();
    index_info->Init(index_meta, table_info, buffer_pool_manager_);

    // update catalog manager
    std::string index_name = index_meta->GetIndexName();
    std::string table_name = table_info->GetTableName();
    // If the table does not have any indexes yet in index_names_ (e.g., during loading), create a new map for it
    if (index_names_.find(table_name) == index_names_.end()) {
      index_names_[table_name] = std::unordered_map<std::string, index_id_t>();
    }
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

  // get table info directly by table id
  table_info = tables_[table_id];

  return DB_SUCCESS;
}