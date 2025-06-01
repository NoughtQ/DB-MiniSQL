#include "catalog/catalog.h"

#include "common/instance.h"
#include "gtest/gtest.h"
#include "utils/utils.h"

#include <cstring>

static string db_file_name_extended = "catalog_extended_test.db";

// Test dropping tables and indexes, including error cases
TEST(CatalogExtendedTest, DropTableAndIndexTest) {
  auto db = new DBStorageEngine(db_file_name_extended, true);
  auto &catalog = db->catalog_mgr_;
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("score", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  Txn txn;
  TableInfo *table_info = nullptr;
  // Create table
  ASSERT_EQ(DB_SUCCESS, catalog->CreateTable("test_table", schema.get(), &txn, table_info));
  ASSERT_TRUE(table_info != nullptr);
  // Create index
  IndexInfo *index_info = nullptr;
  std::vector<std::string> index_keys{"id", "name"};
  ASSERT_EQ(DB_SUCCESS, catalog->CreateIndex("test_table", "test_index", index_keys, &txn, index_info, "bptree"));
  ASSERT_TRUE(index_info != nullptr);
  // Check table and index existence
  TableInfo *retrieved_table = nullptr;
  ASSERT_EQ(DB_SUCCESS, catalog->GetTable("test_table", retrieved_table));
  IndexInfo *retrieved_index = nullptr;
  ASSERT_EQ(DB_SUCCESS, catalog->GetIndex("test_table", "test_index", retrieved_index));
  // Drop index and check
  ASSERT_EQ(DB_SUCCESS, catalog->DropIndex("test_table", "test_index"));
  ASSERT_EQ(DB_INDEX_NOT_FOUND, catalog->GetIndex("test_table", "test_index", retrieved_index));
  // Drop non-existent index and table
  ASSERT_EQ(DB_INDEX_NOT_FOUND, catalog->DropIndex("test_table", "non_existent_index"));
  ASSERT_EQ(DB_TABLE_NOT_EXIST, catalog->DropIndex("non_existent_table", "test_index"));
  ASSERT_EQ(DB_SUCCESS, catalog->DropTable("test_table"));
  ASSERT_EQ(DB_TABLE_NOT_EXIST, catalog->GetTable("test_table", retrieved_table));
  ASSERT_EQ(DB_TABLE_NOT_EXIST, catalog->DropTable("non_existent_table"));
  delete db;
}

// Test batch creation and retrieval of tables and indexes
TEST(CatalogExtendedTest, BatchRetrievalTest) {
  auto db = new DBStorageEngine(db_file_name_extended, true);
  auto &catalog = db->catalog_mgr_;
  Txn txn;
  std::vector<std::string> table_names = {"table1", "table2", "table3"};
  std::vector<TableInfo *> created_tables;
  for (const auto &name : table_names) {
    std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                     new Column("data", TypeId::kTypeChar, 32, 1, true, false)};
    auto schema = std::make_shared<Schema>(columns);
    TableInfo *table_info = nullptr;
    ASSERT_EQ(DB_SUCCESS, catalog->CreateTable(name, schema.get(), &txn, table_info));
    created_tables.push_back(table_info);
  }
  // Create multiple indexes for table1
  std::vector<std::string> index_names = {"index1", "index2", "index3"};
  for (const auto &idx_name : index_names) {
    IndexInfo *index_info = nullptr;
    std::vector<std::string> index_keys{"id"};
    ASSERT_EQ(DB_SUCCESS, catalog->CreateIndex("table1", idx_name, index_keys, &txn, index_info, "bptree"));
  }
  // Retrieve all tables and indexes
  std::vector<TableInfo *> all_tables;
  ASSERT_EQ(DB_SUCCESS, catalog->GetTables(all_tables));
  ASSERT_GE(all_tables.size(), 3);
  std::vector<IndexInfo *> table1_indexes;
  ASSERT_EQ(DB_SUCCESS, catalog->GetTableIndexes("table1", table1_indexes));
  ASSERT_GE(table1_indexes.size(), 3);
  // Try to get indexes for a non-existent table
  std::vector<IndexInfo *> non_existent_indexes;
  ASSERT_EQ(DB_TABLE_NOT_EXIST, catalog->GetTableIndexes("non_existent_table", non_existent_indexes));
  delete db;
}

// Test retrieving table by table_id
TEST(CatalogExtendedTest, GetTableByIdTest) {
  auto db = new DBStorageEngine(db_file_name_extended, true);
  auto &catalog = db->catalog_mgr_;
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("value", TypeId::kTypeFloat, 1, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  Txn txn;
  TableInfo *table_info = nullptr;
  ASSERT_EQ(DB_SUCCESS, catalog->CreateTable("id_test_table", schema.get(), &txn, table_info));
  ASSERT_TRUE(table_info != nullptr);
  table_id_t table_id = table_info->GetTableId();
  // Retrieve all tables and find by id
  std::vector<TableInfo *> all_tables;
  ASSERT_EQ(DB_SUCCESS, catalog->GetTables(all_tables));
  TableInfo *retrieved_table = nullptr;
  for (auto tb_info : all_tables) {
    if (tb_info != nullptr && tb_info->GetTableId() == table_id) {
      retrieved_table = tb_info;
      break;
    }
  }
  ASSERT_TRUE(retrieved_table != nullptr);
  ASSERT_EQ(table_info, retrieved_table);
  ASSERT_EQ("id_test_table", retrieved_table->GetTableName());
  // Check non-existent table_id
  bool non_existent_found = false;
  for (auto tb_info : all_tables) {
    if (tb_info != nullptr && tb_info->GetTableId() == 9999) {
      non_existent_found = true;
      break;
    }
  }
  ASSERT_FALSE(non_existent_found);
  delete db;
}

// Test error handling for duplicate names and invalid operations
TEST(CatalogExtendedTest, ErrorHandlingTest) {
  auto db = new DBStorageEngine(db_file_name_extended, true);
  auto &catalog = db->catalog_mgr_;
  Txn txn;
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false)};
  auto schema = std::make_shared<Schema>(columns);
  TableInfo *table_info1 = nullptr;
  TableInfo *table_info2 = nullptr;
  // Create duplicate table
  ASSERT_EQ(DB_SUCCESS, catalog->CreateTable("duplicate_table", schema.get(), &txn, table_info1));
  ASSERT_EQ(DB_TABLE_ALREADY_EXIST, catalog->CreateTable("duplicate_table", schema.get(), &txn, table_info2));
  // Try to create index on non-existent table
  IndexInfo *index_info = nullptr;
  std::vector<std::string> index_keys{"id"};
  ASSERT_EQ(DB_TABLE_NOT_EXIST,
            catalog->CreateIndex("non_existent_table", "test_index", index_keys, &txn, index_info, "bptree"));
  // Create duplicate index
  IndexInfo *index_info1 = nullptr;
  IndexInfo *index_info2 = nullptr;
  ASSERT_EQ(DB_SUCCESS,
            catalog->CreateIndex("duplicate_table", "duplicate_index", index_keys, &txn, index_info1, "bptree"));
  ASSERT_EQ(DB_INDEX_ALREADY_EXIST,
            catalog->CreateIndex("duplicate_table", "duplicate_index", index_keys, &txn, index_info2, "bptree"));
  // Try to create index with non-existent column
  std::vector<std::string> bad_keys{"non_existent_column"};
  IndexInfo *bad_index = nullptr;
  ASSERT_EQ(DB_COLUMN_NAME_NOT_EXIST,
            catalog->CreateIndex("duplicate_table", "bad_index", bad_keys, &txn, bad_index, "bptree"));
  delete db;
}

// Test complex scenario with multiple tables, indexes, and persistence
TEST(CatalogExtendedTest, ComplexScenarioAndPersistenceTest) {
  // Phase 1: Create complex tables and indexes
  {
    auto db = new DBStorageEngine(db_file_name_extended, true);
    auto &catalog = db->catalog_mgr_;
    Txn txn;
    // Create students table
    std::vector<Column *> student_columns = {new Column("student_id", TypeId::kTypeInt, 0, false, false),
                                             new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                             new Column("age", TypeId::kTypeInt, 2, true, false),
                                             new Column("gpa", TypeId::kTypeFloat, 3, true, false)};
    auto student_schema = std::make_shared<Schema>(student_columns);
    TableInfo *student_table = nullptr;
    ASSERT_EQ(DB_SUCCESS, catalog->CreateTable("students", student_schema.get(), &txn, student_table));
    // Create courses table
    std::vector<Column *> course_columns = {new Column("course_id", TypeId::kTypeInt, 0, false, false),
                                            new Column("course_name", TypeId::kTypeChar, 128, 1, true, false),
                                            new Column("credits", TypeId::kTypeInt, 2, true, false)};
    auto course_schema = std::make_shared<Schema>(course_columns);
    TableInfo *course_table = nullptr;
    ASSERT_EQ(DB_SUCCESS, catalog->CreateTable("courses", course_schema.get(), &txn, course_table));
    // Create indexes for students table
    IndexInfo *student_id_index = nullptr;
    std::vector<std::string> student_id_keys{"student_id"};
    ASSERT_EQ(DB_SUCCESS,
              catalog->CreateIndex("students", "idx_student_id", student_id_keys, &txn, student_id_index, "bptree"));
    IndexInfo *student_name_index = nullptr;
    std::vector<std::string> student_name_keys{"name"};
    ASSERT_EQ(DB_SUCCESS, catalog->CreateIndex("students", "idx_student_name", student_name_keys, &txn,
                                               student_name_index, "bptree"));
    IndexInfo *student_composite_index = nullptr;
    std::vector<std::string> student_composite_keys{"age", "gpa"};
    ASSERT_EQ(DB_SUCCESS, catalog->CreateIndex("students", "idx_age_gpa", student_composite_keys, &txn,
                                               student_composite_index, "bptree"));
    // Create index for courses table
    IndexInfo *course_id_index = nullptr;
    std::vector<std::string> course_id_keys{"course_id"};
    ASSERT_EQ(DB_SUCCESS,
              catalog->CreateIndex("courses", "idx_course_id", course_id_keys, &txn, course_id_index, "bptree"));
    // Check all tables and indexes
    std::vector<TableInfo *> all_tables;
    ASSERT_EQ(DB_SUCCESS, catalog->GetTables(all_tables));
    ASSERT_GE(all_tables.size(), 2);
    std::vector<IndexInfo *> student_indexes;
    ASSERT_EQ(DB_SUCCESS, catalog->GetTableIndexes("students", student_indexes));
    ASSERT_GE(student_indexes.size(), 3);
    std::vector<IndexInfo *> course_indexes;
    ASSERT_EQ(DB_SUCCESS, catalog->GetTableIndexes("courses", course_indexes));
    ASSERT_GE(course_indexes.size(), 1);
    delete db;
  }
  // Phase 2: Test persistence - reload database
  {
    auto db = new DBStorageEngine(db_file_name_extended, false);
    auto &catalog = db->catalog_mgr_;
    // Check tables and indexes after reload
    TableInfo *student_table = nullptr;
    ASSERT_EQ(DB_SUCCESS, catalog->GetTable("students", student_table));
    ASSERT_TRUE(student_table != nullptr);
    ASSERT_EQ("students", student_table->GetTableName());
    TableInfo *course_table = nullptr;
    ASSERT_EQ(DB_SUCCESS, catalog->GetTable("courses", course_table));
    ASSERT_TRUE(course_table != nullptr);
    ASSERT_EQ("courses", course_table->GetTableName());
    IndexInfo *student_id_index = nullptr;
    ASSERT_EQ(DB_SUCCESS, catalog->GetIndex("students", "idx_student_id", student_id_index));
    ASSERT_TRUE(student_id_index != nullptr);
    IndexInfo *course_id_index = nullptr;
    ASSERT_EQ(DB_SUCCESS, catalog->GetIndex("courses", "idx_course_id", course_id_index));
    ASSERT_TRUE(course_id_index != nullptr);
    IndexInfo *composite_index = nullptr;
    ASSERT_EQ(DB_SUCCESS, catalog->GetIndex("students", "idx_age_gpa", composite_index));
    ASSERT_TRUE(composite_index != nullptr);
    // Test drop operations after reload
    ASSERT_EQ(DB_SUCCESS, catalog->DropIndex("students", "idx_student_name"));
    ASSERT_EQ(DB_SUCCESS, catalog->DropTable("courses"));
    IndexInfo *deleted_index = nullptr;
    ASSERT_EQ(DB_INDEX_NOT_FOUND, catalog->GetIndex("students", "idx_student_name", deleted_index));
    TableInfo *deleted_table = nullptr;
    ASSERT_EQ(DB_TABLE_NOT_EXIST, catalog->GetTable("courses", deleted_table));
    delete db;
  }
}