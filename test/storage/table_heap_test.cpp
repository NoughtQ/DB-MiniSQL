#include "storage/table_heap.h"

#include <unordered_map>
#include <vector>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 10000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      row_values.emplace(row.GetRowId().Get(), fields);
      size++;
    }
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);
  for (auto row_kv : row_values) {
    size--;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  ASSERT_EQ(size, 0);
}

TEST(TableHeapTest, TableHeapDeleteTest) {
  // 初始化测试实例
  remove(db_file_name.c_str());
  auto disk_mgr = new DiskManager(db_file_name);
  auto bpm = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr);
  
  // 创建 schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  
  // 创建表堆并插入数据
  TableHeap *table_heap = TableHeap::Create(bpm, schema.get(), nullptr, nullptr, nullptr);
  std::vector<RowId> row_ids;
  
  // 插入5条记录
  for (int i = 0; i < 5; i++) {
    char *name = new char[4]{"abc"};
    Fields *fields = new Fields{Field(TypeId::kTypeInt, i),
                               Field(TypeId::kTypeChar, name, 4, true)};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    row_ids.push_back(row.GetRowId());
    delete[] name;
    delete fields;
  }
  
  // 测试标记删除
  ASSERT_TRUE(table_heap->MarkDelete(row_ids[0], nullptr));
  ASSERT_TRUE(table_heap->MarkDelete(row_ids[2], nullptr));
  
  // 验证标记删除后不能读取数据
  Row row1(row_ids[0]);
  ASSERT_FALSE(table_heap->GetTuple(&row1, nullptr));
  Row row2(row_ids[2]);
  ASSERT_FALSE(table_heap->GetTuple(&row2, nullptr));
  
  // 应用删除
  table_heap->ApplyDelete(row_ids[0], nullptr);
  
  // 验证删除后无法读取数据
  Row row3(row_ids[0]);
  ASSERT_FALSE(table_heap->GetTuple(&row3, nullptr));
  
  // 回滚删除
  table_heap->RollbackDelete(row_ids[2], nullptr);
  
  // 验证回滚后可以读取数据
  Row row4(row_ids[2]);
  ASSERT_TRUE(table_heap->GetTuple(&row4, nullptr));
  ASSERT_EQ(CmpBool::kTrue, row4.GetField(0)->CompareEquals(Field(TypeId::kTypeInt, 2)));
  ASSERT_EQ(CmpBool::kTrue, row4.GetField(1)->CompareEquals(Field(TypeId::kTypeChar, "abc", 4, true)));
}

TEST(TableHeapTest, TableHeapUpdateTest) {
  // 初始化测试实例
  remove(db_file_name.c_str());
  auto disk_mgr = new DiskManager(db_file_name);
  auto bpm = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr);
  
  // 创建 schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  
  // 创建表堆并插入数据
  TableHeap *table_heap = TableHeap::Create(bpm, schema.get(), nullptr, nullptr, nullptr);
  
  // 插入一条记录
  char *name = new char[4]{"abc"};
  Fields *fields = new Fields{Field(TypeId::kTypeInt, 1),
                             Field(TypeId::kTypeChar, name, 4, true)};
  Row row(*fields);
  ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
  RowId rid = row.GetRowId();
  delete[] name;
  delete fields;
  
  // 更新记录（原地更新）
  char *new_name1 = new char[4]{"def"};
  Fields *new_fields1 = new Fields{Field(TypeId::kTypeInt, 1),
                                  Field(TypeId::kTypeChar, new_name1, 4, true)};
  Row new_row1(*new_fields1);
  ASSERT_TRUE(table_heap->UpdateTuple(new_row1, rid, nullptr));
  delete[] new_name1;
  delete new_fields1;
  
  // 验证更新结果
  Row check_row1(rid);
  ASSERT_TRUE(table_heap->GetTuple(&check_row1, nullptr));
  ASSERT_EQ(check_row1.GetField(1)->GetLength(), 4);
  ASSERT_EQ(memcmp(check_row1.GetField(1)->GetData(), "def", 4), 0);
  
  // 更新记录（删除重插入）
  char *new_name2 = new char[64]{"this is a very long string that cannot be updated in place"};
  Fields *new_fields2 = new Fields{Field(TypeId::kTypeInt, 1),
                                  Field(TypeId::kTypeChar, new_name2, 64, true)};
  Row new_row2(*new_fields2);
  ASSERT_TRUE(table_heap->UpdateTuple(new_row2, rid, nullptr));
  delete[] new_name2;
  delete new_fields2;
  
  // 验证更新结果
  Row check_row2(rid);
  ASSERT_TRUE(table_heap->GetTuple(&check_row2, nullptr));
  ASSERT_EQ(check_row2.GetField(1)->GetLength(), 64);
}

TEST(TableHeapTest, TableIteratorTest) {
  // 初始化测试实例
  remove(db_file_name.c_str());
  auto disk_mgr = new DiskManager(db_file_name);
  auto bpm = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr);
  
  // 创建 schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false)};
  auto schema = std::make_shared<Schema>(columns);
  
  // 创建表堆
  TableHeap *table_heap = TableHeap::Create(bpm, schema.get(), nullptr, nullptr, nullptr);
  
  // 测试空表的迭代器
  ASSERT_EQ(table_heap->Begin(nullptr), table_heap->End());
  
  // 插入数据
  std::vector<int> values{1, 2, 3, 4, 5};
  std::vector<RowId> row_ids;
  for (int value : values) {
    Fields *fields = new Fields{Field(TypeId::kTypeInt, value)};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    // LOG(INFO) << "insert rid: " << row.GetRowId().GetPageId() << " " << row.GetRowId().GetSlotNum() << std::endl;
    row_ids.push_back(row.GetRowId());
    delete fields;
  }
  
  // 测试迭代器遍历
  int expected_value = 1;
  printf("1\n");
  auto iter = table_heap->Begin(nullptr);
  ASSERT_NE(iter, table_heap->End());
  for (iter = table_heap->Begin(nullptr); iter != table_heap->End(); ++iter) {
    Row row = *iter;
    // LOG(INFO) << "iter rid: " << row.GetRowId().GetPageId() << " " << row.GetRowId().GetSlotNum() << std::endl;
    ASSERT_EQ(row.GetFields().size(), 1);
    Field expected_field(TypeId::kTypeInt, expected_value);
    ASSERT_EQ(CmpBool::kTrue, (*iter).GetField(0)->CompareEquals(expected_field));
    expected_value++;
  }
  
  // 测试迭代器拷贝构造和赋值
  printf("2\n");
  auto iter1 = table_heap->Begin(nullptr);
  auto iter2(iter1);  // 拷贝构造
  ASSERT_EQ(CmpBool::kTrue, (*iter1).GetField(0)->CompareEquals(*(*iter2).GetField(0)));
  
  printf("3\n");
  auto iter3 = table_heap->Begin(nullptr);
  iter3 = iter1;  // 赋值操作
  ASSERT_EQ(CmpBool::kTrue, (*iter1).GetField(0)->CompareEquals(*(*iter3).GetField(0)));
  
  // 测试迭代器比较操作
  printf("4\n");
  auto begin = table_heap->Begin(nullptr);
  auto end = table_heap->End();
  ASSERT_NE(begin, end);
  
  // 测试后缀自增操作
  auto iter4 = table_heap->Begin(nullptr);
  auto iter5 = iter4++;
//   ASSERT_EQ((*iter5).GetField(0)->GetInt(), 1);
//   ASSERT_EQ((*iter4).GetField(0)->GetInt(), 2);
  ASSERT_EQ(CmpBool::kTrue, (*iter5).GetField(0)->CompareEquals(Field(TypeId::kTypeInt, 1)));
  ASSERT_EQ(CmpBool::kTrue, (*iter4).GetField(0)->CompareEquals(Field(TypeId::kTypeInt, 2)));
}
