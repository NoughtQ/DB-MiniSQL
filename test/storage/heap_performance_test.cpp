#include "storage/table_heap.h"

#include <unordered_map>
#include <vector>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "heap_performance_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, HeapPerformanceTest) {
    // 初始化测试实例，设置较小的缓冲池大小
    remove(db_file_name.c_str());
    auto disk_mgr = new DiskManager(db_file_name);
    auto bpm = new BufferPoolManager(10, disk_mgr);
    const int row_nums = 1000;
    
    // 创建 schema
    std::vector<Column *> columns = {
        new Column("id", TypeId::kTypeInt, 0, false, false),
        new Column("name", TypeId::kTypeChar, 64, 1, true, false),
        new Column("score", TypeId::kTypeFloat, 2, true, false)
    };
    auto schema = std::make_shared<Schema>(columns);
    
    // 创建表堆
    TableHeap *table_heap = TableHeap::Create(bpm, schema.get(), nullptr, nullptr, nullptr);
    std::unordered_map<int64_t, Fields *> row_values;
    std::vector<RowId> row_ids;
    
    // 第一阶段：大规模数据插入
    LOG(INFO) << "开始插入 " << row_nums << " 条数据";
    int current_page = -1;
    for (int i = 0; i < row_nums; i++) {
        int32_t len = RandomUtils::RandomInt(0, 64);
        char *name = new char[len];
        RandomUtils::RandomString(name, len);
        Fields *fields = new Fields{
            Field(TypeId::kTypeInt, i),
            Field(TypeId::kTypeChar, name, len, true),
            Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))
        };
        Row row(*fields);
        ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
        row_values.emplace(row.GetRowId().Get(), fields);
        row_ids.push_back(row.GetRowId());
        if (current_page != row.GetRowId().GetPageId()) {
            current_page = row.GetRowId().GetPageId();
            LOG(INFO) << "插入第 " << i << " 条数据，当前页面：" << current_page;
        }
        delete[] name;
    }
    LOG(INFO) << "插入完成，开始验证数据正确性";
    
    // 第二阶段：验证插入的数据
    ASSERT_EQ(row_nums, row_values.size());
    for (const auto &row_kv : row_values) {
        Row row(RowId(row_kv.first));
        ASSERT_TRUE(table_heap->GetTuple(&row, nullptr));
        ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
        for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
            ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
        }
    }
    LOG(INFO) << "数据验证完成，开始更新操作";
    
    // 第三阶段：更新所有数据
    int i = 0;
    for (auto it = row_ids.begin(); it != row_ids.end(); ) {
        char *new_name = new char[64];
        RandomUtils::RandomString(new_name, 64);
        Fields *new_fields = new Fields{
            Field(TypeId::kTypeInt, row_nums + (i++)),  // 更新id
            Field(TypeId::kTypeChar, new_name, 64, true),  // 更新name
            Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(0.f, 1000.f))  // 更新score
        };
        Row new_row(*new_fields);
        RowId old_row_id = *it;
        ASSERT_TRUE(table_heap->UpdateTuple(new_row, old_row_id, nullptr));
        RowId new_row_id = new_row.GetRowId();
        
        // 更新记录的值
        row_values.erase(old_row_id.Get());
        row_values.emplace(new_row_id.Get(), new_fields);
        if (old_row_id == new_row_id) {
            it++;
        } else {
            it = row_ids.erase(it);
            row_ids.push_back(new_row_id);
        }
        delete[] new_name;
    }
    LOG(INFO) << "更新完成，开始验证更新后的数据";
    
    // 第四阶段：验证更新后的数据
    for (const auto &row_kv : row_values) {
        Row row(RowId(row_kv.first));
        ASSERT_TRUE(table_heap->GetTuple(&row, nullptr));
        ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
        for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
            ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
        }
    }
    LOG(INFO) << "更新后的数据验证完成，开始删除操作";
    
    // 第五阶段：删除一半数据
    size_t delete_count = row_ids.size() / 2;
    for (size_t i = 0; i < delete_count; i++) {
        ASSERT_TRUE(table_heap->MarkDelete(row_ids[i], nullptr));
        table_heap->ApplyDelete(row_ids[i], nullptr);
        delete row_values[row_ids[i].Get()];
        row_values.erase(row_ids[i].Get());
    }
    LOG(INFO) << "删除完成，开始使用迭代器验证剩余数据";
    
    // 第六阶段：使用迭代器验证剩余数据
    size_t count = 0;
    for (auto iter = table_heap->Begin(nullptr); iter != table_heap->End(); ++iter) {
        const Row &row = *iter;
        ASSERT_TRUE(row_values.find(row.GetRowId().Get()) != row_values.end());
        for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
            ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_values[row.GetRowId().Get()]->at(j)));
        }
        count++;
    }
    ASSERT_EQ(row_values.size(), count);
    LOG(INFO) << "迭代器验证完成";
    
    // 清理内存
    for (auto &kv : row_values) {
        delete kv.second;
    }
    delete table_heap;
    delete disk_mgr;
    delete bpm;
}