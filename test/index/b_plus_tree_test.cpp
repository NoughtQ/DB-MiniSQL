#include "index/b_plus_tree.h"

#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  // auua: 增大key的大小，使1个page可以容纳的key数量减少
  std::vector<Column *> columns = {
      new Column("int1", TypeId::kTypeInt, 0, false, false),
      new Column("int2", TypeId::kTypeInt, 1, false, false),
      new Column("name", TypeId::kTypeChar, 64, 2, true, false)
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 128);
  BPlusTree tree(0, engine.bpm_, KP);
  TreeFileManagers mgr("tree_");
  // Prepare data
  // auua: 相比原版数据量乘了8倍，现在B+树有4层了
  const int n = 16000;
  vector<GenericKey *> keys;
  vector<RowId> values;
  vector<GenericKey *> delete_seq;
  map<GenericKey *, RowId> kv_map;
  for (int i = 0; i < n; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{
      Field(TypeId::kTypeInt, i),
      Field(TypeId::kTypeInt, i+n),
      Field(TypeId::kTypeChar, const_cast<char *>("minisql"), 7, true)
    };
    KP.SerializeFromKey(key, Row(fields), table_schema);
    keys.push_back(key);
    values.push_back(RowId(i));
    delete_seq.push_back(key);
  }
  vector<GenericKey *> keys_copy(keys);
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[0], table_schema);
  // Search keys
  vector<RowId> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(keys_copy[i], ans);
    ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Delete half keys
  for (int i = 0; i < n / 2; i++) {
    tree.Remove(delete_seq[i]);
  }
  tree.PrintTree(mgr[1], table_schema);
  // Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
  
  // auua: 删掉剩下的一半，之后只要检查树是否为空即可
  for (int i = n/2; i < n; i++) {
    tree.Remove(delete_seq[i]);
  }
  tree.PrintTree(mgr[2], table_schema);
  ASSERT_TRUE(tree.IsEmpty());
}