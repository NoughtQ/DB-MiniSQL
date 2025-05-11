#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  // 写入魔数
  MACH_WRITE_UINT32(buf + offset, SCHEMA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  // 写入columns_大小
  uint32_t columns_size = GetColumnCount();
  MACH_WRITE_UINT32(buf + offset, columns_size);
  offset += sizeof(uint32_t);
  // 依次序列化每个Column对象
  for (const auto column : columns_) {
    offset += column->SerializeTo(buf + offset);
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = 0;
  size += sizeof(uint32_t);  // MAGIC_NUM
  size += sizeof(uint32_t);  // columns_大小
  // 计算所有Column对象的序列化大小
  for (const auto column : columns_) {
    size += column->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t offset = 0;
  // 读取并验证魔数
  uint32_t magic_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Invalid schema data");
  // 读取columns_大小
  uint32_t columns_size = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  // 依次反序列化每个Column对象
  std::vector<Column *> columns;
  for (uint32_t i = 0; i < columns_size; i++) {
    Column *column = nullptr;
    offset += Column::DeserializeFrom(buf + offset, column);
    columns.push_back(column);
  }
  // 创建新的Schema对象
  schema = new Schema(columns);
  return offset;
}