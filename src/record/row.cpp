#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  
  uint32_t offset = 0;
  uint32_t field_count = fields_.size();
  
  // 写入字段数量
  MACH_WRITE_UINT32(buf + offset, field_count);
  offset += sizeof(uint32_t);
  
  // 计算并写入空值位图
  uint32_t null_bitmap = 0;
  for (uint32_t i = 0; i < field_count; i++) {
    if (fields_[i]->IsNull()) {
      null_bitmap |= (1 << i);
    }
  }
  MACH_WRITE_UINT32(buf + offset, null_bitmap);
  offset += sizeof(uint32_t);
  
  // 写入每个字段的数据
  for (uint32_t i = 0; i < field_count; i++) {
    if (!fields_[i]->IsNull()) {
      offset += fields_[i]->SerializeTo(buf + offset);
    }
  }
  
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  
  uint32_t offset = 0;
  
  // 读取字段数量
  uint32_t field_count = MACH_READ_UINT32(buf + offset);
  ASSERT(field_count == schema->GetColumnCount(), "Fields size do not match schema's column size.");
  offset += sizeof(uint32_t);
  
  // 读取空值位图
  uint32_t null_bitmap = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  
  // 读取每个字段的数据
  for (uint32_t i = 0; i < field_count; i++) {
    bool is_null = (null_bitmap & (1 << i)) != 0;
    TypeId type_id = schema->GetColumn(i)->GetType();
    Field *field = nullptr;
    offset += Field::DeserializeFrom(buf + offset, type_id, &field, is_null);
    fields_.push_back(field);
  }
  
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  
  uint32_t size = 0;
  
  // 字段数量的大小
  size += sizeof(uint32_t);
  
  // 空值位图的大小
  size += sizeof(uint32_t);
  
  // 所有非空字段的序列化大小
  for (auto field : fields_) {
    if (!field->IsNull()) {
      size += field->GetSerializedSize();
    }
  }
  
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
