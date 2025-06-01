#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
 * TODO: Student Implement
 */
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  // 写入魔数
  MACH_WRITE_UINT32(buf + offset, COLUMN_MAGIC_NUM);
  offset += sizeof(uint32_t);
  // 写入name_
  MACH_WRITE_UINT32(buf + offset, name_.length());
  offset += sizeof(uint32_t);
  MACH_WRITE_STRING(buf + offset, name_);
  offset += name_.length();
  // 写入type_
  MACH_WRITE_INT32(buf + offset, type_);
  offset += sizeof(int32_t);
  // 写入len_
  MACH_WRITE_UINT32(buf + offset, len_);
  offset += sizeof(uint32_t);
  // 写入table_ind_
  MACH_WRITE_UINT32(buf + offset, table_ind_);
  offset += sizeof(uint32_t);
  // 写入nullable_
  MACH_WRITE_UINT32(buf + offset, nullable_);
  offset += sizeof(uint32_t);
  // 写入unique_
  MACH_WRITE_UINT32(buf + offset, unique_);
  offset += sizeof(uint32_t);
  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // 计算序列化后的总字节数
  uint32_t size = 0;
  size += sizeof(uint32_t);  // MAGIC_NUM
  size += sizeof(uint32_t);  // name_长度
  size += name_.length();    // name_内容
  size += sizeof(int32_t);   // type_
  size += sizeof(uint32_t);  // len_
  size += sizeof(uint32_t);  // table_ind_
  size += sizeof(uint32_t);  // nullable_
  size += sizeof(uint32_t);  // unique_
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t offset = 0;
  // 读取并验证魔数
  uint32_t magic_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Invalid column data");
  // 读取name_
  uint32_t name_len = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  std::string name(buf + offset, name_len);
  offset += name_len;
  // 读取type_
  TypeId type = (TypeId)MACH_READ_INT32(buf + offset);
  offset += sizeof(int32_t);
  // 读取len_
  uint32_t len = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  // 读取table_ind_
  uint32_t table_ind = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  // 读取nullable_
  bool nullable = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  // 读取unique_
  bool unique = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  // 根据type_选择合适的构造函数创建Column对象
  if (type == TypeId::kTypeChar) {
    column = new Column(name, type, len, table_ind, nullable, unique);
  } else {
    column = new Column(name, type, table_ind, nullable, unique);
  }
  return offset;
}
