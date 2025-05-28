#include "storage/disk_manager.h"

#include <unordered_set>

#include "gtest/gtest.h"

TEST(DiskManagerTest, DISABLED_BitMapPageTest) {
  const size_t size = 512;
  char buf[size];
  memset(buf, 0, size);
  BitmapPage<size> *bitmap = reinterpret_cast<BitmapPage<size> *>(buf);
  auto num_pages = bitmap->GetMaxSupportedSize();
  for (uint32_t i = 0; i < num_pages; i++) {
    ASSERT_TRUE(bitmap->IsPageFree(i));
  }
  uint32_t ofs;
  std::unordered_set<uint32_t> page_set;
  for (uint32_t i = 0; i < num_pages; i++) {
    ASSERT_TRUE(bitmap->AllocatePage(ofs));
    ASSERT_TRUE(page_set.find(ofs) == page_set.end());
    page_set.insert(ofs);
  }
  ASSERT_FALSE(bitmap->AllocatePage(ofs));
  ASSERT_TRUE(bitmap->DeAllocatePage(233));
  ASSERT_TRUE(bitmap->AllocatePage(ofs));
  ASSERT_EQ(233, ofs);
  for (auto v : page_set) {
    ASSERT_TRUE(bitmap->DeAllocatePage(v));
    ASSERT_FALSE(bitmap->DeAllocatePage(v));
  }
  for (uint32_t i = 0; i < num_pages; i++) {
    ASSERT_TRUE(bitmap->AllocatePage(ofs));
  }
  ASSERT_FALSE(bitmap->AllocatePage(ofs));
}

TEST(DiskManagerTest, DISABLED_FreePageAllocationTest) {
  std::string db_name = "disk_test.db";
  remove(db_name.c_str());
  DiskManager *disk_mgr = new DiskManager(db_name);
  int extent_nums = 2;
  for (uint32_t i = 0; i < DiskManager::BITMAP_SIZE * extent_nums; i++) {
    page_id_t page_id = disk_mgr->AllocatePage();
    DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(disk_mgr->GetMetaData());
    EXPECT_EQ(i, page_id);
    EXPECT_EQ(i / DiskManager::BITMAP_SIZE + 1, meta_page->GetExtentNums());
    EXPECT_EQ(i + 1, meta_page->GetAllocatedPages());
    EXPECT_EQ(i % DiskManager::BITMAP_SIZE + 1, meta_page->GetExtentUsedPage(i / DiskManager::BITMAP_SIZE));
  }
  disk_mgr->DeAllocatePage(0);
  disk_mgr->DeAllocatePage(DiskManager::BITMAP_SIZE - 1);
  disk_mgr->DeAllocatePage(DiskManager::BITMAP_SIZE);
  disk_mgr->DeAllocatePage(DiskManager::BITMAP_SIZE + 1);
  disk_mgr->DeAllocatePage(DiskManager::BITMAP_SIZE + 2);
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(disk_mgr->GetMetaData());
  EXPECT_EQ(extent_nums * DiskManager::BITMAP_SIZE - 5, meta_page->GetAllocatedPages());
  EXPECT_EQ(DiskManager::BITMAP_SIZE - 2, meta_page->GetExtentUsedPage(0));
  EXPECT_EQ(DiskManager::BITMAP_SIZE - 3, meta_page->GetExtentUsedPage(1));
}

TEST(DiskManagerTest, DISABLED_ExtentPageAllocationTest) {
    std::string db_name = "disk_test.db";
    remove(db_name.c_str());
    DiskManager *disk_mgr = new DiskManager(db_name);
    DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(disk_mgr->GetMetaData());

    // 先填满第一个分区
    for (uint32_t i = 0; i < DiskManager::BITMAP_SIZE; i++) {
        page_id_t page_id = disk_mgr->AllocatePage();
        EXPECT_EQ(i, page_id);
        EXPECT_EQ(1, meta_page->GetExtentNums());
        EXPECT_EQ(i + 1, meta_page->GetAllocatedPages());
        EXPECT_EQ(i + 1, meta_page->GetExtentUsedPage(0));
    }

    // 释放两块连续区域：[100-199], [300-399]
    for (uint32_t i = 100; i < 200; i++) {
        disk_mgr->DeAllocatePage(i);
    }
    for (uint32_t i = 300; i < 400; i++) {
        disk_mgr->DeAllocatePage(i);
    }
    meta_page = reinterpret_cast<DiskFileMetaPage *>(disk_mgr->GetMetaData());
    EXPECT_EQ(DiskManager::BITMAP_SIZE - 200, meta_page->GetAllocatedPages());
    EXPECT_EQ(DiskManager::BITMAP_SIZE - 200, meta_page->GetExtentUsedPage(0));

    // 重新分配，验证分配的页仍在第一个分区
    std::vector<page_id_t> allocated_pages;
    for (int i = 0; i < 200; i++) {
        page_id_t page_id = disk_mgr->AllocatePage();
        allocated_pages.push_back(page_id);
    }
    // 验证前100个页面是从[100-199]分配的
    for (int i = 0; i < 100; i++) {
        EXPECT_EQ(100 + i, allocated_pages[i]);
    }
    // 验证后100个页面是从[300-399]分配的
    for (int i = 100; i < 200; i++) {
        EXPECT_EQ(300 + (i - 100), allocated_pages[i]);
    }
    meta_page = reinterpret_cast<DiskFileMetaPage *>(disk_mgr->GetMetaData());
    EXPECT_EQ(DiskManager::BITMAP_SIZE, meta_page->GetAllocatedPages());
    EXPECT_EQ(DiskManager::BITMAP_SIZE, meta_page->GetExtentUsedPage(0));

    // 继续分配，验证新的页在第二个分区
    page_id_t new_page_id = disk_mgr->AllocatePage();
    EXPECT_EQ(DiskManager::BITMAP_SIZE, new_page_id);
    meta_page = reinterpret_cast<DiskFileMetaPage *>(disk_mgr->GetMetaData());
    EXPECT_EQ(2, meta_page->GetExtentNums());
    EXPECT_EQ(DiskManager::BITMAP_SIZE + 1, meta_page->GetAllocatedPages());
    EXPECT_EQ(DiskManager::BITMAP_SIZE, meta_page->GetExtentUsedPage(0));
    EXPECT_EQ(1, meta_page->GetExtentUsedPage(1));

    // 释放第一个分区的100个页面后，继续分配100个新页，验证这些页仍然分配在第二个分区
    for (uint32_t i = 0; i < 100; i++) {
        disk_mgr->DeAllocatePage(i);
    }
    meta_page = reinterpret_cast<DiskFileMetaPage *>(disk_mgr->GetMetaData());
    EXPECT_EQ(DiskManager::BITMAP_SIZE - 99, meta_page->GetAllocatedPages());
    EXPECT_EQ(DiskManager::BITMAP_SIZE - 100, meta_page->GetExtentUsedPage(0));
    EXPECT_EQ(1, meta_page->GetExtentUsedPage(1));

    // 分配100个新页，验证都在第二个分区
    for (int i = 0; i < 100; i++) {
        page_id_t page_id = disk_mgr->AllocatePage();
        EXPECT_EQ(DiskManager::BITMAP_SIZE + 1 + i, page_id);
    }
    meta_page = reinterpret_cast<DiskFileMetaPage *>(disk_mgr->GetMetaData());
    EXPECT_EQ(2, meta_page->GetExtentNums());
    EXPECT_EQ(DiskManager::BITMAP_SIZE + 1, meta_page->GetAllocatedPages());
    EXPECT_EQ(DiskManager::BITMAP_SIZE - 100, meta_page->GetExtentUsedPage(0));
    EXPECT_EQ(101, meta_page->GetExtentUsedPage(1));
}