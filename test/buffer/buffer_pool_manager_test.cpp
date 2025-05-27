#include "buffer/buffer_pool_manager.h"

#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"

TEST(BufferPoolManagerTest, BinaryDataTest) {
  const std::string db_name = "bpm_test.db";
  const size_t buffer_pool_size = 10;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<unsigned> uniform_dist(0, 127);

  remove(db_name.c_str());
  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[PAGE_SIZE / 2] = '\0';
  random_binary_data[PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(page_id_temp));
    EXPECT_EQ(i, page_id_temp);
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    EXPECT_TRUE(bpm->FlushPage(i));
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(page_id_temp));
    EXPECT_EQ(buffer_pool_size + i, page_id_temp);
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  delete bpm;
  disk_manager->Close();
  delete disk_manager;
  remove(db_name.c_str());
}

// 初始化缓冲池并填充数据
void InitializeBufferPool(BufferPoolManager *bpm, std::vector<page_id_t> &page_ids, size_t buffer_pool_size, size_t test_pages) {
    printf("Filling buffer pool...\n");
    for (size_t i = 0; i < test_pages; ++i) {
        page_id_t page_id;
        auto *page = bpm->NewPage(page_id);
        EXPECT_EQ(page_id, i);
        if (page != nullptr) {
            sprintf(page->GetData(), "Page content %zu", i);
            page_ids.push_back(page_id);
            if (i >= buffer_pool_size - 1) {
                EXPECT_TRUE(bpm->UnpinPage(page_id, true));
            }
        }
    }

    // 解除前半部分页面的固定
    for (size_t i = 0; i < buffer_pool_size / 2; ++i) {
        EXPECT_TRUE(bpm->UnpinPage(page_ids[i], true));
    }
}

// 测试频繁访问模式
void TestFrequentAccess(BufferPoolManager *bpm, const std::vector<page_id_t> &page_ids, size_t buffer_pool_size) {
    // 应当很快，因为前半部分还没有被替换
    printf("Accessing the whole buffer pool...\n");
    for (size_t i = 0; i < buffer_pool_size; ++i) {
        for (int j = 0; j < 5; ++j) {
            auto *page = bpm->FetchPage(page_ids[i]);
            EXPECT_NE(nullptr, page);
            EXPECT_TRUE(bpm->UnpinPage(page_ids[i], false));
        }
    }
}

// 测试页面替换
void TestPageReplacement(BufferPoolManager *bpm, const std::vector<page_id_t> &page_ids, size_t buffer_pool_size, size_t test_pages) {
    printf("Accessing some pages that have been replaced...\n");
    for (size_t i = buffer_pool_size; i < test_pages; ++i) {  // 后 1000 页
        auto *page = bpm->FetchPage(page_ids[i]);
        EXPECT_NE(nullptr, page);
        if (page != nullptr) {
            char expected_data[PAGE_SIZE];
            sprintf(expected_data, "Page content %zu", i);
            EXPECT_EQ(0, memcmp(page->GetData(), expected_data, strlen(expected_data)));
            EXPECT_TRUE(bpm->UnpinPage(page_ids[i], false));
        }
    }
}

// 测试刚读进来的页和被 Pin 住的后 500 页
void TestRecentPages(BufferPoolManager *bpm, const std::vector<page_id_t> &page_ids, size_t buffer_pool_size, size_t test_pages) {
    printf("Accessing some recently accessed pages...\n");
    for (size_t i = buffer_pool_size / 2; i < buffer_pool_size; ++i) {
        auto *page = bpm->FetchPage(page_ids[i]);
        EXPECT_NE(nullptr, page);
        if (page!= nullptr) {
            EXPECT_TRUE(bpm->UnpinPage(page_ids[i], false));
        }
    }
    for (size_t i = test_pages - 500; i < test_pages; ++i) {
        auto *page = bpm->FetchPage(page_ids[i]);
        EXPECT_NE(nullptr, page);
        if (page!= nullptr) {
            EXPECT_TRUE(bpm->UnpinPage(page_ids[i], false));
        }
    }
}

// 测试被替换掉的前 500 页
void TestReplacedPages(BufferPoolManager *bpm, const std::vector<page_id_t> &page_ids, size_t buffer_pool_size) {
    printf("Accessing some replaced pages...\n");
    for (size_t i = 0; i < buffer_pool_size / 2; ++i) {
        auto *page = bpm->FetchPage(page_ids[i]);
        EXPECT_NE(nullptr, page);
        if (page!= nullptr) {
            char expected_data[PAGE_SIZE];
            sprintf(expected_data, "Page content %zu", i);
            EXPECT_EQ(0, memcmp(page->GetData(), expected_data, strlen(expected_data)));
            EXPECT_TRUE(bpm->UnpinPage(page_ids[i], false));
        }
    }
}

// 测试页面删除
void TestPageDeletion(BufferPoolManager *bpm, const std::vector<page_id_t> &page_ids, size_t buffer_pool_size) {
    printf("Deleting some pages...\n");
    for (size_t i = 0; i < buffer_pool_size / 4; ++i) {
        EXPECT_TRUE(bpm->DeletePage(page_ids[i]));
    }
}

// 测试新页面创建
void TestNewPageCreation(BufferPoolManager *bpm, std::vector<page_id_t> &new_page_ids, size_t buffer_pool_size) {
    printf("Creating new pages...\n");
    for (size_t i = 0; i < buffer_pool_size / 4; ++i) {
        page_id_t new_page_id;
        auto *new_page = bpm->NewPage(new_page_id);
        EXPECT_NE(nullptr, new_page);
        if (new_page != nullptr) {
            new_page_ids.push_back(new_page_id);
            EXPECT_TRUE(bpm->UnpinPage(new_page_id, true));
        }
    }
}

// 验证缓冲池状态
void VerifyBufferPoolState(BufferPoolManager *bpm, const std::vector<page_id_t> &page_ids, size_t buffer_pool_size) {
    printf("Verifying frequent accessed pages...\n");
    for (size_t i = buffer_pool_size / 2; i < buffer_pool_size; ++i) {
        auto *page = bpm->FetchPage(page_ids[i]);
        EXPECT_NE(nullptr, page);
        if (page != nullptr) {
            EXPECT_TRUE(bpm->UnpinPage(page_ids[i], false));
        }
    }
}

TEST(BufferPoolManagerTest, LRUPerformanceTest) {
    const std::string db_name = "bpm_performance_test.db";
    const size_t buffer_pool_size = 1000;
    const size_t test_pages = buffer_pool_size * 2;

    remove(db_name.c_str());
    auto *disk_manager = new DiskManager(db_name);
    auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    std::vector<page_id_t> page_ids;
    std::vector<page_id_t> new_page_ids;

    // 初始化并填充缓冲池
    InitializeBufferPool(bpm, page_ids, buffer_pool_size, test_pages);

    // 执行各个测试阶段
    TestFrequentAccess(bpm, page_ids, buffer_pool_size);
    TestPageReplacement(bpm, page_ids, buffer_pool_size, test_pages);
    TestRecentPages(bpm, page_ids, buffer_pool_size, test_pages);
    TestReplacedPages(bpm, page_ids, buffer_pool_size);
    TestPageDeletion(bpm, page_ids, buffer_pool_size);
    TestNewPageCreation(bpm, new_page_ids, buffer_pool_size);

    // 验证最终状态
    VerifyBufferPoolState(bpm, page_ids, buffer_pool_size);

    // 清理
    delete bpm;
    disk_manager->Close();
    delete disk_manager;
    remove(db_name.c_str());
}