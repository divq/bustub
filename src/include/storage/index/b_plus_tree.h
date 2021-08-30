//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>
enum class OperationType { READ = 0, INSERT, DELETE };
/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);
  ~BPlusTree() {
    delete virtual_page;
    delete read_page;
  }

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);
  INDEXITERATOR_TYPE end();

  void Print(BufferPoolManager *bpm) {
    ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
  }

  void Draw() {
    time_t t = time(nullptr);
    char ch[64];
    strftime(ch, sizeof(ch), "%Y-%m-%d-%H-%M-%S", localtime(&t));
    std::string time_of_this_run(ch);
    std::string extra_info("ManualDraw");
    Draw(888, &time_of_this_run, &extra_info);
  }

  void Draw(size_t iter = 0, const std::string *time_stamp = nullptr, const std::string *extra_info = nullptr) {
    std::string filename("tree_graph_");
    if (time_stamp != nullptr) {
      filename += *time_stamp;
      filename += "_";
    }
    std::string iter_sequence = std::to_string(iter);
    filename += iter_sequence;

    if (extra_info != nullptr) {
      filename += "_";
      filename += *extra_info;
    }
    filename += ".dot";
    Draw(buffer_pool_manager_, filename);
  }
  void Draw(BufferPoolManager *bpm, const std::string &outf) {
    root_page_lock.lock();
    std::ofstream out(outf);
    out << "digraph G {" << std::endl;
    ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
    out << "}" << std::endl;
    out.close();
    root_page_lock.unlock();
  }

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);
  // expose for test purpose
  Page *FindLeafPage(const KeyType &key, bool leftMost = false);

 private:
  void StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  template <typename N>
  N *Split(N *node);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  template <typename N>
  bool Coalesce(N **neighbor_node, N **node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
                int index, Transaction *transaction = nullptr);

  template <typename N>
  void Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  std::mutex root_page_lock;

  // added by me
  int tree_height;
  Page *virtual_page;
  Page *read_page;
  Page *pre_root_page;
  page_id_t pre_root_page_id;

  Page *GetPreRootPage() { return buffer_pool_manager_->FetchPage(pre_root_page_id); }
  page_id_t ExamineNextPageId(page_id_t page_id) {
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    page_id_t answer;
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      auto node_ = reinterpret_cast<LeafPage *>(page->GetData());
      answer = node_->GetNextPageId();
    } else {
      auto node_ = reinterpret_cast<InternalPage *>(page->GetData());
      answer = node_->GetNextPageId();
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    return answer;
  }

  page_id_t ExamineParentPageId(page_id_t page_id) {
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    page_id_t answer;
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    answer = node->GetParentPageId();
    buffer_pool_manager_->UnpinPage(page_id, false);
    return answer;
  }

  template <typename N>
  page_id_t GetPreviousPageId(N *node);

  template <typename N>
  page_id_t GetNextPageId(N *node);

  template <typename N>
  Page *GoToBranchLayer(N *node, int *branch_index = nullptr, int *traverse_height = nullptr, bool right = true,
                        KeyType *right_key = nullptr);

  Page *WhichLeafPage(const KeyType &key, bool leftMost = false, bool precise_key = false,
                      OperationType operation = OperationType::READ, page_id_t *dangerous_page = nullptr);

  Page *FindLeafPageToInsert(const KeyType &key, std::vector<page_id_t> *w_latched_pages);

  Page *FindLeafPageGetValue(const KeyType &key);

  Page *SingleThreadWhichLeafPage(const KeyType &key, bool leftMost = false, bool precise_key = false);

  page_id_t GoDown(Page *branch_page, int branch_index, int traverse_height, bool go_right_down);

  page_id_t GoLeftBoundary();

  void UnPinPage(page_id_t page_id, bool dirty= false){
    buffer_pool_manager_->UnpinPage(page_id,dirty);
  }
  Page *ReadFetch(page_id_t page_id, Page *previous_page = nullptr, bool unpin_previous_page = false,
                  bool dirty = false) {
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    page->RLatch();
    if (previous_page != nullptr) {
      previous_page->RUnlatch();
    }
    if (unpin_previous_page) {
      buffer_pool_manager_->UnpinPage(previous_page->GetPageId(), dirty);
    }
    return page;
  }

  Page *DeleteFetch(page_id_t page_id, page_id_t *dangerous_page_id = nullptr) {
    // Page *page = buffer_pool_manager_->FetchPage(page_id);
    // page->WLatch();
    // InternalPage *node = reinterpret_cast<InternalPage*>(page->GetData());
    // if(dangerous_page_id&&node->GetSize())
    return nullptr;
  }

  void RUnlatchAndUnpin(Page *page, bool dirty = false) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), dirty);
  }

  void WUnlatchAndUnpin(Page *page, bool dirty = true) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), dirty);
  }

  Page *OperationFetch(OperationType operation, page_id_t page_id, Page *previous_page = nullptr,
                       bool unpin_previous_page = false, bool dirty = false, page_id_t *dangerous_page_id = nullptr) {
    Page *answer = nullptr;
    if (operation == OperationType::READ) {
      answer = ReadFetch(page_id, previous_page, unpin_previous_page, dirty);
    } else if (operation == OperationType::DELETE) {
      answer = DeleteFetch(page_id, dangerous_page_id);
    }
    // else {
    //   answer = InsertFetch(page_id);
    // }
    return answer;
  }
  inline Page *GetPage(page_id_t page_id) { return buffer_pool_manager_->FetchPage(page_id); }
  inline void ReleasePage(page_id_t page_id, bool dirty = false) { buffer_pool_manager_->UnpinPage(page_id, dirty); }
  inline void ReleasePage(Page *page, bool dirty = false) { buffer_pool_manager_->UnpinPage(page->GetPageId(), dirty); }
  inline InternalPage *GetInternalPage(Page *page) { return reinterpret_cast<InternalPage *>(page->GetData()); }
  inline LeafPage *GetLeafPage(Page *page) { return reinterpret_cast<LeafPage *>(page->GetData()); }
  inline InternalPage *GetInternalPage(page_id_t page_id) {
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    assert(page != nullptr);
    return reinterpret_cast<InternalPage *>(page->GetData());
  }
  inline LeafPage *GetLeafPage(page_id_t page_id) {
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    assert(page != nullptr);
    return reinterpret_cast<LeafPage *>(page->GetData());
  }
};

}  // namespace bustub
