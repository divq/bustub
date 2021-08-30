//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(page_id_t current_page_id, page_id_t next_page_id, int index, int size,
                BufferPoolManager *buffer_pool_manager);

  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    return (current_page_id_ == itr.current_page_id_ && next_page_id_ == itr.next_page_id_ && index_ == itr.index_ &&
            current_page_size_ == itr.current_page_size_) ||
           (current_page_id_ == INVALID_PAGE_ID && itr.current_page_id_ == INVALID_PAGE_ID);
  }

  bool operator!=(const IndexIterator &itr) const { return !(*this == itr); }
  page_id_t GetCurrentPageId() { return current_page_id_; }
  page_id_t GetNextPageId() { return next_page_id_; }

 private:
  page_id_t current_page_id_;
  page_id_t next_page_id_;
  int index_;
  int current_page_size_;
  page_id_t page_occupied;
  BufferPoolManager *buffer_pool_manager_;
  inline void ReleasePage() { buffer_pool_manager_->UnpinPage(current_page_id_, false); }
  MappingType *value = nullptr;
  // add your own private member variables here
};

}  // namespace bustub
