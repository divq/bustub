/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() : current_page_id_{INVALID_PAGE_ID}, buffer_pool_manager_{nullptr} {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { delete value; }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t current_page_id, page_id_t next_page_id, int index, int size,
                                  BufferPoolManager *buffer_pool_manager)
    : current_page_id_(current_page_id),
      next_page_id_(next_page_id),
      index_(index),
      current_page_size_(size),
      buffer_pool_manager_(buffer_pool_manager) {
  Page *leaf_page = buffer_pool_manager_->FetchPage(current_page_id_);
  if (leaf_page != nullptr) {
    auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
    value = new MappingType;
    *value = leaf_node->GetItem(index_);
    buffer_pool_manager_->UnpinPage(current_page_id_, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return current_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  auto leaf_page = buffer_pool_manager_->FetchPage(current_page_id_);
  auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  *value = leaf_node->GetItem(index_);
  ReleasePage();
  return *value;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (current_page_id_ != INVALID_PAGE_ID) {
    index_++;
    if (index_ >= current_page_size_) {
      if (next_page_id_ == INVALID_PAGE_ID) {
        ReleasePage();
        current_page_id_ = INVALID_PAGE_ID;
      } else {
        auto next_page = buffer_pool_manager_->FetchPage(next_page_id_);
        auto next_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page->GetData());
        ReleasePage();
        current_page_id_ = next_page_id_;
        next_page_id_ = next_node->GetNextPageId();
        index_ = 0;
        current_page_size_ = next_node->GetSize();
        *value = next_node->GetItem(index_);
        ReleasePage();
      }
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
