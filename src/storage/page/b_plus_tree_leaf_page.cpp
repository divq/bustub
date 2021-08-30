//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetLSN(0);
  this->SetSize(0);
  this->SetMaxSize(max_size);
  this->SetParentPageId(parent_id);
  this->SetPageId(page_id);
  this->SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int index = -1;
  for (int i = 0; i < this->GetSize(); i++) {
    if (comparator(array[i].first, key) != -1) {
      index = i;
      break;
    }
  }
  assert(index != -1);
  return index;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index >= 0 && index < this->GetSize());
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  assert(this->GetSize() < this->GetMaxSize());
  if (this->GetSize() == 0) {
    array[0].first = key;
    array[0].second = value;
  } else if (comparator(array[0].first, key) == 1) {
    for (int i = this->GetSize() - 1; i >= 0; i--) {
      array[i + 1] = array[i];
    }
    array[0].first = key;
    array[0].second = value;
  } else if (comparator(array[this->GetSize() - 1].first, key) != 1) {
    array[this->GetSize()].first = key;
    array[this->GetSize()].second = value;
  } else {
    for (int i = 0; i < this->GetSize() - 1; i++) {
      if (comparator(array[i].first, key) == -1 && comparator(array[i + 1].first, key) == 1) {
        for (int j = this->GetSize() - 1; j > i; j--) {
          array[j + 1] = array[j];
        }
        array[i + 1].first = key;
        array[i + 1].second = value;
        break;
      }
    }
  }
  this->IncreaseSize(1);
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  // 如果size是奇数，那么实际这么做会，移动的比留下的多1
  int start_position = this->GetSize() / 2;
  int total_move = this->GetSize() - this->GetSize() / 2;
  assert(recipient->GetMaxSize() - recipient->GetSize() >= total_move);
  for (int i = 0; i < recipient->GetSize(); i++) {
    recipient->array[i + total_move] = recipient->array[i];
  }
  for (int i = 0; i < total_move; i++) {
    recipient->array[i] = array[start_position + i];  //  这个地方怎么让record id对应的page改变它们的parent id呢？
  }
  recipient->IncreaseSize(total_move);
  this->SetSize(this->GetSize() - total_move);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  for (int i = 0; i < size; i++) {
    array[i + this->GetSize()] = items[i];
  }
  this->IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  bool result = false;
  for (int i = 0; i < this->GetSize(); i++) {
    if (comparator(key, array[i].first) == 0) {
      if (value != nullptr) {
        *value = array[i].second;
      }
      result = true;
      break;
    }
  }
  return result;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  bool found = false;
  for (int i = 0; i < this->GetSize(); i++) {
    if (comparator(array[i].first, key) == 0) {
      for (int j = i; j < this->GetSize() - 1; j++) {
        array[j] = array[j + 1];
      }
      found = true;
      break;
    }
  }
  this->SetSize(this->GetSize() - static_cast<int>(found));
  return this->GetSize();
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't
 * forget to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(array, this->GetSize());
  recipient->SetNextPageId(this->GetNextPageId());
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyLastFrom(array[0]);
  for (int i = 0; i < this->GetSize() - 1; i++) {
    array[i] = array[i + 1];
  }
  this->IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array[this->GetSize()] = item;
  this->IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyFirstFrom(array[this->GetSize() - 1]);
  this->IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  for (int i = this->GetSize(); i > 0; i--) {
    array[i] = array[i - 1];
  }
  array[0] = item;
  this->IncreaseSize(1);
}

// INDEX_TEMPLATE_ARGUMENTS
// page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetPreviousPageId(BufferPoolManager
// *buffer_pool_manager) {
//     typedef BPlusTreePage<KeyType, ValueType, KeyComparator>
//     B_PLUS_TREE_INTERNAL_PAGE_TYPE;
//   assert(!this->IsRootPage());
//   page_id_t parent_page_id = this->GetParentPageId();
//   page_id_t rightmost_child_page_id{-1};
//   Page *parent_page{nullptr};
//   Page *rightmost_child_page{nullptr};
//   B_PLUS_TREE_INTERNAL_PAGE_TYPE *child_node{nullptr};
//   B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent_node{nullptr};
//   int degree{0};
//   while (parent_page_id != INVALID_PAGE_ID) {
//     parent_page = buffer_pool_manager->FetchPage(parent_page_id);
//     parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE
//     *>(parent_page->GetData()); auto index =
//     parent_node->ValueIndex(this->GetPageId()); degree++; if (index != 0) {
//       break;
//     }
//     auto parent_page_id_to_delete = parent_page_id;
//     parent_page_id = parent_node->GetParentPageId();
//     buffer_pool_manager->UnpinPage(parent_page_id_to_delete, false);
//   }
//   assert(index != 0);
//   rightmost_child_page_id = parent_node->ValueAt(parent_node->GetSize() - 1);
//   buffer_pool_manager->UnpinPage(parent_page_id, false);
//   degree--;
//   while (degree) {
//     rightmost_child_page =
//     buffer_pool_manager->FetchPage(rightmost_child_page_id); child_node =
//     reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE
//     *>(rightmost_child_page->GetData()); auto page_to_unpin =
//     rightmost_child_page_id; rightmost_child_page_id =
//     child_node->ValueAt(child_node->GetSize() - 1); degree--;
//     buffer_pool_manager->UnpinPage(page_to_unpin, false);
//   }
//   return rightmost_child_page_id;
// }

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
