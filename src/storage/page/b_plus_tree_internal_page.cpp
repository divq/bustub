//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetLSN(0);
  this->SetSize(0);
  this->SetMaxSize(max_size);
  this->SetParentPageId(parent_id);
  this->SetPageId(page_id);
  this->SetNextPageId(INVALID_PAGE_ID);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index >= 0 && index < this->GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < this->GetSize());
  array[index].first = key;
}
/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  // binary search?
  for (int i = 0; i < this->GetSize(); i++) {
    if (array[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  assert(index >= 0 && index < this->GetSize());
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  ValueType result = INVALID_PAGE_ID;
  if (comparator(key, array[1].first) == -1) {  // 这个地方，对于只有一个节点的情况怎么办？？to-do
    result = array[0].second;
  } else if (comparator(key, array[this->GetSize() - 1].first) != -1) {
    result = array[this->GetSize() - 1].second;
  } else {
    for (int i = 1; i < this->GetSize() - 1; i++) {
      if (comparator(key, array[i].first) != -1 && comparator(key, array[i + 1].first) == -1) {
        result = array[i].second;
        break;
      }
    }
  }
  return result;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  assert(IsRootPage());
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  this->SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int point = ValueIndex(old_value);
  int return_value = -1;
  if (point != -1) {
    for (int i = this->GetSize() - 1; i > point; i--) {
      array[i + 1] = array[i];
    }
    array[point + 1].first = new_key;
    array[point + 1].second = new_value;
    this->IncreaseSize(1);
    return_value = this->GetSize();
  }
  return return_value;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // 如果size是奇数，那么实际这么做会，移动的比留下的多1
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *page;
  int start_position = this->GetSize() / 2;
  int total_move = this->GetSize() - this->GetSize() / 2;
  assert(recipient->GetMaxSize() - recipient->GetSize() >= total_move);
  for (int i = 0; i < recipient->GetSize(); i++) {
    recipient->array[i + total_move] = recipient->array[i];
  }
  for (int i = 0; i < total_move; i++) {
    page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(
        buffer_pool_manager->FetchPage(array[start_position + i].second));
    page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
    recipient->array[i] = array[start_position + i];  // 这个地方怎么让record id对应的page改变它们的parent id呢？
  }
  this->IncreaseSize(-total_move);
  recipient->IncreaseSize(total_move);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents
 * page now changes to me. So I need to 'adopt' them by changing their parent
 * page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *page;
  for (int i = 0; i < size; i++) {
    page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(buffer_pool_manager->FetchPage(items[i].second));
    page->SetParentPageId(this->GetPageId());
    buffer_pool_manager->UnpinPage(items[i].second, true);
    array[i + this->GetSize()] = items[i];
  }
  this->IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(index >= 0 && index < this->GetSize());
  for (int i = index; i < this->GetSize() - 1; i++) {
    array[i] = array[i + 1];
  }
  this->SetSize(this->GetSize() - 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() { return INVALID_PAGE_ID; }
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the
 * invariant. You also need to use BufferPoolManager to persist changes to the
 * parent page id for those pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  array[0].first = middle_key;
  page_id_t new_parent_id = recipient->GetPageId();
  Page *child_page{nullptr};

  for (int i = 0; i < this->GetSize(); i++) {
    child_page = buffer_pool_manager->FetchPage(array[i].second);
    B_PLUS_TREE_INTERNAL_PAGE_TYPE *child_node =
        reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page->GetData());
    child_node->SetParentPageId(new_parent_id);
    buffer_pool_manager->UnpinPage(array[0].second, false);
  }
  recipient->CopyNFrom(array, this->GetSize(), buffer_pool_manager);
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the
 * invariant. You also need to use BufferPoolManager to persist changes to the
 * parent page id for those pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  array[0].first = middle_key;
  recipient->CopyFirstFrom(array[0], buffer_pool_manager);
  for (int i = 0; i < this->GetSize() - 1; i++) {
    array[i] = array[i + 1];
  }
  this->IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be
 * updated. So I need to 'adopt' it by changing its parent page id, which needs
 * to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  for (int i = this->GetSize(); i > 0; i--) {
    array[i] = array[i - 1];
  }
  array[0] = pair;
  Page *child_page = buffer_pool_manager->FetchPage(pair.second);
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *child_node =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page->GetData());
  child_node->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);
  this->IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s
 * array to position the middle_key at the right place. You also need to use
 * BufferPoolManager to persist changes to the parent page id for those pages
 * that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(array[this->GetSize() - 1], buffer_pool_manager);
  recipient->SetKeyAt(1, middle_key);
  this->IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be
 * updated. So I need to 'adopt' it by changing its parent page id, which needs
 * to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array[this->GetSize()].first = pair.first;
  array[this->GetSize()].second = pair.second;
  Page *child_page = buffer_pool_manager->FetchPage(pair.second);
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *child_node =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page->GetData());
  child_node->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);
  this->IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;

// find the index where array[index].second includes the key
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindIndex(const KeyType &key, const KeyComparator &comparator) const {
  int index = -1;
  assert(this->GetSize() > 1);
  if (comparator(array[1].first, key) == 1) {
    index = 0;
  } else if (comparator(key, array[this->GetSize() - 1].first) != -1) {
    index = this->GetSize() - 1;
  } else {
    for (int i = 1; i < this->GetSize() - 1; i++) {
      if (comparator(key, array[i].first) != -1 && comparator(array[i + 1].first, key) == 1) {
        index = i;
        break;
      }
    }
  }
  return index;
}
// INDEX_TEMPLATE_ARGUMENTS
// page_id_t B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetPreviousPageId(BufferPoolManager
// *buffer_pool_manager) {
//   assert(!this->IsRootPage());
//   page_id_t parent_page_id = this->GetParentPageId();
//   page_id_t rightmost_child_page_id{-1};
//   int index{-1};
//   Page *parent_page{nullptr};
//   Page *rightmost_child_page{nullptr};
//   B_PLUS_TREE_INTERNAL_PAGE_TYPE *child_node{nullptr};
//   B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent_node{nullptr};
//   int degree{0};
//   while (parent_page_id != INVALID_PAGE_ID) {
//     parent_page = buffer_pool_manager->FetchPage(parent_page_id);
//     parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE
//     *>(parent_page->GetData()); index =
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

}  // namespace bustub
