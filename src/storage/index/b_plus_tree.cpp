//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

const int max_size = 300;
namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size < max_size ? leaf_max_size : max_size),
      internal_max_size_(internal_max_size < max_size ? internal_max_size : max_size),
      tree_height(0),
      virtual_page(new Page),
      read_page(new Page) {
  pre_root_page = buffer_pool_manager_->NewPage(&pre_root_page_id);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  bool return_value = false;
  bool is_empty = true;
  read_page->RLatch();
  (GetPreRootPage())->RLatch();
  is_empty = IsEmpty();
  if (!is_empty) {
    Page *leaf_page = FindLeafPageGetValue(key);
    if (leaf_page != nullptr) {
      LeafPage *leaf_node = GetLeafPage(leaf_page);
      ValueType res;
      bool exist = leaf_node->Lookup(key, &res, comparator_);
      if (exist) {
        result->push_back(res);
        return_value = true;
      }
      RUnlatchAndUnpin(leaf_page);
    }
  } else {  // 防止unlatch和latch不匹配
    (GetPreRootPage())->RUnlatch();
  }
  read_page->RUnlatch();
  return return_value;
}  // namespace bustub

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  bool success = true;
  read_page->RLatch();
  (GetPreRootPage())->WLatch();
  if (IsEmpty()) {
    StartNewTree(key, value);
    (GetPreRootPage())->WUnlatch();
  } else {
    success = InsertIntoLeaf(key, value);
  }
  read_page->RUnlatch();
  return success;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page *new_page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (new_page == nullptr) {
    throw std::bad_alloc();
  }
  LeafPage *page = reinterpret_cast<LeafPage *>(new_page->GetData());
  root_page_id_ = new_page->GetPageId();
  page->SetPageId(root_page_id_);
  page->SetNextPageId(INVALID_PAGE_ID);
  page->SetPageType(IndexPageType::LEAF_PAGE);
  page->SetParentPageId(INVALID_PAGE_ID);
  page->SetMaxSize(leaf_max_size_);
  page->SetSize(0);
  page->Insert(key, value, comparator_);
  UpdateRootPageId();
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  tree_height = 1;
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  bool result = false;
  std::vector<page_id_t> w_latched_pages;
  Page *leaf_page = FindLeafPageToInsert(key, &w_latched_pages);
  if (leaf_page != nullptr) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
    bool find = leaf_node->Lookup(key, nullptr, comparator_);
    if (!find) {
      leaf_node->Insert(key, value, comparator_);
      if (leaf_node->GetSize() == leaf_node->GetMaxSize()) {
        auto middle_key = leaf_node->GetMiddleKey();
        auto new_leaf_node = Split<LeafPage>(leaf_node);
        InsertIntoParent(leaf_node, middle_key, new_leaf_node);
        buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);
      }
      result = true;
    }
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), !find);
  }

  // if (root_page_locked) {
  // root_page_lock.unlock();
  //}
  Page *page{nullptr};
  for (auto page_id : w_latched_pages) {
    page = GetPage(page_id);
    page->WUnlatch();
    ReleasePage(page);
  }
  return result;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw std::bad_alloc();
  }
  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  new_node->Init(new_page_id, node->GetParentPageId(), node->GetMaxSize());
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  new_node->SetNextPageId(GetNextPageId(node));
  node->SetNextPageId(new_node->GetPageId());
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->GetParentPageId() == INVALID_PAGE_ID) {  // this splits from root page as leaf page
    page_id_t new_root_page_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
    // new_root_page->WLatch();
    if (new_root_page == nullptr) {
      throw std::bad_alloc();
    }
    auto old_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    root_page_id_ = new_root_page_id;
    // old_root_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(old_root_page->GetPageId(), false);
    UpdateRootPageId();
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_node->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    tree_height++;
  } else {
    Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    if (parent_node->GetSize() < parent_node->GetMaxSize()) {
      parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      new_node->SetParentPageId(parent_node->GetPageId());
      if (parent_node->GetSize() == parent_node->GetMaxSize()) {
        auto middle_key = parent_node->GetMiddleKey();
        auto new_parent_node = Split(parent_node);
        InsertIntoParent(parent_node, middle_key, new_parent_node);
      }
    } else {
      InternalPage *new_parent_node{nullptr};
      auto middle_key = parent_node->GetMiddleKey();
      new_parent_node = Split<InternalPage>(parent_node);
      InsertIntoParent(parent_node, middle_key, new_parent_node);
      int check = parent_node->ValueIndex(old_node->GetPageId());
      InternalPage *node_to_insert = ((check != -1 && check < parent_node->GetSize()) ? parent_node : new_parent_node);
      node_to_insert->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      new_node->SetParentPageId(node_to_insert->GetPageId());
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  read_page->WLatch();
  Page *leaf_page = SingleThreadWhichLeafPage(key);
  page_id_t leaf_page_id{INVALID_PAGE_ID};
  bool to_be_deleted{false};
  int size_after_remove{-1};

  if (leaf_page != nullptr) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
    leaf_page_id = leaf_node->GetPageId();
    size_after_remove = leaf_node->RemoveAndDeleteRecord(key, comparator_);
    if (size_after_remove < leaf_node->GetMinSize()) {
      to_be_deleted = CoalesceOrRedistribute<LeafPage>(leaf_node);
    }
    buffer_pool_manager_->UnpinPage(leaf_page_id, !to_be_deleted);
    if (to_be_deleted) {
      buffer_pool_manager_->DeletePage(leaf_page_id);
    }
  }
  read_page->WUnlatch();
  // Page *current_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  // current_root_page->WUnlatch();
  // buffer_pool_manager_->UnpinPage(root_page_id_, false);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  bool answer = false;
  bool flag = false;
  if (node->IsRootPage()) {
    bool is_leaf = node->IsLeafPage();
    bool is_internal = !is_leaf;
    bool need_to_adjust = (is_leaf && node->GetSize() < 1) || (is_internal && node->GetSize() <= 1);
    if (need_to_adjust) {
      answer = AdjustRoot(reinterpret_cast<BPlusTreePage *>(node));
    }
  } else {
    if (node->GetSize() >= node->GetMinSize()) {
      answer = false;
    } else {
      page_id_t sibling_page_id{-1};
      sibling_page_id = GetNextPageId(node);

      if (sibling_page_id == INVALID_PAGE_ID) {  // 如果右边没有sibling，则需要往左边找
        sibling_page_id = GetPreviousPageId<N>(node);
        flag = true;
      }
      Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
      N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());
      if (node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize()) {  // redistribute
        Redistribute<N>(sibling_node, node, GetNextPageId(sibling_node) == node->GetPageId());
        answer = false;
      } else {  // merge
        if (flag) {
          auto tmp = node;
          node = sibling_node;
          sibling_node = tmp;
        }
        auto sibling_parent_page_id = sibling_node->GetParentPageId();
        Page *sibling_parent_page = buffer_pool_manager_->FetchPage(sibling_parent_page_id);
        InternalPage *sibling_parent_node = reinterpret_cast<InternalPage *>(sibling_parent_page->GetData());
        bool parent_to_be_deleted = Coalesce<N>(&sibling_node, &node, &sibling_parent_node, 0, nullptr);
        if (parent_to_be_deleted) {
          buffer_pool_manager_->UnpinPage(sibling_parent_page_id, false);
          buffer_pool_manager_->DeletePage(sibling_parent_page_id);
        }
        answer = false;
      }
    }
  }
  return answer;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  auto left_node = *node;
  auto right_node = *neighbor_node;
  auto right_node_parent_page_id = right_node->GetParentPageId();
  auto right_node_parent_page = buffer_pool_manager_->FetchPage(right_node_parent_page_id);
  auto right_node_parent_node = reinterpret_cast<InternalPage *>(right_node_parent_page->GetData());
  auto current_page_id = right_node->GetPageId();
  int current_page_position{-1};
  int position_to_remove{-1};
  bool find = false;

  KeyType key_E;
  bool key_E_set = false;
  // KeyType key_C;
  // KeyType right_key;

  if (right_node_parent_node->GetSize() > 1) {
    key_E = right_node_parent_node->KeyAt(1);
    // Draw();
    assert(key_E.ToString() < 100000);
    key_E_set = true;
    // key_C = right_node->KeyAt(1);
  }

  while (right_node_parent_page_id != INVALID_PAGE_ID) {
    right_node_parent_page = buffer_pool_manager_->FetchPage(right_node_parent_page_id);
    right_node_parent_node = reinterpret_cast<InternalPage *>(right_node_parent_page->GetData());
    current_page_position = right_node_parent_node->ValueIndex(current_page_id);
    if (current_page_position != 0 || right_node_parent_node->GetSize() != 1) {
      if (!find) {
        *parent = right_node_parent_node;
        position_to_remove = current_page_position;
        find = true;
      }
      if (current_page_position == 0 && right_node_parent_node->GetSize() > 1 && !key_E_set) {
        key_E = right_node_parent_node->KeyAt(1);
        // Draw();
        assert(key_E.ToString() < 100000);
        key_E_set = true;
      }  // 如果一直没有找到这样的KEYE，
      // 说明之前的路径上全部都是一个节点，那么这个时候在分支层会把这个删除，所以KEYE也不需要找到了
      if (current_page_position != 0) {  //到达分支层
        auto Key_A = right_node_parent_node->KeyAt(current_page_position);
        // Draw();
        assert(Key_A.ToString() < 100000);
        if (left_node->IsLeafPage()) {
          LeafPage *right_node_ = reinterpret_cast<LeafPage *>(right_node);
          LeafPage *left_node_ = reinterpret_cast<LeafPage *>(left_node);
          right_node_->MoveAllTo(left_node_);
        } else {
          InternalPage *right_node_ = reinterpret_cast<InternalPage *>(right_node);
          InternalPage *left_node_ = reinterpret_cast<InternalPage *>(left_node);
          right_node_->MoveAllTo(left_node_, Key_A, buffer_pool_manager_);
        }
        if (key_E_set) {
          right_node_parent_node->SetKeyAt(current_page_position, key_E);
        }
        buffer_pool_manager_->UnpinPage(right_node_parent_page_id, key_E_set);
        left_node->SetNextPageId(GetNextPageId(right_node));
        (*parent)->Remove(position_to_remove);
        break;
      }
      current_page_id = right_node_parent_page_id;
      right_node_parent_page_id = right_node_parent_node->GetParentPageId();
      buffer_pool_manager_->UnpinPage(current_page_id, false);
    } else {
      auto previous_page_id = GetPreviousPageId(right_node_parent_node);  // to-do没考虑前面没page的情况
      auto previous_page = buffer_pool_manager_->FetchPage(previous_page_id);
      auto previous_node = reinterpret_cast<InternalPage *>(previous_page->GetData());
      previous_node->SetNextPageId(right_node_parent_node->GetNextPageId());
      current_page_id = right_node_parent_page_id;
      right_node_parent_page_id = right_node_parent_node->GetParentPageId();
      buffer_pool_manager_->UnpinPage(current_page_id, false);
      buffer_pool_manager_->DeletePage(current_page_id);
    }
  }
  buffer_pool_manager_->UnpinPage(right_node->GetPageId(), false);
  buffer_pool_manager_->DeletePage(right_node->GetPageId());
  return CoalesceOrRedistribute(*parent);
}  // namespace bustub

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  N *left_node{nullptr};
  N *right_node{nullptr};
  // bool move_right_to_left = static_cast<bool>(index);
  bool neighbor_is_left = static_cast<bool>(index);
  if (!neighbor_is_left) {  // index为0说明node在左边，否则node在右边
    left_node = node;
    right_node = neighbor_node;
  } else {
    left_node = neighbor_node;
    right_node = node;
  }

  int branch_index{-1};
  KeyType key_C;
  key_C.SetFromInteger(88888);
  Page *branch_page = GoToBranchLayer(right_node, &branch_index, nullptr, true);
  // Draw();
  assert(key_C.ToString() < 100000);

  assert(branch_page != nullptr);
  InternalPage *branch_node = reinterpret_cast<InternalPage *>(branch_page->GetData());
  auto key_D = left_node->KeyAt(left_node->GetSize() - 1);
  // Draw();
  assert(key_D.ToString() < 100000);
  KeyType key_A;

  if (node->IsLeafPage()) {
    LeafPage *right_node_ = reinterpret_cast<LeafPage *>(right_node);
    LeafPage *left_node_ = reinterpret_cast<LeafPage *>(left_node);
    if (!neighbor_is_left) {
      key_C = right_node_->KeyAt(1);
      right_node_->MoveFirstToEndOf(left_node_);
      branch_node->SetKeyAt(branch_index, key_C);
    } else {
      left_node_->MoveLastToFrontOf(right_node_);
      branch_node->SetKeyAt(branch_index, key_D);
    }
  } else {
    InternalPage *right_node_ = reinterpret_cast<InternalPage *>(right_node);
    InternalPage *left_node_ = reinterpret_cast<InternalPage *>(left_node);
    key_A = branch_node->KeyAt(branch_index);
    // Draw();
    assert(key_A.ToString() < 100000);
    if (!neighbor_is_left) {
      key_C = right_node_->KeyAt(1);
      right_node_->MoveFirstToEndOf(left_node_, key_A, buffer_pool_manager_);
      branch_node->SetKeyAt(branch_index, key_C);
    } else {
      left_node_->MoveLastToFrontOf(right_node_, key_A, buffer_pool_manager_);
      branch_node->SetKeyAt(branch_index, key_D);
    }
  }
  buffer_pool_manager_->UnpinPage(branch_page->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->GetSize() == 0) {  // case 2 or internal page no child
    // Page *current_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    // current_root_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
    root_page_id_ = INVALID_PAGE_ID;
    tree_height = 0;
  } else {  // situation: the root is an internal page and has only one child
    auto old_root_node_cast = reinterpret_cast<InternalPage *>(old_root_node);
    // auto old_root_page = buffer_pool_manager_->FetchPage(old_root_node_cast->GetPageId());
    auto only_child_page_id = old_root_node_cast->ValueAt(0);
    auto only_child_page = buffer_pool_manager_->FetchPage(only_child_page_id);
    // only_child_page->WLatch();
    auto only_child_bplustree_page = reinterpret_cast<BPlusTreePage *>(only_child_page);
    only_child_bplustree_page->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = only_child_page_id;
    // old_root_page->WUnlatch();
    // buffer_pool_manager_->UnpinPage(old_root_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    tree_height--;
  }
  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  // Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  // BPlusTreePage *root_node_raw = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
  auto leaf_page_id = GoLeftBoundary();
  auto leaf_page = buffer_pool_manager_->FetchPage(leaf_page_id);
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  auto next_page_id = GetNextPageId(leaf_node);
  auto size = leaf_node->GetSize();
  UnPinPage(leaf_page_id);
  return INDEXITERATOR_TYPE(leaf_page_id, next_page_id, 0, size, buffer_pool_manager_);
}  // namespace bustub

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *leaf_page = FindLeafPage(key);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  page_id_t leaf_page_id = leaf_node->GetPageId();
  page_id_t next_page_id = GetNextPageId(leaf_node);
  int index = leaf_node->KeyIndex(key, comparator_);
  int size = leaf_node->GetSize();
  RUnlatchAndUnpin(leaf_page);
  return INDEXITERATOR_TYPE(leaf_page_id, next_page_id, index, size, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
page_id_t BPLUSTREE_TYPE::GetPreviousPageId(N *node) {
  assert(!node->IsRootPage());
  int traverse_height{0};
  int branch_index{-1};
  Page *branch_page = GoToBranchLayer(node, &branch_index, &traverse_height);
  if (branch_page == nullptr) {
    return INVALID_PAGE_ID;
  }
  return GoDown(branch_page, branch_index, traverse_height, false);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
page_id_t BPLUSTREE_TYPE::GetNextPageId(N *node) {
  page_id_t answer{-1};
  if (node != nullptr) {
    if (node->IsLeafPage()) {
      answer = node->GetNextPageId();
    } else {
      InternalPage *node_ = reinterpret_cast<InternalPage *>(node);
      int traverse_height{-1};
      int branch_index{-1};
      Page *branch_page = GoToBranchLayer(node_, &branch_index, &traverse_height, false);
      if (branch_page == nullptr) {
        answer = INVALID_PAGE_ID;
      } else {
        answer = GoDown(branch_page, branch_index, traverse_height, true);
      }
    }
  }
  return answer;
}

// go to the branch layer where the node(as the right sibling) and its
// left sibling join
// if the node is in the highest node(root), it will return nullptr
// right means go up from right to middle
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
Page *BPLUSTREE_TYPE::GoToBranchLayer(N *node, int *branch_index, int *traverse_height, bool go_from_right,
                                      KeyType *right_key) {
  page_id_t parent_page_id{-1};
  page_id_t current_page_id{-1};
  int current_page_position{-1};
  Page *parent_page{nullptr};
  InternalPage *parent_node{nullptr};
  Page *answer{nullptr};
  int height{0};
  int compare_value{-1};
  bool right_key_set{false};
  parent_page_id = node->GetParentPageId();
  current_page_id = node->GetPageId();
  while (parent_page_id != INVALID_PAGE_ID) {
    parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    current_page_position = parent_node->ValueIndex(current_page_id);
    if (right_key != nullptr && !right_key_set && go_from_right && current_page_position == 0 &&
        parent_node->GetSize() > 1) {
      *right_key = parent_node->KeyAt(1);
      right_key_set = true;
    }
    height++;
    compare_value = go_from_right ? 0 : (parent_node->GetSize() - 1);
    if (current_page_position != compare_value) {
      if (branch_index) {
        if (go_from_right) {
          (*branch_index) = current_page_position;
        } else {
          (*branch_index) = current_page_position + 1;
        }
      }
      answer = parent_page;
      break;
    }
    current_page_id = parent_page_id;
    parent_page_id = parent_node->GetParentPageId();
    buffer_pool_manager_->UnpinPage(current_page_id, false);
  }
  if (traverse_height) {
    (*traverse_height) = height;
  }
  return answer;
}

/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
  return WhichLeafPage(key, leftMost, true);
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::WhichLeafPage(const KeyType &key, bool leftMost, bool precise_key, OperationType operation,
                                    page_id_t *dangerous_page) {
  Page *result = nullptr;
  Page *root_page_holder;
  Page *internal_page_holder;
  Page *previous_page_holder{virtual_page};
  page_id_t next;
  LeafPage *leaf_page;
  InternalPage *internal_page;
  root_page_holder = ReadFetch(root_page_id_);
  internal_page_holder = root_page_holder;

  if (leftMost) {
    for (int i = 0; i < tree_height - 1; i++) {
      internal_page = reinterpret_cast<InternalPage *>(internal_page_holder->GetData());
      next = internal_page->ValueAt(0);
      previous_page_holder = internal_page_holder;
      internal_page_holder = ReadFetch(next, previous_page_holder, true);
    }
    result = internal_page_holder;
  } else {
    int i = 0;
    for (; i < tree_height - 1; i++) {
      internal_page = reinterpret_cast<InternalPage *>(internal_page_holder->GetData());
      if (internal_page->GetSize() == 1) {
        next = internal_page->ValueAt(0);
      } else {
        next = (reinterpret_cast<InternalPage *>(internal_page_holder->GetData()))->Lookup(key, comparator_);
      }
      if (next == INVALID_PAGE_ID) {
        break;
      }
      previous_page_holder = internal_page_holder;
      internal_page_holder = ReadFetch(next, previous_page_holder, true);
    }
    if (i != tree_height - 1) {
      RUnlatchAndUnpin(internal_page_holder);
      result = nullptr;
    } else {  //  这个地方没有使用unpin，因为page指针返回了，需要考虑一下这个问题
      leaf_page = reinterpret_cast<LeafPage *>(internal_page_holder->GetData());
      if (!precise_key || leaf_page->Lookup(key, nullptr, comparator_)) {
        result = internal_page_holder;
      } else {
        RUnlatchAndUnpin(internal_page_holder);
        result = nullptr;
      }
    }
  }
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPageGetValue(const KeyType &key) {
  Page *result = nullptr;
  Page *internal_page_holder{(GetPreRootPage())};
  Page *previous_page_holder;
  page_id_t next{root_page_id_};  // 只要pre_root_page的R latch拿着没放，说明此时不会有W latch。
  // 这就意味着W latch必然在root及以下。root page id会变的情况只可能是pre root 的W latch在拿着。所以
  // 这个时候可以放心使用root page id
  InternalPage *internal_page;
  LeafPage *leaf_page{nullptr};

  int i = 0;
  for (; i < tree_height - 1; i++) {
    previous_page_holder = internal_page_holder;
    internal_page_holder = ReadFetch(next, previous_page_holder);
    internal_page = GetInternalPage(internal_page_holder);
    if (internal_page->GetSize() == 1) {
      next = internal_page->ValueAt(0);
    } else {
      next = internal_page->Lookup(key, comparator_);
    }
    if (next == INVALID_PAGE_ID) {
      break;
    }
  }
  if (i != tree_height - 1) {
    RUnlatchAndUnpin(internal_page_holder);
    result = nullptr;
  } else {
    previous_page_holder = internal_page_holder;
    internal_page_holder = ReadFetch(next, previous_page_holder);
    leaf_page = GetLeafPage(internal_page_holder);
    if (leaf_page->Lookup(key, nullptr, comparator_)) {
      result = internal_page_holder;
    } else {
      RUnlatchAndUnpin(internal_page_holder);
      result = nullptr;
    }
  }

  return result;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPageToInsert(const KeyType &key, std::vector<page_id_t> *w_latched_pages) {
  Page *result = nullptr;
  Page *internal_page_holder;
  Page *previous_page_holder{(GetPreRootPage())};
  page_id_t next;
  InternalPage *internal_page;
  // InternalPage *previous_page;
  bool latch_point_found = false;
  // bool previous_page_latch_released = false;
  w_latched_pages->push_back((GetPreRootPage())->GetPageId());
  // root_page_lock.lock();
  next = root_page_id_;

  internal_page_holder = (GetPreRootPage());
  int i = 0;
  for (; i < tree_height - 1; i++) {
    // previous_page_latch_released = false;
    previous_page_holder = internal_page_holder;
    internal_page_holder = GetPage(next);
    internal_page = GetInternalPage(internal_page_holder);
    internal_page_holder->WLatch();
    if (!latch_point_found) {
      if (internal_page->GetSize() < internal_page->GetMaxSize() - 1) {  // previous page can be unlatched
        previous_page_holder->WUnlatch();
        ReleasePage(previous_page_holder);
        w_latched_pages->pop_back();
        // previous_page_latch_released = true;
        w_latched_pages->push_back(internal_page_holder->GetPageId());
      } else {  // the previous page becomes the latch point
        latch_point_found = true;
        w_latched_pages->push_back(internal_page_holder->GetPageId());
      }
    } else {
      w_latched_pages->push_back(internal_page_holder->GetPageId());
    }
    if (internal_page->GetSize() == 1) {
      next = internal_page->ValueAt(0);
    } else {
      next = internal_page->Lookup(key, comparator_);
    }
    if (next == INVALID_PAGE_ID) {
      break;
    }
    //    if (previous_page_holder->GetPageId() == root_page_id_ && previous_page_latch_released) {
    //    *root_page_locked = false;
    //  root_page_lock.unlock();
    //}
  }

  if (i != tree_height - 1) {
    result = nullptr;
  } else {
    result = GetPage(next);
    auto leaf = GetLeafPage(result);
    result->WLatch();
    if (leaf->GetSize() < leaf->GetMaxSize() - 1) {
      internal_page_holder->WUnlatch();
      w_latched_pages->pop_back();
    }
    w_latched_pages->push_back(result->GetPageId());
  }
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::SingleThreadWhichLeafPage(const KeyType &key, bool leftMost, bool precise_key) {
  Page *result = nullptr;
  Page *leaf_page_holder;
  Page *root_page_holder;
  Page *internal_page_holder;
  page_id_t next;
  LeafPage *leaf_page;
  root_page_holder = buffer_pool_manager_->FetchPage(root_page_id_);
  internal_page_holder = root_page_holder;

  if (leftMost) {
    InternalPage *internal_page;
    for (int i = 0; i < tree_height - 1; i++) {
      internal_page = reinterpret_cast<InternalPage *>(internal_page_holder->GetData());
      next = internal_page->ValueAt(0);
      buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
      internal_page_holder = buffer_pool_manager_->FetchPage(next);
    }
    result = internal_page_holder;
  } else {
    int i = 0;
    for (; i < tree_height - 1; i++) {
      auto internal_page = reinterpret_cast<InternalPage *>(internal_page_holder->GetData());
      if (internal_page->GetSize() == 1) {
        next = internal_page->ValueAt(0);
      } else {
        next = (reinterpret_cast<InternalPage *>(internal_page_holder->GetData()))->Lookup(key, comparator_);
      }
      buffer_pool_manager_->UnpinPage(internal_page_holder->GetPageId(), false);
      if (next == INVALID_PAGE_ID) {
        break;
      }
      internal_page_holder = buffer_pool_manager_->FetchPage(next);
    }
    if (i != tree_height - 1) {
      result = nullptr;
    } else {  //  这个地方没有使用unpin，因为page指针返回了，需要考虑一下这个问题
      leaf_page_holder = internal_page_holder;
      leaf_page = reinterpret_cast<LeafPage *>(leaf_page_holder->GetData());
      if (!precise_key || leaf_page->Lookup(key, nullptr, comparator_)) {
        result = leaf_page_holder;
      } else {
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
        result = nullptr;
      }
    }
  }

  return result;
}

INDEX_TEMPLATE_ARGUMENTS
page_id_t BPLUSTREE_TYPE::GoDown(Page *branch_page, int branch_index, int traverse_height, bool go_right_down) {
  Page *child_page{nullptr};
  InternalPage *child_node{nullptr};
  page_id_t child_page_id{-1};
  InternalPage *branch_node = reinterpret_cast<InternalPage *>(branch_page->GetData());
  if (go_right_down) {
    child_page_id = branch_node->ValueAt(branch_index);
  } else {
    child_page_id = branch_node->ValueAt(branch_index - 1);
  }
  buffer_pool_manager_->UnpinPage(branch_node->GetPageId(), false);
  traverse_height--;
  while (traverse_height != 0) {
    child_page = buffer_pool_manager_->FetchPage(child_page_id);
    child_node = reinterpret_cast<InternalPage *>(child_page->GetData());
    auto page_to_unpin = child_page_id;
    int index{-1};
    index = go_right_down ? 0 : child_node->GetSize() - 1;
    child_page_id = child_node->ValueAt(index);
    traverse_height--;
    buffer_pool_manager_->UnpinPage(page_to_unpin, false);
  }
  return child_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
page_id_t BPLUSTREE_TYPE::GoLeftBoundary() {
  InternalPage *previous_page;
  page_id_t current_page_id;
  Page *current_page_holder = GetPage(root_page_id_);
  InternalPage *current_page = GetInternalPage(root_page_id_);
  while (!current_page->IsLeafPage()) {
    previous_page = current_page;
    current_page_id = current_page->ValueAt(0);
    current_page_holder = GetPage(current_page_id);
    current_page = GetInternalPage(current_page_holder);
    UnPinPage(previous_page->GetPageId());
  }
  current_page_id = current_page_holder->GetPageId();
  UnPinPage(current_page_id);
  return current_page_id;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
