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

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      tree_height(0) {}

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
  if (!IsEmpty()) {
    Page *leaf_page_holder;
    Page *root_page_holder;
    Page *internal_page_holder;
    page_id_t next;
    LeafPage *leaf_page;
    root_page_holder = buffer_pool_manager_->FetchPage(root_page_id_);
    internal_page_holder = root_page_holder;

    int i = 0;
    for (; i < tree_height - 1; i++) {
      next = (reinterpret_cast<InternalPage *>(internal_page_holder->GetData()))->Lookup(key, comparator_);
      buffer_pool_manager_->UnpinPage(internal_page_holder->GetPageId(), false);
      if (next == INVALID_PAGE_ID) {
        break;
      }
      internal_page_holder = buffer_pool_manager_->FetchPage(next);
    }
    if (i == tree_height - 1) {
      leaf_page_holder = internal_page_holder;
      leaf_page = reinterpret_cast<LeafPage *>(leaf_page_holder->GetData());
      ValueType res;
      bool exist = leaf_page->Lookup(key, &res, comparator_);
      buffer_pool_manager_->UnpinPage(internal_page_holder->GetPageId(), false);
      if (exist) {
        result->push_back(res);
        return_value = true;
      }
    }
  }
  return return_value;
}

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
  if (IsEmpty()) {
    StartNewTree(key, value);
  } else {
    success = InsertIntoLeaf(key, value);
  }
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
  Page *leaf_page_holder;
  Page *root_page_holder;
  Page *internal_page_holder;
  page_id_t next;
  LeafPage *leaf_page;
  root_page_holder = buffer_pool_manager_->FetchPage(root_page_id_);
  internal_page_holder = root_page_holder;

  int i = 0;
  for (; i < tree_height - 1; i++) {
    next = reinterpret_cast<InternalPage *>(internal_page_holder->GetData())->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(internal_page_holder->GetPageId(), false);
    if (next == INVALID_PAGE_ID) {
      break;
    }
    internal_page_holder = buffer_pool_manager_->FetchPage(next);
  }

  if (i == tree_height - 1) {
    leaf_page_holder = internal_page_holder;
    leaf_page = reinterpret_cast<LeafPage *>(leaf_page_holder->GetData());
    bool find = leaf_page->Lookup(key, nullptr, comparator_);
    // if (!find) {                                              // not found in the leaf page
    //   if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {  // already full, needs splitting
    //     auto middle_key = leaf_page->GetMiddleKey();
    //     auto new_leaf_page = Split<LeafPage>(leaf_page);
    //     if (comparator_(key, middle_key) != -1) {
    //       new_leaf_page->Insert(key, value, comparator_);
    //     } else {
    //       leaf_page->Insert(key, value, comparator_);
    //     }
    //     buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
    //   } else {
    //     leaf_page->Insert(key, value, comparator_);
    //   }
    //   result = true;
    // }
    if (!find) {
      leaf_page->Insert(key, value, comparator_);
      if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
        auto new_leaf_page = Split<LeafPage>(leaf_page);
        buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
      }
      result = true;
    }
    buffer_pool_manager_->UnpinPage(leaf_page_holder->GetPageId(), true);
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
  KeyType middle_key = node->GetMiddleKey();
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  new_node->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_node->GetPageId());
  InsertIntoParent(node, middle_key, new_node);
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
  // assert(old_node->IsRootPage() == false);
  if (old_node->GetParentPageId() == INVALID_PAGE_ID) {  // this splits from root page as leaf page
    page_id_t new_root_page_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
    if (new_root_page == nullptr) {
      throw std::bad_alloc();
    }
    root_page_id_ = new_root_page_id;
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
    if (parent_node->GetSize() == parent_node->GetMaxSize()) {
      if (!parent_node->IsRootPage()) {
        InternalPage *new_parent_node = Split<InternalPage>(parent_node);
        int check = parent_node->ValueIndex(new_node->GetPageId());
        InternalPage *node_to_insert = (check != -1 ? parent_node : new_parent_node);
        node_to_insert->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(node_to_insert->GetPageId());
      } else {  // create new root page
        page_id_t new_root_page_id = INVALID_PAGE_ID;
        Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
        if (new_root_page == nullptr) {
          throw std::bad_alloc();
        }
        root_page_id_ = new_root_page_id;
        UpdateRootPageId();
        InternalPage *root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
        root_node->Init(new_root_page_id, INVALID_PAGE_ID, parent_node->GetMaxSize());

        parent_node->SetParentPageId(new_root_page_id);

        page_id_t the_other_parent_page_id;
        Page *the_other_parent_page = buffer_pool_manager_->NewPage(&the_other_parent_page_id);
        if (the_other_parent_page == nullptr) {
          throw std::bad_alloc();
        }
        InternalPage *the_other_parent_node = reinterpret_cast<InternalPage *>(the_other_parent_page->GetData());
        the_other_parent_node->Init(the_other_parent_page_id, new_root_page_id, parent_node->GetMaxSize());

        parent_node->MoveHalfTo(the_other_parent_node, buffer_pool_manager_);
        KeyType middle_key = the_other_parent_node->KeyAt(0);
        root_node->PopulateNewRoot(parent_page->GetPageId(), middle_key, the_other_parent_node->GetPageId());
        tree_height++;
        buffer_pool_manager_->UnpinPage(new_root_page_id, true);
        // find out the key should land on new or old parent
        int size_after_insert = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        if (size_after_insert != -1) {
          new_node->SetParentPageId(parent_node->GetPageId());
        } else {
          the_other_parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
          new_node->SetParentPageId(the_other_parent_page_id);
        }
        buffer_pool_manager_->UnpinPage(the_other_parent_page_id, true);
      }
    } else {
      parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      new_node->SetParentPageId(parent_node->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(old_node->GetParentPageId(), true);
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

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
  return false;
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
  return false;
}

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
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
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
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { return INDEXITERATOR_TYPE(); }

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
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
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
      next = reinterpret_cast<InternalPage *>(internal_page_holder->GetData())->Lookup(key, comparator_);
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
      if (leaf_page->Lookup(key, nullptr, comparator_)) {
        result = leaf_page_holder;
      } else {
        result = nullptr;
      }
    }
  }

  return result;
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
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
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
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
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
