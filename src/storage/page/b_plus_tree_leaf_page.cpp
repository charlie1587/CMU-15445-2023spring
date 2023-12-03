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
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IndexAt(const KeyType &key, const KeyComparator &comparator) const -> int {
  auto cmp = [&](const MappingType &element, const KeyType &val) -> bool { return comparator(element.first, val) < 0; };
  return std::lower_bound(array_, array_ + GetSize(), key, cmp) - array_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ReduceToHalf() {
  SetSize(GetSize()/2);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, ValueType *value, const KeyComparator &comparator) const
    -> bool {
  //  auto cmp = [&](const MappingType &element, const KeyType &val) -> bool { return comparator(element.first, val) <
  //  0; }; int id = std::lower_bound(array_, array_ + GetSize(), key, cmp) - array_;
  int id = IndexAt(key, comparator);
  if (id == GetSize() || comparator(KeyAt(id), key) != 0) {
    return false;
  }
  *value = array_[id].second;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertValue(const KeyType &key, const ValueType &value,
                                             const KeyComparator &comparator) -> bool {
  //  auto cmp = [&](const MappingType &element, const KeyType &val) -> bool { return comparator(element.first, val) <
  //  0; }; int id = std::lower_bound(array_, array_ + GetSize(), key, cmp) - array_;
  int id = IndexAt(key, comparator);
  if (id != GetSize() && comparator(KeyAt(id), key) == 0) {
    return false;
  }
  auto size = GetSize();
  IncreaseSize(1);
  for (int i = size - 1; i >= id; --i) {
    array_[i + 1] = array_[i];
  }
  array_[id] = {key, value};
  return true;
};

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAtBack(const KeyType &key, const ValueType &value) {
  int back_id = GetSize();
  array_[back_id] = {key, value};
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAtBack(const MappingType &pair) {
  int back_id = GetSize();
  array_[back_id] = pair;
  IncreaseSize(1);
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
