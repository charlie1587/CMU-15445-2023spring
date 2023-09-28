#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // 创建一个指针指向根节点
  auto cur = root_;
  if (root_ == nullptr) {
    return nullptr;
  }
  // 遍历key
  for (auto c : key) {
    // std::cout<<c<<std::endl;
    // 如果当前节点的孩子中没有c，说明key不存在
    if (cur->children_.find(c) == cur->children_.end()) {
      return nullptr;
    }

    // 否则，将当前节点指向c对应的孩子
    cur = cur->children_.at(c);
  }
  // 如果当前节点是值节点，返回值
  if (cur->is_value_node_) {
    auto node = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
    // T and the type of the value doesn't match
    if (node == nullptr) {
      return nullptr;
    }
    return node->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  Trie new_trie;
  auto val_ptr = std::make_shared<T>(std::move(value));
  auto new_cur = root_ ? root_->Clone() : std::make_shared<TrieNode>();
  std::shared_ptr<TrieNode> parent;
  new_trie.root_ = new_cur;

  for (auto c : key) {
    auto &children = new_cur->children_;
    if (children.find(c) != children.end()) {
      parent = new_cur;
      auto new_node = std::shared_ptr<TrieNode>(children[c]->Clone());
      children[c] = new_node;
      new_cur = new_node;
    } else {
      parent = new_cur;
      auto new_node = std::make_shared<TrieNode>();
      children[c] = new_node;
      new_cur = new_node;
    }
  }
  if (!key.empty()) {
    auto ch = key.back();
    parent->children_[ch] = std::make_shared<TrieNodeWithValue<T>>(std::move(new_cur->children_), std::move(val_ptr));
  } else {
    new_trie.root_ = std::make_shared<TrieNodeWithValue<T>>(std::move(new_cur->children_), std::move(val_ptr));
  }
  return new_trie;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  Trie new_trie;
  auto cur = root_;
  new_trie.root_ = cur;
  // record the path
  std::vector<std::shared_ptr<TrieNode>> vec;

  // Step 1: Create the vec
  for (auto c : key) {
    if (cur->children_.find(c) != cur->children_.end()) {
      vec.push_back(cur->Clone());
      cur = cur->children_.at(c);
    } else {
      return new_trie;  // can't find the key
    }
  }
  vec.push_back(cur->Clone());
  // Step 2: Connect the vec
  for (int i = 0; i < static_cast<int>(vec.size()) - 1; i++) {
    vec[i]->children_[key[i]] = vec[i + 1];
  }
  new_trie.root_ = vec[0];
  // Step 3: Remove the TrieNode
  bool flag;
  if (!vec.back()->children_.empty()) {
    // TODO(Tang): should change type
    auto node = std::make_shared<TrieNode>();
    node->children_ = vec.back()->children_;
    vec[static_cast<int>(vec.size()) - 2]->children_[key[static_cast<int>(key.size()) - 1]] = node;
    flag = false;
  } else {
    flag = true;
  }
  for (int i = key.size() - 1; i >= 0; i--) {
    if (flag && vec[i]->children_.size() == 1) {
      vec[i]->children_.erase(key[i]);
      if (vec[i]->is_value_node_) {
        flag = false;
      }
    } else if (flag) {
      vec[i]->children_.erase(key[i]);
      flag = false;
    }
  }
  return new_trie;
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
