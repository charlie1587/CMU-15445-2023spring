#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
//  throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  // 创建一个指针指向根节点
  std::shared_ptr<const TrieNode> cur = root_;
  // 遍历key
  for (auto c : key) {
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
    return node->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //  throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  //-------------------------------------------------------------------------
  // 创建一个新的Trie和一个指针指向这个新Trie根节点的共享指针
  Trie new_trie;
  std::shared_ptr<const TrieNode> new_cur = new_trie.root_;
  // 初始化new_trie的根节点
  if (new_cur == nullptr) {
    new_cur = std::make_shared<TrieNode>();
  }
  // 创建一个指针指向根节点
  std::shared_ptr<const TrieNode> cur = root_;
  // 判断是不是要新创建节点：0表示不创建，1表示创建
  int flag = 0;
  //-------------------------------------------------------------------------
  // 遍历key
  for (auto c : key) {
    // 如果当前节点的孩子中没有c，说明key不存在
    if (flag||cur->children_.find(c) == cur->children_.end()) {
      // 创建一个新的孩子节点
      std::shared_ptr<TrieNode> new_child = std::make_shared<TrieNode>();
      // 将新孩子节点加入到new_cur的孩子中
      new_cur->children_.insert(std::make_pair(c, new_child));
      flag=1;
    } else {
      // 拷贝cur指向c对应的孩子节点
      std::shared_ptr<TrieNode> new_child = cur->children_.at(c)->Clone();
      // 如果new_child是数值节点，还要拷贝值信息
      if (new_child->is_value_node_) {
        auto node = dynamic_cast<const TrieNodeWithValue<T> *>(new_child.get());
        new_child = std::make_shared<TrieNodeWithValue<T>>(new_child->children_, std::make_shared<T>(*node->value_));
        // 插入
        new_cur->children_.insert(std::make_pair(c, new_child));
      }
      else{
        // 插入
        new_cur->children_.insert(std::make_pair(c, new_child));
      }
    }
    // 把cur除了孩子节点外的其他孩子指针都拷贝到new_cur中
    for (auto &child : cur->children_) {
      if (child.first != c) {
        new_cur->children_.insert(std::make_pair(child.first, child.second));
      }
    }
    // 移动cur和new_cur
    if(!flag)cur = cur->children_.at(c);
    new_cur = new_cur->children_.at(c);
  }
  // 检查当前的new_cur是不是值节点
  //如果是的话用value覆盖；如果不是的话，修改为值节点，值为value
  if (new_cur->is_value_node_) {
    new_cur->value_ = std::move(value);
  } else {
    new_cur = std::make_shared<TrieNodeWithValue<T>>(new_cur->children_, std::make_shared<T>(std::move(value)));
  }
  //-------------------------------------------------------------------------
  // 返回新建立的Trie
  return new_trie;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
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
