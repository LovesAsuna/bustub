#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  WritePageGuard leaf_page_guard = FindLeafPageByOperation(key, Operation::FIND, txn).first;
  auto leaf_node = leaf_page_guard.As<LeafPage>();

  ValueType value{};
  bool is_exist = leaf_node->Lookup(key, &value, comparator_);

  bpm_->UnpinPage(leaf_page_guard.PageId(), false);

  if (!is_exist) {
    return false;
  }
  result->push_back(value);
  return true;
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
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, txn);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_page_id = INVALID_PAGE_ID;
  auto new_page_guard = bpm_->NewPageGuarded(&new_page_id);

  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = new_page_id;

  auto leaf_page = new_page_guard.AsMut<LeafPage>();
  leaf_page->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  leaf_page->Insert(key, value, comparator_);
  bpm_->UnpinPage(new_page_guard.PageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *txn, bool *root_is_latched) {
  if (old_node->IsRootPage()) {
    page_id_t new_page_id = INVALID_PAGE_ID;
    auto new_page_guard = bpm_->NewPageGuarded(&new_page_id);

    UpdateRootPageId(new_page_id);

    auto new_root_node = new_page_guard.AsMut<InternalPage>();
    new_root_node->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);

    bpm_->UnpinPage(new_page_guard.PageId(), true);

    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }

    UnlockPages(txn);
    return;
  }

  auto parent_page_guard = bpm_->FetchPageWrite(old_node->GetParentPageId());
  auto parent_node = parent_page_guard.AsMut<InternalPage>();

  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }

    UnlockPages(txn);
    bpm_->UnpinPage(parent_page_guard.PageId(), true);
    return;
  }

  InternalPage *new_parent_node = Split(parent_node);

  InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, txn, root_is_latched);

  bpm_->UnpinPage(parent_page_guard.PageId(), true);
  bpm_->UnpinPage(new_parent_node->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  auto pair = FindLeafPageByOperation(key, Operation::INSERT, txn);
  WritePageGuard leaf_page_guard = std::move(pair.first);
  bool root_is_latched = pair.second;

  auto leaf_node = leaf_page_guard.AsMut<LeafPage>();
  int size = leaf_node->GetSize();
  int new_size = leaf_node->Insert(key, value, comparator_);
  if (new_size == size) {
    if (root_is_latched) {
      root_latch_.unlock();
    }
    UnlockUnpinPages(txn);
    leaf_page_guard.WUnlatch();
    bpm_->UnpinPage(leaf_page_guard.PageId(), false);
    return false;
  }

  if (new_size < leaf_node->GetMaxSize()) {
    if (root_is_latched) {
      root_latch_.unlock();
    }
    leaf_page_guard.WUnlatch();
    bpm_->UnpinPage(leaf_page_guard.PageId(), true);
    return true;
  }

  LeafPage *new_leaf_node = Split(leaf_node);

  bool *pointer_root_is_latched = new bool(root_is_latched);

  InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node, txn, pointer_root_is_latched);

  assert((*pointer_root_is_latched) == false);

  delete pointer_root_is_latched;

  leaf_page_guard.WUnlatch();
  bpm_->UnpinPage(leaf_page_guard.PageId(), true);
  bpm_->UnpinPage(new_leaf_node->GetPageId(), true);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  page_id_t new_page_id = INVALID_PAGE_ID;
  auto new_page_guard = bpm_->NewPageGuarded(&new_page_id);

  auto new_node = new_page_guard.AsMut<N>();
  new_node->SetPageType(node->GetPageType());

  if (node->IsLeafPage()) {
    auto old_leaf_node = reinterpret_cast<LeafPage *>(node);
    auto new_leaf_node = reinterpret_cast<LeafPage *>(new_node);

    new_leaf_node->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
    old_leaf_node->MoveHalfTo(new_leaf_node);
    new_leaf_node->SetNextPageId(old_leaf_node->GetNextPageId());
    old_leaf_node->SetNextPageId(new_leaf_node->GetPageId());
    new_node = reinterpret_cast<N *>(new_leaf_node);
  } else {
    auto old_internal_node = reinterpret_cast<InternalPage *>(node);
    auto new_internal_node = reinterpret_cast<InternalPage *>(new_node);
    new_internal_node->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
    old_internal_node->MoveHalfTo(new_internal_node, bpm_);
    new_node = reinterpret_cast<N *>(new_internal_node);
  }

  // fetch page and new page need to unpin page (do it outside)

  return new_node;  // 注意，此时new_node还没有unpin
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) -> WritePageGuard {
  return FindLeafPageByOperation(key, Operation::FIND, nullptr, leftMost, false).first;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageByOperation(const KeyType &key, Operation operation, Transaction *txn, bool leftMost,
                                             bool rightMost) -> std::pair<WritePageGuard, bool> {
  assert(operation == Operation::FIND ? !(leftMost && rightMost) : txn != nullptr);

  root_latch_.lock();
  bool is_root_page_id_latched = true;

  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();

  auto page_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  auto node = page_guard.As<BPlusTreePage>();

  if (operation == Operation::FIND) {
    page_guard.RLatch();
    is_root_page_id_latched = false;
    root_latch_.unlock();
  } else {
    page_guard.WLatch();
    if (IsSafe(node, operation)) {
      is_root_page_id_latched = false;
      root_latch_.unlock();
    }
  }
  while (!node->IsLeafPage()) {
    auto i_node = page_guard.As<InternalPage>();

    page_id_t child_node_page_id;
    if (leftMost) {
      child_node_page_id = i_node->ValueAt(0);
    } else if (rightMost) {
      child_node_page_id = i_node->ValueAt(i_node->GetSize() - 1);
    } else {
      child_node_page_id = i_node->Lookup(key, comparator_);
    }

    auto child_page_guard = bpm_->FetchPageWrite(child_node_page_id);
    auto child_node = child_page_guard.As<BPlusTreePage>();

    if (operation == Operation::FIND) {
      child_page_guard.RLatch();
      page_guard.RUnlatch();
      bpm_->UnpinPage(page_guard.PageId(), false);
    } else {
      child_page_guard.WLatch();
      txn->AddIntoPageSet(page_guard.GetPage());
      // child node is safe, release all locks on ancestors
      if (IsSafe(child_node, operation)) {
        if (is_root_page_id_latched) {
          is_root_page_id_latched = false;
          root_latch_.unlock();
        }
        UnlockUnpinPages(txn);
      }
    }
    page_guard = std::move(child_page_guard);
    node = child_node;
  }

  return std::make_pair(std::move(page_guard), is_root_page_id_latched);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  if (IsEmpty()) {
    return;
  }

  auto pair = FindLeafPageByOperation(key, Operation::DELETE, txn);
  WritePageGuard leaf_page_guard = std::move(pair.first);
  bool root_is_latched = pair.second;

  auto leaf_node = leaf_page_guard.AsMut<LeafPage>();
  int old_size = leaf_node->GetSize();
  int new_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);

  if (new_size == old_size) {
    if (root_is_latched) {
      root_latch_.unlock();
    }
    UnlockUnpinPages(txn);

    leaf_page_guard.WUnlatch();
    bpm_->UnpinPage(leaf_page_guard.PageId(), false);
    return;
  }

  bool *pointer_root_is_latched = new bool(root_is_latched);

  bool leaf_should_delete = CoalesceOrRedistribute(leaf_node, txn, pointer_root_is_latched);
  // NOTE: unlock and unpin are finished in CoalesceOrRedistribute
  // NOTE: root node must be unlocked in CoalesceOrRedistribute
  assert((*pointer_root_is_latched) == false);

  delete pointer_root_is_latched;

  if (leaf_should_delete) {
    txn->AddIntoDeletedPageSet(leaf_page_guard.PageId());
  }

  leaf_page_guard.WUnlatch();
  bpm_->UnpinPage(leaf_page_guard.PageId(), true);

  // NOTE: ensure deleted pages have been unpined
  for (page_id_t page_id : *txn->GetDeletedPageSet()) {
    bpm_->DeletePage(page_id);
  }
  txn->GetDeletedPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *txn, bool *root_is_latched) -> bool {
  if (node->IsRootPage()) {
    bool root_should_delete = AdjustRoot(node);

    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }

    UnlockPages(txn);
    return root_should_delete;  // NOTE: size of root page can be less than min size
  }

  if (node->GetSize() >= node->GetMinSize()) {
    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }

    UnlockPages(txn);
    return false;
  }

  WritePageGuard parent_page_guard = bpm_->FetchPageWrite(node->GetParentPageId());
  auto parent = parent_page_guard.AsMut<InternalPage>();

  int index = parent->ValueIndex(node->GetPageId());
  page_id_t sibling_page_id = parent->ValueAt(index == 0 ? 1 : index - 1);
  WritePageGuard sibling_page_guard = bpm_->FetchPageWrite(sibling_page_id);

  sibling_page_guard.WLatch();

  auto sibling_node = sibling_page_guard.AsMut<N>();

  if (node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize()) {
    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }

    Redistribute(sibling_node, node, index);

    UnlockPages(txn);
    bpm_->UnpinPage(parent_page_guard.PageId(), true);

    sibling_page_guard.WUnlatch();
    bpm_->UnpinPage(sibling_page_guard.PageId(), true);

    return false;
  }

  bool parent_should_delete = Coalesce(&sibling_node, &node, &parent, index, txn, root_is_latched);
  assert((*root_is_latched) == false);

  if (parent_should_delete) {
    txn->AddIntoDeletedPageSet(parent->GetPageId());
  }

  // NOTE: parent unlock is finished in Coalesce
  bpm_->UnpinPage(parent_page_guard.PageId(), true);

  sibling_page_guard.WUnlatch();
  bpm_->UnpinPage(sibling_page_guard.PageId(), true);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node, InternalPage **parent, int index, Transaction *txn,
                              bool *root_is_latched) -> bool {
  int key_index = index;
  if (index == 0) {
    std::swap(neighbor_node, node);
    key_index = 1;
  }
  KeyType middle_key = (*parent)->KeyAt(key_index);

  if ((*node)->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(*node);
    auto neighbor_leaf_node = reinterpret_cast<LeafPage *>(*neighbor_node);
    leaf_node->MoveAllTo(neighbor_leaf_node);
    neighbor_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
  } else {
    auto internal_node = reinterpret_cast<InternalPage *>(*node);
    auto neighbor_internal_node = reinterpret_cast<InternalPage *>(*neighbor_node);
    internal_node->MoveAllTo(neighbor_internal_node, middle_key, bpm_);
  }

  (*parent)->Remove(key_index);

  return CoalesceOrRedistribute(*parent, txn, root_is_latched);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  BasicPageGuard parent_page_guard = bpm_->FetchPageBasic(node->GetParentPageId());
  auto parent = parent_page_guard.AsMut<InternalPage>();

  if (node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(node);
    auto neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if (index == 0) {
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
      parent->SetKeyAt(1, neighbor_leaf_node->KeyAt(0));
    } else {
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    auto neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (index == 0) {
      neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(1), bpm_);
      parent->SetKeyAt(1, neighbor_internal_node->KeyAt(0));
    } else {
      neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index), bpm_);
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    }
  }
  bpm_->UnpinPage(parent_page_guard.PageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) -> bool {
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    auto internal_node = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t child_page_id = internal_node->RemoveAndReturnOnlyChild();

    // NOTE: don't need to unpin old_root_node, this operation will be done in CoalesceOrRedistribute function
    // buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);

    // update root page id
    UpdateRootPageId(child_page_id);
    // update parent page id of new root node
    BasicPageGuard new_root_page_guard = bpm_->FetchPageBasic(GetRootPageId());
    auto new_root_node = new_root_page_guard.AsMut<InternalPage>();
    new_root_node->SetParentPageId(INVALID_PAGE_ID);

    bpm_->UnpinPage(new_root_page_guard.PageId(), true);
    return true;
  }

  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    // NOTE: don't need to unpin old_root_node, this operation will be done in Remove function
    UpdateRootPageId(INVALID_PAGE_ID);

    return true;
  }

  return false;
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  WritePageGuard leaf_page_guard = FindLeafPageByOperation(KeyType(), Operation::FIND, nullptr, true).first;
  return INDEXITERATOR_TYPE(bpm_, std::move(leaf_page_guard), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  WritePageGuard leaf_page_guard = FindLeafPageByOperation(key, Operation::FIND).first;
  auto leaf_node = leaf_page_guard.As<LeafPage>();
  int index = leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(bpm_, std::move(leaf_page_guard), index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  WritePageGuard leaf_page_guard = FindLeafPageByOperation(KeyType(), Operation::FIND, nullptr, false, true).first;
  auto leaf_node = leaf_page_guard.As<LeafPage>();
  return INDEXITERATOR_TYPE(bpm_, std::move(leaf_page_guard), leaf_node->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS

auto BPLUSTREE_TYPE::UpdateRootPageId(page_id_t page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }

  for (Page *page : *transaction->GetPageSet()) {
    page->WUnlatch();
  }
  transaction->GetPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }

  for (Page *page : *transaction->GetPageSet()) {
    page->WUnlatch();
    bpm_->UnpinPage(page->GetPageId(), false);
  }
  transaction->GetPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::IsSafe(N *node, Operation op) -> bool {
  if (node->IsRootPage()) {
    return (op == Operation::INSERT && node->GetSize() < node->GetMaxSize() - 1) ||
           (op == Operation::DELETE && node->GetSize() > 2);
  }

  if (op == Operation::INSERT) {
    return node->GetSize() < node->GetMaxSize() - 1;
  }

  if (op == Operation::DELETE) {
    return node->GetSize() > node->GetMinSize();
  }

  return true;
}
/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
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
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
