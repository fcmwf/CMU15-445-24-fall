//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

#define DEBUG_INSERT
#define DEBUG_DELETE
#define TEMP_LOG
// #define DEBUG_INTERATOR

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
      root_page_id_ = INVALID_PAGE_ID;
      std::string insert_log_name = "insert.log";
      std::string delete_log_name = "delete.log";
      std::string temp_log_name = "temp.log";
      {
        std::ofstream ofs(insert_log_name, std::ios::out | std::ios::trunc);
        std::ofstream ofs2(delete_log_name, std::ios::out | std::ios::trunc);
        std::ofstream ofs3(temp_log_name, std::ios::out | std::ios::trunc);
      }
      insert_log.open(insert_log_name, std::ios::out | std::ios::app);
      delete_log.open(delete_log_name, std::ios::out | std::ios::app);
      temp_log.open(temp_log_name, std::ios::out | std::ios::app);
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  std::shared_lock<std::shared_mutex> lock(mutex_); 
  if(IsEmpty()) {
    return false;
  }
  ReadPageGuard read_page_guard;
  read_page_guard = bpm_->ReadPage(root_page_id_);
  auto node = reinterpret_cast< const BPlusTreePage *>(read_page_guard.GetData());
  while (!node->IsLeafPage()) {
      auto v =  reinterpret_cast<const InternalPage*>(node)->Lookup(key, comparator_);
      read_page_guard = bpm_->ReadPage(v);
      node = reinterpret_cast<const BPlusTreePage *>(read_page_guard.GetData());
  }
  auto n = reinterpret_cast< const LeafPage *>(node);
  std::optional<ValueType> v = n->Lookup(key, comparator_);
  if(v.has_value()){ 
    result->push_back(v.value());
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // std::cout <<((BUSTUB_PAGE_SIZE - 12) / ((int)(sizeof(KeyType) + sizeof(ValueType)))) << std::endl;
  // Declaration of context instance. Using the Context is not necessary but advised.
  // Context ctx;
  //Get root and check wheather it is existed
  Context ctx;
  #ifdef DEBUG_INSERT
  insert_log << std::endl << "Insert key: " << key << std::endl;
  insert_log << "value: " << value;
  #endif
  std::unique_lock<std::shared_mutex> lock(mutex_);
  {
    if(IsEmpty()){
      page_id_t page_id = bpm_->NewPage();
      #ifdef DEBUG_INSERT
      insert_log << "root is empty" << std::endl;
      insert_log << "page_id: " << page_id << std::endl;
      #endif
      WritePageGuard write_guard = bpm_->WritePage(page_id);
      LeafPage* root_page_leaf = reinterpret_cast<LeafPage *>(write_guard.GetDataMut());
      root_page_leaf->Init(page_id,leaf_max_size_);
      root_page_leaf->Insert(key, value, comparator_);
      SetRootPage(page_id);
      #ifdef DEBUG_INSERT
      root_page_leaf->NodePrint(insert_log);
      #endif
      return true;
    }
  }
  page_id_t leaf_page = FindLeaf(key, ctx, Operation::INSERT);
  if(!ctx.write_nodes_.count(GetRootPageId())){    // can't change root, so we can unlock, otherwise may have difference in result!
    lock.unlock();
  }
  BUSTUB_ASSERT(ctx.write_nodes_.count(leaf_page), "leaf page should be in write set");
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(ctx.write_nodes_[leaf_page]);
  #ifdef DEBUG_INSERT
  insert_log << "leaf size: " << leaf_node->GetSize() << std::endl;
  insert_log << "leaf maxsize: " << leaf_node->GetMaxSize() << std::endl;
  #endif
  if(!leaf_node->Insert(key,value,comparator_)){   //duplicate key
    #ifdef DEBUG_INSERT
    insert_log << "duplicate key!" << std::endl;
    #endif
    return false;
  }
  if(leaf_node->GetSize() > leaf_node->GetMaxSize()) {   // split leaf node
    #ifdef DEBUG_INSERT
    insert_log << "before split" << std::endl;
    leaf_node->NodePrint(insert_log);
    insert_log << "parent info:" << std::endl;
    page_id_t pa_id = leaf_node->GetParentId();
    if(!ctx.write_nodes_.count(pa_id)){
      WritePageGuard write_guard = bpm_->WritePage(pa_id);
      auto node = reinterpret_cast< BPlusTreePage *>(write_guard.GetDataMut());
      ctx.write_nodes_.insert({pa_id, node});
      ctx.write_set_.push_back(std::move(write_guard));
    }
    reinterpret_cast<InternalPage*>(ctx.write_nodes_[pa_id])->NodePrint(insert_log);
    #endif
    page_id_t new_page = SplitLeafNode(leaf_node->GetPageId(), ctx);
    BUSTUB_ASSERT(ctx.write_nodes_.count(new_page), "new page should be in write set");
    LeafPage* new_leaf_node = reinterpret_cast<LeafPage *>(ctx.write_nodes_[new_page]);
    #ifdef DEBUG_INSERT
    insert_log << "leaf need split" << std::endl;
    leaf_node->NodePrint(insert_log);
    new_leaf_node->NodePrint(insert_log);
    #endif
    // insert to parent 递归
    InsertLeaf2Parent(new_leaf_node->KeyAt(0), leaf_node->GetPageId(), new_leaf_node->GetPageId(), ctx);
    return true;
  }
  #ifdef DEBUG_INSERT
  leaf_node->NodePrint(insert_log);
  #endif
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertLeaf2Parent(const KeyType &key, page_id_t old_page, page_id_t new_page, Context &ctx) {
    BUSTUB_ASSERT(ctx.write_nodes_.count(old_page), "old page should be in write set");
    BUSTUB_ASSERT(ctx.write_nodes_.count(new_page), "new page should be in write set");
    LeafPage* old_node = reinterpret_cast<LeafPage *>(ctx.write_nodes_[old_page]);
    LeafPage* new_node = reinterpret_cast<LeafPage *>(ctx.write_nodes_[new_page]);
    if (old_node->GetPageId()==GetRootPageId()) {
        // std::lock_guard<std::shared_mutex> lock(mutex_);
        page_id_t pageId = bpm_->NewPage();
        WritePageGuard write_guard = bpm_->WritePage(pageId);
        InternalPage* root_page = reinterpret_cast<InternalPage *>(write_guard.GetDataMut());
        { // root set
          root_page->Init(pageId,internal_max_size_);
          root_page->SetSize(2);
          root_page->SetKeyAt(1, key);
          root_page->SetValueAt(0,old_node->GetPageId());
          root_page->SetValueAt(1,new_node->GetPageId());
        }
        root_page_id_ = pageId;
        old_node->SetParentId(root_page_id_);
        new_node->SetParentId(root_page_id_);
        SetRootPage(pageId);
        #ifdef DEBUG_INSERT
        insert_log << "Set new root page: " << pageId << std::endl;
        root_page->NodePrint(insert_log);
        #endif
        return;
    }
    new_node->SetParentId(old_node->GetParentId());
    page_id_t pa_page = old_node->GetParentId();
    BUSTUB_ASSERT(ctx.write_nodes_.count(pa_page), "parent page should be in write set");
    // if(!ctx.write_nodes_.count(pa_page)){
    //     WritePageGuard write_guard = bpm_->WritePage(pa_page);
    //     ctx.write_nodes_.insert({pa_page, reinterpret_cast<BPlusTreePage *>(write_guard.GetDataMut())});
    //     ctx.write_set_.push_back(std::move(write_guard));
    // }
    InternalPage * pa_node = reinterpret_cast<InternalPage *>(ctx.write_nodes_[pa_page]);
    pa_node->Insert(key, new_node->GetPageId(), comparator_);
    #ifdef DEBUG_INSERT
    pa_node->NodePrint(insert_log);
    #endif
    if (pa_node->GetSize() <= pa_node->GetMaxSize()) {
        return;
    }
    InsertInternalPage2Parent(pa_page, ctx);
    return;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInternalPage2Parent(page_id_t page_id, Context &ctx) {  // node should bbe splited and insert to their parent
  BUSTUB_ASSERT(ctx.write_nodes_.count(page_id), "page should be in write set");
  InternalPage* old_node = reinterpret_cast<InternalPage *>(ctx.write_nodes_[page_id]);
  page_id_t new_page = SplitInternalNode(page_id,ctx);
  BUSTUB_ASSERT(ctx.write_nodes_.count(new_page), "page should be in write set");
  InternalPage* new_node = reinterpret_cast<InternalPage *>(ctx.write_nodes_[new_page]);

  KeyType insert_key = new_node->KeyAt(0);
  if (old_node->GetPageId()==GetRootPageId()) {
    // std::lock_guard<std::shared_mutex> lock(mutex_);
    page_id_t pageId = bpm_->NewPage();
    WritePageGuard write_guard = bpm_->WritePage(pageId);
    InternalPage* root_page = reinterpret_cast<InternalPage *>(write_guard.GetDataMut());
    { // root set
      root_page->Init(pageId,internal_max_size_);
      root_page->SetSize(2);
      root_page->SetKeyAt(1, insert_key);
      root_page->SetValueAt(0,old_node->GetPageId());
      root_page->SetValueAt(1,new_node->GetPageId());
    }
    root_page_id_ = pageId;
    old_node->SetParentId(root_page_id_);
    new_node->SetParentId(root_page_id_);
    SetRootPage(pageId);
    #ifdef DEBUG_INSERT
    insert_log << "Set new root page: " << pageId << std::endl;
    root_page->NodePrint(insert_log);
    #endif
    return;
  }
  page_id_t pa_page = old_node->GetParentId();
  BUSTUB_ASSERT(ctx.write_nodes_.count(pa_page), "page should be in write set");
  // if(!ctx.write_nodes_.count(pa_page)){
  //   WritePageGuard write_guard = bpm_->WritePage(pa_page);
  //   ctx.write_nodes_.insert({pa_page, reinterpret_cast<BPlusTreePage *>(write_guard.GetDataMut())});
  //   ctx.write_set_.push_back(std::move(write_guard));
  // }
  InternalPage* parent_page = reinterpret_cast<InternalPage *>(ctx.write_nodes_[pa_page]);
  parent_page->Insert(insert_key,new_node->GetPageId(),comparator_);
  new_node->SetParentId(old_node->GetParentId());
  if(parent_page->GetSize() > parent_page->GetMaxSize()){
    InsertInternalPage2Parent(pa_page,ctx);
  }
  return;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafNode(page_id_t page_id, Context &ctx) -> page_id_t{
    BUSTUB_ASSERT(ctx.write_nodes_.count(page_id), "leaf page should be in write set");
    LeafPage* old_node = reinterpret_cast<LeafPage *>(ctx.write_nodes_[page_id]);
    // old_node->NodePrint();
    page_id_t pageId = bpm_->NewPage();
    WritePageGuard write_new_guard = bpm_->WritePage(pageId);
    LeafPage* new_node = reinterpret_cast<LeafPage *>(write_new_guard.GetDataMut());
    
    WritePageGuard write_guard_next;
    page_id_t leaf_next = old_node->GetNextPageId();
    if(leaf_next != INVALID_PAGE_ID){
      write_guard_next = bpm_->WritePage(leaf_next);
    }
    new_node->Init(pageId, leaf_max_size_);
    new_node->CopyLeafData(old_node->GetSize() / 2, old_node);
    new_node->SetSize(old_node->GetSize() - (old_node->GetSize()) / 2);
    old_node->SetSize(old_node->GetSize() / 2);

    new_node->SetNextPageId(leaf_next);
    new_node->SetPrevPageId(old_node->GetPageId());
    old_node->SetNextPageId(pageId);
    new_node->SetParentId(old_node->GetParentId());
    ctx.write_nodes_.insert({pageId, 
                            reinterpret_cast<BPlusTreePage *>(write_new_guard.GetDataMut())});
    ctx.write_set_.push_back(std::move(write_new_guard));
    if(leaf_next != INVALID_PAGE_ID){
      reinterpret_cast<LeafPage *>(write_guard_next.GetDataMut())->SetPrevPageId(pageId);
      ctx.write_nodes_.insert({write_guard_next.GetPageId(),
                              reinterpret_cast<BPlusTreePage *>(write_guard_next.GetDataMut())});
      ctx.write_set_.push_back(std::move(write_guard_next));
    }
    return pageId;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalNode(page_id_t page_id, Context &ctx) -> page_id_t{
    BUSTUB_ASSERT(ctx.write_nodes_.count(page_id), "page should be in write set");
    InternalPage* old_node = reinterpret_cast<InternalPage *>(ctx.write_nodes_[page_id]);
    page_id_t pageId = bpm_->NewPage();
    #ifdef DEBUG_INSERT
    insert_log << "new page id: " << pageId << std::endl;
    #endif
    WritePageGuard write_new_guard = bpm_->WritePage(pageId);
    InternalPage* new_node = reinterpret_cast<InternalPage *>(write_new_guard.GetDataMut());

    new_node->Init(pageId, internal_max_size_);
    new_node->CopyInternalData((old_node->GetSize())/2 , old_node);
    new_node->SetSize(old_node->GetSize() - (old_node->GetSize())/2);
    old_node->SetSize((old_node->GetSize())/2);
    new_node->SetParentId(old_node->GetParentId());

    for(int i=0; i<new_node->GetSize();i++){
      page_id_t page_id = new_node->ValueAt(i);
      #ifdef DEBUG_INSERT
      insert_log << "page id: " << page_id << std::endl;
      #endif
      if(!ctx.SetPageParent(page_id,pageId)){
        WritePageGuard guard = bpm_->WritePage(page_id);
        reinterpret_cast<BPlusTreePage*>(guard.GetDataMut())->SetParentId(pageId);
      }
    }

    ctx.write_nodes_.insert({pageId, 
                            reinterpret_cast<BPlusTreePage *>(write_new_guard.GetDataMut())});
    ctx.write_set_.push_back(std::move(write_new_guard));
    return pageId;
}



/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  #ifdef DEBUG_DELETE
  delete_log << std::endl << "cur root page id: " << GetRootPageId() << std::endl;
  delete_log << "remove key: " << key << std::endl;
  #endif
  std::unique_lock<std::shared_mutex> lock(mutex_);
  if(IsEmpty()){ return; }

  page_id_t leaf_page = FindLeaf(key,ctx,Operation::DELETE);
  if(!ctx.write_nodes_.count(GetRootPageId())){
    lock.unlock();
  }
  BUSTUB_ASSERT(ctx.write_nodes_.count(leaf_page), "page should be in write set");
  LeafPage *leaf_node = reinterpret_cast< LeafPage *>(ctx.write_nodes_[leaf_page]);
  #ifdef DEBUG_DELETE
  delete_log << "remove key in page:  " << leaf_node->GetPageId() << std::endl;
  leaf_node->NodePrint(delete_log);
  #endif
  if(!leaf_node->Remove(key,comparator_)){   
    // remove fail
    #ifdef DEBUG_DELETE        
    delete_log << "remove fail" << std::endl;
    #endif
    return;
  }

  if(GetRootPageId()==leaf_node->GetPageId()){   // Should I do this? Two test have different answer...
    if(leaf_node->GetSize()==0){
      SetRootPage(INVALID_PAGE_ID);
    }
    return;
  }
  if (leaf_node->GetSize() >= leaf_node->GetMinSize()) { 
    // node size enough or it is root page
    #ifdef DEBUG_DELETE        
    delete_log << "remove success" << std::endl;
    #endif
    return; 
  }

  auto leaf_siblings = LeafGetSibling(leaf_node->GetPageId(), ctx);
  // process leaf , try borrow sibling
  auto borrow_leaf_res = SiblingParser(leaf_siblings, PageParser::Borrow);
  if(borrow_leaf_res.has_value()){
    auto [sib_id, direct] = borrow_leaf_res.value();
    LeafBorrow(leaf_page, sib_id, direct, ctx);
    #ifdef DEBUG_DELETE
    delete_log << "borrow from sibling page: " << sib_id << std::endl;
    #endif
    return;    
  }
  // Merge sibling
  auto merge_leaf_res = SiblingParser(leaf_siblings, PageParser::Merge);
  BUSTUB_ASSERT(merge_leaf_res.has_value(),"no sibling to merge");
  auto [sib_id, direct] = merge_leaf_res.value();
  #ifdef DEBUG_DELETE
  delete_log << "remove key finish!" << std::endl;
  leaf_node->NodePrint(delete_log);
  delete_log << "merge sibling page: " << sib_id << std::endl;
  reinterpret_cast<const LeafPage *>(ctx.write_nodes_[sib_id])->NodePrint(delete_log);
  #endif
  if(direct){
    LeafMerge(leaf_page,sib_id, ctx);  
  }else{
    LeafMerge(sib_id,leaf_page, ctx);
  }
  return;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InternalProcess(page_id_t page_id, Context &ctx){
  #ifdef DEBUG_DELETE
  delete_log << "internal process begin" << std::endl;
  #endif
  BUSTUB_ASSERT(ctx.write_nodes_.count(page_id), "page should be in write set");
  InternalPage* node = reinterpret_cast< InternalPage *>(ctx.write_nodes_[page_id]);
  while(node->GetPageId() != GetRootPageId() && node->GetSize() < node->GetMinSize()){
    #ifdef DEBUG_DELETE
    delete_log << "internal process page: " << node->GetPageId() << std::endl;
    node->NodePrint(delete_log);
    #endif
    auto node_siblings = InternalGetSibling(node->GetPageId(), ctx);
    auto borrow_res =  SiblingParser(node_siblings,PageParser::Borrow);
    if(borrow_res.has_value()){
      auto [sib_id, direct] = borrow_res.value();
      #ifdef DEBUG_DELETE
      delete_log << "borrow from sibling internal: " << sib_id << std::endl;
      #endif
      InternalBorrow(node->GetPageId(), sib_id, direct, ctx);
      return;
    }
    auto merge_res = SiblingParser(node_siblings,PageParser::Merge);
    BUSTUB_ASSERT(merge_res.has_value(), "no sibling to merge");
    auto [sib_id, direct] = merge_res.value();
    #ifdef DEBUG_DELETE
    delete_log << "merge from sibling page: " << sib_id << std::endl;
    #endif
    page_id_t parent_id;
    if(direct){
      parent_id = InternalMerge(node->GetPageId(), sib_id, ctx);
    }else{
      parent_id = InternalMerge(sib_id, node->GetPageId(), ctx);
    }
    node = reinterpret_cast< InternalPage *>(ctx.write_nodes_[parent_id]);
  }

  if(node->GetPageId() == GetRootPageId() && node->GetSize() == 1){
    SetRootPage(node->ValueAt(0));
    node->SetSize(0);
  }
  #ifdef DEBUG_DELETE
  delete_log << "internal process finish" << std::endl;
  #endif
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalMerge(page_id_t leaf_page, page_id_t right_page, Context &ctx) -> page_id_t{
  #ifdef DEBUG_DELETE
  delete_log << "left page id: " << leaf_page << std::endl;
  delete_log << "right page id: " << right_page << std::endl;
  #endif
  BUSTUB_ASSERT(ctx.write_nodes_.count(leaf_page), "page should be in write set");
  BUSTUB_ASSERT(ctx.write_nodes_.count(right_page), "page should be in write set");
  InternalPage *left_node = reinterpret_cast<InternalPage *>(ctx.write_nodes_[leaf_page]);
  InternalPage *right_node = reinterpret_cast<InternalPage *>(ctx.write_nodes_[right_page]);
  BUSTUB_ASSERT(ctx.write_nodes_.count(left_node->GetParentId()), "page should be in write set");
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(ctx.write_nodes_[left_node->GetParentId()]);

  int index = parent_node->ValueIndex(right_node->GetPageId());
  KeyType insert_key = parent_node->KeyAt(index);
  int insert_pos = left_node->GetSize();
  for(int i=0; i<right_node->GetSize(); i++){
    left_node->Insert_back(right_node->KeyAt(i), right_node->ValueAt(i));
  }
  for(int i=0; i<right_node->GetSize(); i++){
    if(!ctx.SetPageParent(right_node->ValueAt(i), left_node->GetPageId())){
      WritePageGuard guard = bpm_->WritePage(right_node->ValueAt(i));
      BPlusTreePage * page = reinterpret_cast<BPlusTreePage *>(guard.GetDataMut());
      page->SetParentId(left_node->GetPageId());
      ctx.write_nodes_.insert({right_node->ValueAt(i), page});
      ctx.write_set_.push_back(std::move(guard));
    }
  }
  left_node->SetKeyAt(insert_pos,insert_key);
  right_node->SetSize(0);
  #ifdef DEBUG_DELETE
  left_node->NodePrint(delete_log);
  #endif
  // process parent node
  parent_node->Remove(right_page);
  if(parent_node->GetPageId()==GetRootPageId() && parent_node->GetSize()==1){  // parent node is root and only have one child
    SetRootPage(left_node->GetPageId());
    return left_node->GetPageId();
  }
  return parent_node->GetPageId();
}

INDEX_TEMPLATE_ARGUMENTS    
void BPLUSTREE_TYPE::InternalBorrow(page_id_t page_id, page_id_t sib_id, bool direct, Context &ctx){
  BUSTUB_ASSERT(ctx.write_nodes_.count(page_id), "page should be in write set");
  BUSTUB_ASSERT(ctx.write_nodes_.count(sib_id), "sibling page should be in write set");
  InternalPage *node = reinterpret_cast<InternalPage *>(ctx.write_nodes_[page_id]);
  InternalPage *sib_node = reinterpret_cast<InternalPage *>(ctx.write_nodes_[sib_id]);

  BUSTUB_ASSERT(ctx.write_nodes_.count(node->GetParentId()), "page should be in write set");
  InternalPage *parent_internal = reinterpret_cast<InternalPage *>(ctx.write_nodes_[node->GetParentId()]);
  if(direct){   // borrow from right sibling
    page_id_t page_borrow = sib_node->ValueAt(0);
    if(!ctx.write_nodes_.count(page_borrow)){
      WritePageGuard write_guard = bpm_->WritePage(page_borrow);
      ctx.write_nodes_.insert({page_borrow, reinterpret_cast<BPlusTreePage *>(write_guard.GetDataMut())});
      ctx.write_set_.push_back(std::move(write_guard));
    }
    KeyType insert_key = GetMostLeftKey(page_borrow, ctx);
    #ifdef DEBUG_DELETE
    delete_log << "borrow page info: " << page_borrow << std::endl;
    if((ctx.write_nodes_[page_borrow])->IsLeafPage())
      {
        reinterpret_cast<LeafPage *>(ctx.write_nodes_[page_borrow])->NodePrint(delete_log);
      }
    else
      {
        reinterpret_cast<InternalPage *>(ctx.write_nodes_[page_borrow])->NodePrint(delete_log);
      }
    delete_log << "insert key: " << insert_key << std::endl;
    #endif
    BPlusTreePage *borrow_page = reinterpret_cast<BPlusTreePage *>(ctx.write_nodes_[page_borrow]);
    borrow_page->SetParentId(node->GetPageId());
    sib_node->Remove(page_borrow);
    node->Insert(insert_key,page_borrow,comparator_);
    #ifdef DEBUG_DELETE
    sib_node->NodePrint(delete_log);
    node->NodePrint(delete_log);
    #endif
    // internal's parent should change
    KeyType parent_change_key = GetMostLeftKey(sib_node->ValueAt(0),ctx);
    int index = parent_internal->ValueIndex(sib_node->GetPageId());
    parent_internal->SetKeyAt(index,parent_change_key);
  }else{        // borrow from left sibling
    page_id_t page_borrow = sib_node->ValueAt(sib_node->GetSize()-1);
    if(!ctx.write_nodes_.count(page_borrow)){
      WritePageGuard write_guard = bpm_->WritePage(page_borrow);
      ctx.write_nodes_.insert({page_borrow, reinterpret_cast<BPlusTreePage *>(write_guard.GetDataMut())});
      ctx.write_set_.push_back(std::move(write_guard));
    }
    KeyType insert_key = GetMostLeftKey(node->ValueAt(0), ctx);
    #ifdef DEBUG_DELETE
    delete_log << "borrow page info: " << page_borrow << std::endl;
    if((ctx.write_nodes_[page_borrow])->IsLeafPage())
      {
        reinterpret_cast<LeafPage *>(ctx.write_nodes_[page_borrow])->NodePrint(delete_log);
      }
    else
      {
        reinterpret_cast<InternalPage *>(ctx.write_nodes_[page_borrow])->NodePrint(delete_log);
      }
    delete_log << "insert key: " << insert_key << std::endl;
    #endif
    BPlusTreePage *borrow_page = reinterpret_cast<BPlusTreePage *>(ctx.write_nodes_[page_borrow]);
    borrow_page->SetParentId(node->GetPageId());
    sib_node->Remove(page_borrow);
    node->Insert_begin(insert_key,page_borrow);
    #ifdef DEBUG_DELETE
    sib_node->NodePrint(delete_log);
    node->NodePrint(delete_log);
    #endif
     // internal's parent should change
     KeyType parent_change_key = GetMostLeftKey(node->ValueAt(0), ctx);
     int index = parent_internal->ValueIndex(node->GetPageId());
     parent_internal->SetKeyAt(index,parent_change_key);
  }
  // std::cout << this->DrawBPlusTree() << std::endl;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetMostLeftKey(const page_id_t &page, Context &ctx) -> KeyType{

  BPlusTreePage *node;
  if(ctx.write_nodes_.count(page)){
    node = ctx.write_nodes_[page];
  }else{
    WritePageGuard write_guard = bpm_->WritePage(page);
    node = reinterpret_cast< BPlusTreePage *>(write_guard.GetDataMut());
    ctx.write_nodes_.insert({page, node});
    ctx.write_set_.push_back(std::move(write_guard));
  }

  while(!node->IsLeafPage()){
    auto v = reinterpret_cast< InternalPage *>(node)->ValueAt(0);
    if(ctx.write_nodes_.count(v)){
      node = ctx.write_nodes_[v];
    }else{
      WritePageGuard  write_guard = bpm_->WritePage(v);
      node = reinterpret_cast< BPlusTreePage *>(write_guard.GetDataMut());
      ctx.write_nodes_.insert({v, node});
      ctx.write_set_.push_back(std::move(write_guard));
    }

  }
  return reinterpret_cast<const LeafPage *>(node)->KeyAt(0);
  // const BPlusTreePage *node;
  // ReadPageGuard read_guard;
  // if(ctx.write_nodes_.count(page)){
  //   node = ctx.write_nodes_[page];
  // }else{
  //   read_guard = bpm_->ReadPage(page);
  //   node = reinterpret_cast<const BPlusTreePage *>(read_guard.GetData());
  // }
  // while(!node->IsLeafPage()){
  //   auto v = reinterpret_cast<const InternalPage *>(node)->ValueAt(0);
  //   read_guard = bpm_->ReadPage(v);
  //   node = reinterpret_cast<const BPlusTreePage *>(read_guard.GetData());
  // }
  // return reinterpret_cast<const LeafPage *>(node)->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SiblingParser(std::vector<std::tuple<page_id_t,bool,bool>> &sibling_list,const PageParser &parser) -> std::optional<std::tuple<page_id_t,bool>>{
  if(parser == PageParser::Borrow){
    for(auto &sib : sibling_list){
      auto [sib_id,direct,more_than_half] = sib;
      if(more_than_half){
        return std::make_tuple(sib_id,direct);
      }
    }
    return std::nullopt;
  }
  // merge mode we just need to return the first element
  if(!sibling_list.empty()){
    auto[sib_id,direct,more_than_half] = sibling_list.front();
    return std::make_tuple(sib_id,direct);
  }
  return std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalGetSibling(page_id_t page_id, Context &ctx) -> std::vector<std::tuple<page_id_t,bool,bool>> {
  // find sibling node, false means left, true means right    vec: page id, direct, more than half full
  #ifdef DEBUG_DELETE
  delete_log << "get sibling" << std::endl;
  #endif
  std::vector<std::tuple<page_id_t,bool,bool>> sibling_vec;
  BUSTUB_ASSERT(ctx.write_nodes_.count(page_id), "page should be in write set");
  page_id_t parent_id = reinterpret_cast<const InternalPage *>(ctx.write_nodes_[page_id])->GetParentId();
  BUSTUB_ASSERT(ctx.write_nodes_.count(parent_id), "page should be in write set");
  const InternalPage* parent_node = reinterpret_cast<const InternalPage*>(ctx.write_nodes_[parent_id]);
  int index = parent_node->ValueIndex(page_id);
  int prev_index = index - 1;
  int next_index = index + 1;
  if (prev_index >= 0) {
    bool direct = false;
    bool more_than_half;
    WritePageGuard prev_guard = bpm_->WritePage(parent_node->ValueAt(prev_index));
    const InternalPage* prev_page = reinterpret_cast<const InternalPage*>(prev_guard.GetData());
    #ifdef DEBUG_DELETE
    prev_page->NodePrint(delete_log);
    #endif
    more_than_half = prev_page->GetSize() > prev_page->GetMinSize();
    sibling_vec.push_back(std::make_tuple(prev_page->GetPageId(),direct,more_than_half));
    ctx.write_nodes_.insert({prev_page->GetPageId(),
                              reinterpret_cast<BPlusTreePage *>(prev_guard.GetDataMut())});
    ctx.write_set_.push_back(std::move(prev_guard));
  }
  if(next_index < parent_node->GetSize()){
    bool direct = true;
    bool more_than_half;
    WritePageGuard next_guard = bpm_->WritePage(parent_node->ValueAt(next_index));
    const InternalPage* next_page = reinterpret_cast<const InternalPage*>(next_guard.GetData());
    #ifdef DEBUG_DELETE
    next_page->NodePrint(delete_log);
    #endif
    more_than_half = next_page->GetSize() > next_page->GetMinSize();
    sibling_vec.push_back(std::make_tuple(next_page->GetPageId(),direct,more_than_half));
    ctx.write_nodes_.insert({next_page->GetPageId(),
                              reinterpret_cast<BPlusTreePage *>(next_guard.GetDataMut())});
    ctx.write_set_.push_back(std::move(next_guard));
  }
  return sibling_vec;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LeafMerge(page_id_t left_id, page_id_t right_id, Context &ctx){
  #ifdef DEBUG_DELETE
  delete_log << "LeafMerge: " << left_id << " " << right_id << std::endl; 
  #endif
  BUSTUB_ASSERT(ctx.write_nodes_.count(left_id), "page should be in write set");
  BUSTUB_ASSERT(ctx.write_nodes_.count(right_id), "page should be in write set");
  LeafPage* left_leaf = reinterpret_cast<LeafPage*>(ctx.write_nodes_[left_id]);
  LeafPage* right_leaf = reinterpret_cast<LeafPage*>(ctx.write_nodes_[right_id]);

  for(int i=0; i<right_leaf->GetSize(); i++){
    left_leaf->Insert(right_leaf->KeyAt(i), right_leaf->ValueAt(i), comparator_);
  }
  right_leaf->SetSize(0);
  if(right_leaf->GetPageId()==GetRootPageId()){
    SetRootPage(left_leaf->GetPageId());
    #ifdef DEBUG_DELETE
    delete_log << "set new root page id: " << left_leaf->GetPageId() << std::endl;
    delete_log << "merge finish" << std::endl;
    #endif
    return;
  }
  {  // set next and prev page
    page_id_t next_page_id = right_leaf->GetNextPageId();
    left_leaf->SetNextPageId(next_page_id);
    if(next_page_id != INVALID_PAGE_ID){
      if(ctx.write_nodes_.count(next_page_id)){
        reinterpret_cast<LeafPage*>(ctx.write_nodes_[next_page_id])->SetPrevPageId(left_leaf->GetPageId());
      }else{
        WritePageGuard write_guard_next = bpm_->WritePage(next_page_id);
        auto page = reinterpret_cast<LeafPage *>(write_guard_next.GetDataMut());
        page->SetPrevPageId(left_leaf->GetPageId());
        ctx.write_nodes_.insert({next_page_id,  
                                reinterpret_cast<BPlusTreePage *>(write_guard_next.GetDataMut())});
        ctx.write_set_.push_back(std::move(write_guard_next));
      }
    }
  }
  #ifdef DEBUG_DELETE
  left_leaf->NodePrint(delete_log);
  delete_log << "merge finish" << std::endl;
  delete_log << "parent node should change" << std::endl;
  #endif
  BUSTUB_ASSERT(ctx.write_nodes_.count(left_leaf->GetParentId()), "page should be in write set");
  InternalPage* parent_node = reinterpret_cast<InternalPage *>(ctx.write_nodes_[left_leaf->GetParentId()]);
  BUSTUB_ASSERT(parent_node->Remove(right_leaf->GetPageId()),"internal page remove fail, value not found");
  InternalProcess(parent_node->GetPageId(), ctx);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafGetSibling(page_id_t page_id, Context &ctx ) -> std::vector<std::tuple<page_id_t,bool,bool>>{
  std::vector<std::tuple<page_id_t,bool,bool>> sibling_vec;    // sib_id, direct, more than half full

  // leaf_node->NodePrint();
  BUSTUB_ASSERT(ctx.write_nodes_.count(page_id), "page should be in write set");
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(ctx.write_nodes_[page_id]);
  page_id_t pre_leaf_id = leaf_node->GetPrevPageId();
  page_id_t next_leaf_id = leaf_node->GetNextPageId();

  if(pre_leaf_id != INVALID_PAGE_ID){
    bool direct = false;
    bool more_than_half;
    WritePageGuard pre_guard = bpm_->WritePage(pre_leaf_id);
    LeafPage* pre_page = reinterpret_cast< LeafPage*>(pre_guard.GetDataMut());
    // pre_page->NodePrint();
    more_than_half = pre_page->GetSize() > pre_page->GetMinSize();
    if(pre_page->GetParentId()==leaf_node->GetParentId()){
      ctx.write_nodes_.insert({pre_leaf_id,
                              reinterpret_cast<BPlusTreePage *>(pre_guard.GetDataMut())});
      ctx.write_set_.push_back(std::move(pre_guard));
      sibling_vec.push_back(std::make_tuple(pre_leaf_id,direct,more_than_half));
    }
  }
  if(next_leaf_id != INVALID_PAGE_ID){
    bool direct = true;
    bool more_than_half;
    WritePageGuard next_guard = bpm_->WritePage(next_leaf_id);
    LeafPage* next_page = reinterpret_cast<LeafPage*>(next_guard.GetDataMut());
    // next_page->NodePrint();
    more_than_half = next_page->GetSize() > next_page->GetMinSize();
    if(next_page->GetParentId()==leaf_node->GetParentId()){
      ctx.write_nodes_.insert({next_leaf_id,
                              reinterpret_cast<BPlusTreePage *>(next_guard.GetDataMut())});
      ctx.write_set_.push_back(std::move(next_guard));
      sibling_vec.push_back(std::make_tuple(next_leaf_id,direct,more_than_half));
    }
  }
  return sibling_vec;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LeafBorrow(page_id_t page_id, page_id_t sib_id, bool direct, Context &ctx){
  BUSTUB_ASSERT(ctx.write_nodes_.count(page_id), "page should be in write set");
  BUSTUB_ASSERT(ctx.write_nodes_.count(sib_id), "page should be in write set");
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(ctx.write_nodes_[page_id]);
  LeafPage* sib_node = reinterpret_cast<LeafPage*>(ctx.write_nodes_[sib_id]);
  if(!direct){  // borrow from left sibling
    KeyType k_new = sib_node->KeyAt(sib_node->GetSize()-1);
    ValueType v_new = sib_node->ValueAt(sib_node->GetSize()-1);
    sib_node->Remove(k_new,comparator_);
    leaf_node->Insert(k_new,v_new,comparator_); 
    // parent node should change
    BUSTUB_ASSERT(ctx.write_nodes_.count(leaf_node->GetParentId()), "page should be in write set");
    InternalPage* parent_page = reinterpret_cast<InternalPage *>(ctx.write_nodes_[leaf_node->GetParentId()]);
    int index = parent_page->ValueIndex(leaf_node->GetPageId());
    parent_page->SetKeyAt(index,k_new);
  }else{ //     borrow from right sibling
    KeyType k_new = sib_node->KeyAt(0);
    ValueType v_new = sib_node->ValueAt(0);
    sib_node->Remove(k_new,comparator_);
    leaf_node->Insert(k_new,v_new,comparator_);             
    // parent node should change
    BUSTUB_ASSERT(ctx.write_nodes_.count(sib_node->GetParentId()), "page should be in write set");
    InternalPage* parent_page = reinterpret_cast<InternalPage *>(ctx.write_nodes_[sib_node->GetParentId()]);
    int index = parent_page->ValueIndex(sib_node->GetPageId());
    parent_page->SetKeyAt(index,sib_node->KeyAt(0));
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key , Context &ctx, const Operation &op ) -> page_id_t {
  {
    WritePageGuard write_guard = bpm_->WritePage(root_page_id_);    // find leaf
    ctx.write_nodes_.insert({write_guard.GetPageId(),
                            reinterpret_cast<BPlusTreePage *>(write_guard.GetDataMut())});
    ctx.write_set_.push_back(std::move(write_guard));
  }
  auto node = reinterpret_cast< const BPlusTreePage *>(ctx.write_set_.back().GetData());
  while (!node->IsLeafPage()) {
      auto v = reinterpret_cast< const InternalPage *>(ctx.write_set_.back().GetData()) -> Lookup(key, comparator_);
      WritePageGuard write_guard = bpm_->WritePage(v);
      node = reinterpret_cast<const  BPlusTreePage *>(write_guard.GetData());
      if(op == Operation::INSERT && node->GetSize() < node->GetMaxSize()){
          ctx.write_set_.clear();
      }else if(op == Operation::DELETE && node->GetSize() > node->GetMinSize()){
          ctx.write_set_.clear();
      }else{}
      ctx.write_nodes_.insert({write_guard.GetPageId(),
                              reinterpret_cast<BPlusTreePage *>(write_guard.GetDataMut())});
      ctx.write_set_.push_back(std::move(write_guard));
  }
  return node->GetPageId();
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  if(IsEmpty()){
    return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, 0);
  }
  ReadPageGuard read_guard = bpm_->ReadPage(root_page_id_);
  const BPlusTreePage* node = reinterpret_cast<const BPlusTreePage *>(read_guard.GetData());
  while(!node->IsLeafPage()){
    auto v = reinterpret_cast<const InternalPage *>(node)->ValueAt(0);
    read_guard = bpm_->ReadPage(v);
    node = reinterpret_cast<const BPlusTreePage *>(read_guard.GetData());
  }
  #ifdef DEBUG_INTERATOR
  const LeafPage *n = reinterpret_cast<const LeafPage *>(node);
  std::cout << "start index: " << n->KeyAt(0) << std::endl;
  std::cout << "start page: " << n->GetPageId() << std::endl;
  #endif
  return INDEXITERATOR_TYPE(bpm_, node->GetPageId(), 0);
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  if(IsEmpty()){
    return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, 0);
  }
  ReadPageGuard read_page_guard;
  read_page_guard = bpm_->ReadPage(root_page_id_);
  auto node = reinterpret_cast< const BPlusTreePage *>(read_page_guard.GetData());
  while (!node->IsLeafPage()) {
      auto v =  reinterpret_cast<const InternalPage*>(node)->Lookup(key, comparator_);
      ReadPageGuard pre_guard = std::move(read_page_guard);
      read_page_guard = bpm_->ReadPage(v);
      node = reinterpret_cast<const BPlusTreePage *>(read_page_guard.GetData());
  }
  const LeafPage *n = reinterpret_cast< const LeafPage *>(node);
  int index = n->KeyIndex(key,comparator_);
  BUSTUB_ASSERT(index != -1, "key not found");
  #ifdef DEBUG_INTERATOR
  std::cout << "key index: " << index << std::endl;
  std::cout << "key in page: " << n->GetPageId() << std::endl;
  #endif
  return INDEXITERATOR_TYPE(bpm_, n->GetPageId(), index);
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, 0);
}

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  return root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
