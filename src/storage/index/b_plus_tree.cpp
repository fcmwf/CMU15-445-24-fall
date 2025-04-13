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

// #define DEBUG_INSERT
// #define DEBUG_DELETE

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
  ReadPageGuard read_page_guard = bpm_->ReadPage(root_page_id_);
  auto node = reinterpret_cast< const BPlusTreePage *>(read_page_guard.GetData());
  ctx.read_set_.push_back(std::move(read_page_guard));
  while (!node->IsLeafPage()) {
      auto v = reinterpret_cast<const InternalPage *>(ctx.read_set_.back().GetData())->Lookup(key, comparator_);
      read_page_guard = bpm_->ReadPage(v);
      node = reinterpret_cast<const BPlusTreePage *>(read_page_guard.GetData());
      ctx.read_set_.pop_back();
      ctx.read_set_.push_back(std::move(read_page_guard));
  }
  auto n = reinterpret_cast< const LeafPage *>(ctx.read_set_.back().GetData());
  std::optional<ValueType> v = n->Lookup(key, comparator_);
  if(v.has_value()){ 
    result->emplace_back(std::move(v.value()));
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
  // Declaration of context instance. Using the Context is not necessary but advised.
  // Context ctx;
  //Get root and check wheather it is existed
  Context ctx;
  #ifdef DEBUG_INSERT
  std::cout << "Insert key: " << key << std::endl;
  std::cout << "value: " <<value;
  #endif
  if(IsEmpty()){
    page_id_t page_id = bpm_->NewPage();
    #ifdef DEBUG_INSERT
    std::cout << "root is empty" << std::endl;
    std::cout << "page_id: " << page_id << std::endl;
    #endif
    WritePageGuard write_guard = bpm_->WritePage(page_id);
    LeafPage* root_page_leaf = reinterpret_cast<LeafPage *>(write_guard.GetDataMut());
    root_page_leaf->Init(page_id,leaf_max_size_);
    root_page_leaf->Insert(key, value, comparator_);
    SetRootPage(page_id);
    #ifdef DEBUG_INSERT
    root_page_leaf->NodePrint();
    #endif
    return true;
  }
  FindLeaf(key, ctx, Operation::INSERT);
  WritePageGuard write_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();   
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(write_guard.GetDataMut());
  if(!leaf_node->Insert(key,value,comparator_)){   //duplicate key
    #ifdef DEBUG_INSERT
    std::cout << "duplicate key!" << std::endl;
    #endif
    return false;
  }
  if (leaf_node->GetSize() > leaf_node->GetMaxSize()) {   // split leaf node
    WritePageGuard new_leaf_guard = SplitLeafNode(leaf_node, ctx);
    LeafPage* new_leaf_node = reinterpret_cast<LeafPage *>(new_leaf_guard.GetDataMut());
    ctx.write_nodes_.push_back(reinterpret_cast<BPlusTreePage *>(new_leaf_guard.GetDataMut()));
    #ifdef DEBUG_INSERT
    std::cout << "leaf need split" << std::endl;
    leaf_node->NodePrint();
    new_leaf_node->NodePrint();
    #endif
    // insert to parent 递归
    InsertLeaf2Parent(new_leaf_node->KeyAt(0), leaf_node, new_leaf_node, ctx);
    return true;
  }
  #ifdef DEBUG_INSERT
  leaf_node->NodePrint();
  #endif
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertLeaf2Parent(const KeyType &key, LeafPage * old_node, LeafPage * new_node, Context &ctx) {

    if (old_node->GetPageId()==GetRootPageId()) {
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
        std::cout << "Set new root page: " << pageId << std::endl;
        root_page->NodePrint();
        #endif
        return;
    }
    new_node->SetParentId(old_node->GetParentId());
    WritePageGuard parent_page = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    InternalPage * pa_node = reinterpret_cast<InternalPage *>(parent_page.GetDataMut());
    pa_node->Insert(key, new_node->GetPageId(), comparator_);
    #ifdef DEBUG_INSERT
    pa_node->NodePrint();
    #endif
    if (pa_node->GetSize() <= pa_node->GetMaxSize()) {
        return;
    }
    InsertInternalPage2Parent(pa_node, ctx);
    return;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInternalPage2Parent(InternalPage* old_node, Context &ctx) {  // node should bbe splited and insert to their parent
  WritePageGuard write_guard_newNode = SplitInternalNode(old_node,ctx);
  InternalPage* new_node = reinterpret_cast<InternalPage *>(write_guard_newNode.GetDataMut());
  ctx.write_nodes_.push_back(reinterpret_cast<BPlusTreePage *>(write_guard_newNode.GetDataMut()));

  KeyType insert_key = new_node->KeyAt(0);
  if (old_node->GetPageId()==GetRootPageId()) {
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
    std::cout << "Set new root page: " << pageId << std::endl;
    root_page->NodePrint();
    #endif
    return;
  }
  WritePageGuard write_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  InternalPage* parent_page = reinterpret_cast<InternalPage *>(write_guard.GetDataMut());
  parent_page->Insert(insert_key,new_node->GetPageId(),comparator_);
  new_node->SetParentId(old_node->GetParentId());
  if(parent_page->GetSize() > parent_page->GetMaxSize()){
    InsertInternalPage2Parent(parent_page,ctx);
  }
  return;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafNode(LeafPage* old_node, Context &ctx) -> WritePageGuard{
    old_node->NodePrint();
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
    
    if(leaf_next != INVALID_PAGE_ID){
      reinterpret_cast<LeafPage *>(write_guard_next.GetDataMut())->SetPrevPageId(pageId);
      ctx.write_nodes_.push_back(reinterpret_cast<BPlusTreePage *>(write_guard_next.GetDataMut()));
      ctx.lock_set_.push_back(std::move(write_guard_next));
    }
    return write_new_guard;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalNode(InternalPage* old_node, Context &ctx) -> WritePageGuard{
    page_id_t pageId = bpm_->NewPage();
    #ifdef DEBUG_INSERT
    std::cout << "new page id: " << pageId << std::endl;
    #endif
    WritePageGuard write_new_guard = bpm_->WritePage(pageId);
    InternalPage* new_node = reinterpret_cast<InternalPage *>(write_new_guard.GetDataMut());

    new_node->Init(pageId, internal_max_size_);
    new_node->CopyInternalData((old_node->GetSize())/2 , old_node);
    new_node->SetSize(old_node->GetSize() - (old_node->GetSize())/2);
    old_node->SetSize((old_node->GetSize())/2);

    for(int i=0; i<new_node->GetSize();i++){
      page_id_t page_id = new_node->ValueAt(i);
      #ifdef DEBUG_INSERT
      std::cout << "page id: " << page_id << std::endl;
      #endif
      if(!ctx.SetPageParent(page_id,pageId)){
        WritePageGuard guard = bpm_->WritePage(page_id);
        reinterpret_cast<BPlusTreePage*>(guard.GetDataMut())->SetParentId(pageId);
      }
    }
    return write_new_guard;
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
  std::cout << "cur root page id: " << GetRootPageId() << std::endl;
  std::cout << "remove key: " << key << std::endl;
  #endif
  if(IsEmpty()){ return; }

  page_id_t leaf_page_id = FindLeaf(key,ctx,Operation::DELETE);
  WritePageGuard write_guard = bpm_->WritePage(leaf_page_id);
  LeafPage *leaf_node = reinterpret_cast< LeafPage *>(write_guard.GetDataMut());
  #ifdef DEBUG_DELETE
  std::cout << "remove key in page:  " << leaf_page_id << std::endl;
  leaf_node->NodePrint();
  #endif
  if(!leaf_node->Remove(key,comparator_)){   
    // remove fail
    #ifdef DEBUG_DELETE        
    std::cout << "remove fail" << std::endl;
    #endif
    return;
  }

  if(GetRootPageId()==leaf_node->GetPageId()){
    // if(leaf_node->GetSize()==0){
    //   SetRootPage(INVALID_PAGE_ID);
    // }
    return;
  }
  if (leaf_node->GetSize() >= leaf_node->GetMinSize()) { 
    // node size enough or it is root page
    #ifdef DEBUG_DELETE        
    std::cout << "remove success" << std::endl;
    #endif
    return; 
  }

  
  write_guard.Drop();
  auto leaf_siblings = LeafGetSibling(leaf_page_id);
  // process leaf , try borrow sibling
  auto borrow_leaf_res = SiblingParser(leaf_siblings, PageParser::Borrow);
  if(borrow_leaf_res.has_value()){
    auto [sib_id, direct] = borrow_leaf_res.value();
    page_id_t page_id = leaf_page_id;
    LeafBorrow(page_id, sib_id, direct);
    #ifdef DEBUG_DELETE
    std::cout << "borrow from sibling page: " << sib_id << std::endl;
    #endif
    return;    
  }
  // Merge sibling
  auto merge_leaf_res = SiblingParser(leaf_siblings, PageParser::Merge);
  BUSTUB_ASSERT(merge_leaf_res.has_value(),"no sibling to merge");
  auto [sib_id, direct] = merge_leaf_res.value();
  #ifdef DEBUG_DELETE
  std::cout << "merge sibling page: " << sib_id << std::endl;
  ReadPageGuard read_guard = bpm_->ReadPage(sib_id);
  reinterpret_cast<const LeafPage *>(read_guard.GetData())->NodePrint();
  read_guard.Drop();
  #endif
  if(direct){
    LeafMerge(leaf_page_id,sib_id);  
  }else{
    LeafMerge(sib_id,leaf_page_id);
  }
  return;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InternalProcess(page_id_t page_id){
  #ifdef DEBUG_DELETE
  std::cout << "internal process begin" << std::endl;
  #endif
  ReadPageGuard read_guard = bpm_->ReadPage(page_id);
  const InternalPage* node = reinterpret_cast<const InternalPage *>(read_guard.GetData());
  while(node->GetPageId() != GetRootPageId() && node->GetSize() < node->GetMinSize()){
    #ifdef DEBUG_DELETE
    std::cout << "internal process page: " << node->GetPageId() << std::endl;
    node->NodePrint();
    #endif
    auto node_siblings = InternalGetSibling(node->GetPageId(), node->GetParentId());
    auto borrow_res =  SiblingParser(node_siblings,PageParser::Borrow);
    if(borrow_res.has_value()){
      auto [sib_id, direct] = borrow_res.value();
      #ifdef DEBUG_DELETE
      std::cout << "borrow from sibling page: " << sib_id << std::endl;
      #endif
      page_id_t pageId = node->GetPageId();
      read_guard.Drop();
      InternalBorrow(pageId, sib_id, direct);
      return;
    }
    auto merge_res = SiblingParser(node_siblings,PageParser::Merge);
    BUSTUB_ASSERT(merge_res.has_value(), "no sibling to merge");
    auto [sib_id, direct] = merge_res.value();
    #ifdef DEBUG_DELETE
    std::cout << "merge from sibling page: " << sib_id << std::endl;
    #endif
    read_guard.Drop();
    page_id_t parent_id;
    if(direct){
      parent_id = InternalMerge(node->GetPageId(), sib_id);
    }else{
      parent_id = InternalMerge(sib_id, node->GetPageId());
    }
    read_guard = bpm_->ReadPage(parent_id);
    node = reinterpret_cast<const InternalPage *>(read_guard.GetData());
  }

  if(node->GetPageId() == GetRootPageId() && node->GetSize() == 1){
    page_id_t page_id = node->GetPageId();
    read_guard.Drop();
    SetRootPage(node->ValueAt(0));
    WritePageGuard write_guard = bpm_->WritePage(page_id);
    reinterpret_cast<InternalPage *>(write_guard.GetDataMut())->SetSize(0);
  }
  #ifdef DEBUG_DELETE
  std::cout << "internal process finish" << std::endl;
  #endif
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalMerge(page_id_t leaf_page, page_id_t right_page) -> page_id_t{
  #ifdef DEBUG_INSERT
  std::cout << "left page id: " << leaf_page << std::endl;
  std::cout << "right page id: " << right_page << std::endl;
  #endif
  WritePageGuard left_guard = bpm_->WritePage(leaf_page);
  WritePageGuard right_guard = bpm_->WritePage(right_page);
  InternalPage *left_node = reinterpret_cast<InternalPage *>(left_guard.GetDataMut());
  InternalPage *right_node = reinterpret_cast<InternalPage *>(right_guard.GetDataMut());
  WritePageGuard parent_guard = bpm_->WritePage(left_node->GetParentId());
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_guard.GetDataMut());
  int index = parent_node->ValueIndex(right_node->GetPageId());
  KeyType insert_key = parent_node->KeyAt(index);
  int insert_pos = left_node->GetSize();
  for(int i=0; i<right_node->GetSize(); i++){
    left_node->Insert_back(right_node->KeyAt(i), right_node->ValueAt(i));
  }
  for(int i=0; i<right_node->GetSize(); i++){
    WritePageGuard guard = bpm_->WritePage(right_node->ValueAt(i));
    reinterpret_cast<BPlusTreePage *>(guard.GetDataMut())->SetParentId(left_node->GetPageId());
  }
  left_node->SetKeyAt(insert_pos,insert_key);
  right_node->SetSize(0);
  #ifdef DEBUG_DELETE
  left_node->NodePrint();
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
void BPLUSTREE_TYPE::InternalBorrow(page_id_t page_id, page_id_t sib_id, bool direct){
  WritePageGuard guard = bpm_->WritePage(page_id);
  WritePageGuard sib_guard = bpm_->WritePage(sib_id); 
  InternalPage *node = reinterpret_cast<InternalPage *>(guard.GetDataMut());
  InternalPage *sib_node = reinterpret_cast<InternalPage *>(sib_guard.GetDataMut());
  WritePageGuard parent_guard = bpm_->WritePage(node->GetParentId());
  if(direct){   // borrow from right sibling
    page_id_t page_borrow = sib_node->ValueAt(0);
    KeyType insert_key = GetMostLeftKey(page_borrow);
    #ifdef DEBUG_DELETE
    std::cout << "borrow page id: " << page_borrow << std::endl;
    std::cout << "insert key: " << insert_key << std::endl;
    #endif
    WritePageGuard borrow_guard = bpm_->WritePage(page_borrow);
    BPlusTreePage *borrow_page = reinterpret_cast<BPlusTreePage *>(borrow_guard.GetDataMut());
    borrow_page->SetParentId(node->GetPageId());
    sib_node->Remove(page_borrow);
    node->Insert(insert_key,page_borrow,comparator_);
    #ifdef DEBUG_DELETE
    sib_node->NodePrint();
    node->NodePrint();
    #endif
    // internal's parent should change
    KeyType parent_change_key = GetMostLeftKey(sib_node->ValueAt(0));
    InternalPage *parent_internal = reinterpret_cast<InternalPage *>(parent_guard.GetDataMut());
    int index = parent_internal->ValueIndex(sib_node->GetPageId());
    parent_internal->SetKeyAt(index,parent_change_key);
  }else{        // borrow from left sibling
    page_id_t page_borrow = sib_node->ValueAt(sib_node->GetSize()-1);
    KeyType insert_key = GetMostLeftKey(node->ValueAt(0));
    WritePageGuard borrow_guard = bpm_->WritePage(page_borrow);
    BPlusTreePage *borrow_page = reinterpret_cast<BPlusTreePage *>(borrow_guard.GetDataMut());
    borrow_page->SetParentId(node->GetParentId());
    sib_node->Remove(page_borrow);
    node->Insert(insert_key,page_borrow,comparator_);
     // internal's parent should change
     KeyType parent_change_key = GetMostLeftKey(node->ValueAt(0));
     InternalPage *parent_internal = reinterpret_cast<InternalPage *>(parent_guard.GetDataMut());
     int index = parent_internal->ValueIndex(node->GetPageId());
     parent_internal->SetKeyAt(index,parent_change_key);
  }
  // std::cout << this->DrawBPlusTree() << std::endl;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetMostLeftKey(const page_id_t &page) -> KeyType{
  ReadPageGuard read_guard = bpm_->ReadPage(page);
  auto node = reinterpret_cast< const BPlusTreePage *>(read_guard.GetData());
  while(!node->IsLeafPage()){
    ReadPageGuard  pre_guard;
    auto v = reinterpret_cast<const InternalPage *>(read_guard.GetData())->ValueAt(0);
    pre_guard = std::move(read_guard);
    read_guard = bpm_->ReadPage(v);
    pre_guard.Drop();
    node = reinterpret_cast<const BPlusTreePage *>(read_guard.GetData());
  }
  return reinterpret_cast<const LeafPage *>(read_guard.GetData())->KeyAt(0);
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
auto BPLUSTREE_TYPE::InternalGetSibling(page_id_t page_id, page_id_t parent_id) -> std::vector<std::tuple<page_id_t,bool,bool>> {
  // find sibling node, false means left, true means right    vec: page id, direct, more than half full
  std::vector<std::tuple<page_id_t,bool,bool>> sibling_vec;

  ReadPageGuard parent_guard = bpm_->ReadPage(parent_id);
  const InternalPage* parent_node = reinterpret_cast<const InternalPage*>(parent_guard.GetData());
  // parent_node->NodePrint();
  int index = parent_node->ValueIndex(page_id);
  // std::cout << "page index: " << index << std::endl;
  int prev_index = index - 1;
  int next_index = index + 1;
  if (prev_index >= 0) {
    bool direct = false;
    bool more_than_half;
    ReadPageGuard prev_guard = bpm_->ReadPage(parent_node->ValueAt(prev_index));
    const InternalPage* prev_page = reinterpret_cast<const InternalPage*>(prev_guard.GetData());
    more_than_half = prev_page->GetSize() > prev_page->GetMinSize();
    sibling_vec.push_back(std::make_tuple(prev_page->GetPageId(),direct,more_than_half));
  }
  if(next_index < parent_node->GetSize()){
    bool direct = true;
    bool more_than_half;
    ReadPageGuard next_guard = bpm_->ReadPage(parent_node->ValueAt(next_index));
    const InternalPage* next_page = reinterpret_cast<const InternalPage*>(next_guard.GetData());
    more_than_half = next_page->GetSize() > next_page->GetMinSize();
    sibling_vec.push_back(std::make_tuple(next_page->GetPageId(),direct,more_than_half));
  }
  return sibling_vec;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LeafMerge(page_id_t left_id, page_id_t right_id){
  #ifdef DEBUG_DELETE
  std::cout << "LeafMerge: " << left_id << " " << right_id << std::endl; 
  #endif
  WritePageGuard left_leaf_guard = bpm_->WritePage(left_id);
  WritePageGuard right_leaf_guard = bpm_->WritePage(right_id);
  LeafPage* left_leaf = reinterpret_cast<LeafPage *>(left_leaf_guard.GetDataMut());
  LeafPage* right_leaf = reinterpret_cast<LeafPage *>(right_leaf_guard.GetDataMut());
  for(int i=0; i<right_leaf->GetSize(); i++){
    left_leaf->Insert(right_leaf->KeyAt(i), right_leaf->ValueAt(i), comparator_);
  }
  right_leaf->SetSize(0);
  if(right_leaf->GetPageId()==GetRootPageId()){
    SetRootPage(left_leaf->GetPageId());
    #ifdef DEBUG_DELETE
    std::cout << "set new root page id: " << left_leaf->GetPageId() << std::endl;
    std::cout << "merge finish" << std::endl;
    #endif
    return;
  }
  {  // set next and prev page
    page_id_t next_page_id = right_leaf->GetNextPageId();
    left_leaf->SetNextPageId(next_page_id);
    if(next_page_id != INVALID_PAGE_ID){
      WritePageGuard write_guard_next = bpm_->WritePage(next_page_id);
      reinterpret_cast<LeafPage *>(write_guard_next.GetDataMut())->SetPrevPageId(left_leaf->GetPageId());
    }
  }
  #ifdef DEBUG_DELETE
  left_leaf->NodePrint();
  std::cout << "merge finish" << std::endl;
  std::cout << "parent node should change" << std::endl;
  #endif
  WritePageGuard parent_guard = bpm_->WritePage(left_leaf->GetParentId());
  InternalPage* parent_node = reinterpret_cast<InternalPage *>(parent_guard.GetDataMut());
  BUSTUB_ASSERT(parent_node->Remove(right_leaf->GetPageId()),"internal page remove fail, value not found");
  left_leaf_guard.Drop();
  right_leaf_guard.Drop();
  page_id_t parent_page = parent_node->GetPageId();
  parent_guard.Drop();
  InternalProcess(parent_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafGetSibling(page_id_t page_id) -> std::vector<std::tuple<page_id_t,bool,bool>>{
  std::vector<std::tuple<page_id_t,bool,bool>> sibling_vec;    // sib_id, direct, more than half full

  ReadPageGuard read_guard = bpm_->ReadPage(page_id);
  const LeafPage* leaf_node = reinterpret_cast<const LeafPage*>(read_guard.GetData());
  // leaf_node->NodePrint();
  page_id_t pre_leaf_id = leaf_node->GetPrevPageId();
  page_id_t next_leaf_id = leaf_node->GetNextPageId();

  if(pre_leaf_id != INVALID_PAGE_ID){
    bool direct = false;
    bool more_than_half;
    ReadPageGuard pre_guard = bpm_->ReadPage(pre_leaf_id);
    const LeafPage* pre_page = reinterpret_cast<const LeafPage*>(pre_guard.GetData());
    // pre_page->NodePrint();
    more_than_half = pre_page->GetSize() > pre_page->GetMinSize();
    if(pre_page->GetParentId()==leaf_node->GetParentId()){
      sibling_vec.push_back(std::make_tuple(pre_leaf_id,direct,more_than_half));
    }
  }
  if(next_leaf_id != INVALID_PAGE_ID){
    bool direct = true;
    bool more_than_half;
    ReadPageGuard next_guard = bpm_->ReadPage(next_leaf_id);
    const LeafPage* next_page = reinterpret_cast<const LeafPage*>(next_guard.GetData());
    // next_page->NodePrint();
    more_than_half = next_page->GetSize() > next_page->GetMinSize();
    if(next_page->GetParentId()==leaf_node->GetParentId()){
      sibling_vec.push_back(std::make_tuple(next_leaf_id,direct,more_than_half));
    }
  }
  return sibling_vec;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LeafBorrow(page_id_t page_id, page_id_t sib_id, bool direct){
  WritePageGuard leaf_guard = bpm_->WritePage(page_id);
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(leaf_guard.GetDataMut());
  WritePageGuard sib_guard = bpm_->WritePage(sib_id);
  LeafPage* sib_node = reinterpret_cast<LeafPage*>(sib_guard.GetDataMut());
  if(!direct){  // borrow from left sibling
    KeyType k_new = sib_node->KeyAt(sib_node->GetSize()-1);
    ValueType v_new = sib_node->ValueAt(sib_node->GetSize()-1);
    sib_node->Remove(k_new,comparator_);
    leaf_node->Insert(k_new,v_new,comparator_); 
    // parent node should change
    WritePageGuard parent_guard = bpm_->WritePage(leaf_node->GetParentId());
    InternalPage* parent_page = reinterpret_cast<InternalPage *>(parent_guard.GetDataMut());
    int index = parent_page->ValueIndex(leaf_node->GetPageId());
    parent_page->SetKeyAt(index,k_new);
  }else{ //     borrow from right sibling
    KeyType k_new = sib_node->KeyAt(0);
    ValueType v_new = sib_node->ValueAt(0);
    sib_node->Remove(k_new,comparator_);
    leaf_node->Insert(k_new,v_new,comparator_);             
    // parent node should change
    WritePageGuard parent_guard = bpm_->WritePage(sib_node->GetParentId());
    InternalPage* parent_page = reinterpret_cast<InternalPage *>(parent_guard.GetDataMut());
    int index = parent_page->ValueIndex(sib_node->GetPageId());
    parent_page->SetKeyAt(index,sib_node->KeyAt(0));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FindLeaf(const KeyType &key , Context &ctx, const Operation &op ){
  WritePageGuard write_guard = bpm_->WritePage(root_page_id_);    // find leaf
  ctx.write_nodes_.push_back(reinterpret_cast<BPlusTreePage*>(write_guard.GetDataMut()));
  ctx.write_set_.push_back(std::move(write_guard));
  auto node = reinterpret_cast< const BPlusTreePage *>(ctx.write_set_.back().GetData());
  while (!node->IsLeafPage()) {
      auto v = reinterpret_cast< const InternalPage *>(ctx.write_set_.back().GetData()) -> Lookup(key, comparator_);
      write_guard = bpm_->WritePage(v);
      node = reinterpret_cast<const  BPlusTreePage *>(write_guard.GetData());
      if(op == Operation::INSERT && node->GetSize() < node->GetMaxSize()){
          ctx.write_set_.clear();
      }else if(op == Operation::DELETE && node->GetSize() > node->GetMinSize()){
          ctx.write_set_.clear();
      }else{}
      ctx.write_nodes_.push_back(reinterpret_cast<BPlusTreePage*>(write_guard.GetDataMut()));
      ctx.write_set_.push_back(std::move(write_guard));
  }
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

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
