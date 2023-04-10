//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
    new(&pages_[i]) Page();
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  /* throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager_instance.cpp`."); */
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * { 
  std::scoped_lock<std::mutex> lock(latch_);

  if(free_list_.size() == 0 && replacer_->Size() == 0) {
    page_id = nullptr;
    return nullptr; 
  }
  //Page* new_page = new Page();
  auto new_page_id = AllocatePage();
  //new_page->page_id_ = new_page_id;
  frame_id_t new_frame_id;
  if(free_list_.size() != 0) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // replacer
    replacer_->Evict(&new_frame_id);
    // 若 dirty 写磁盘
    //auto evict_page = pages_[new_frame_id];
    if(pages_[new_frame_id].IsDirty() == true){
      pages_[new_frame_id].is_dirty_ = false;
      disk_manager_->WritePage(pages_[new_frame_id].GetPageId(), pages_[new_frame_id].GetData());
    }
    // 更新 hashtable
    //page_table_->Remove(new_page_id); 
    page_table_->Remove(pages_[new_frame_id].GetPageId());
  }
  // 更新 hashtable
  page_table_->Insert(new_page_id, new_frame_id);
  // 更新 replacer
  replacer_->RecordAccess(new_frame_id);
  replacer_->SetEvictable(new_frame_id, false);
  // 更新 buffer pool
  pages_[new_frame_id].ResetMemory();
  pages_[new_frame_id].page_id_ = new_page_id;
  *page_id = new_page_id;
  pages_[new_frame_id].pin_count_ = 1;
  pages_[new_frame_id].is_dirty_ = false;
  return &pages_[new_frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { 
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t target_frame_id;
  if(page_table_->Find(page_id, target_frame_id) == true) {
    replacer_->RecordAccess(target_frame_id);
    pages_[target_frame_id].pin_count_++;
    replacer_->SetEvictable(target_frame_id, false);
    //std::cout << "Find success ! target_frame_id :" << target_frame_id << std::endl;
    return &pages_[target_frame_id];
  }
  if(free_list_.size() == 0 && replacer_->Size() == 0) {
    return nullptr; 
  }
  //Page* new_page = new Page();
  //new_page->page_id_ = page_id;
  //disk_manager_->ReadPage(page_id, new_page->GetData());

  // 同上（NewPgImp）
  frame_id_t new_frame_id;
  if(free_list_.size() != 0) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
    //std::cout << "free_list_ success ! target_frame_id :" << target_frame_id << std::endl;
  } else {
    // replacer
    replacer_->Evict(&new_frame_id);
    // 若 dirty 写磁盘
    //auto evict_page = pages_[new_frame_id];
    if(pages_[new_frame_id].IsDirty() == true){
      pages_[new_frame_id].is_dirty_ = false;
      disk_manager_->WritePage(pages_[new_frame_id].GetPageId(), pages_[new_frame_id].GetData());
    }
    // 更新 hashtable
    //page_table_->Remove(page_id);
    page_table_->Remove(pages_[new_frame_id].GetPageId());
  }
  // 更新 hashtable
  page_table_->Insert(page_id, new_frame_id);
  // 更新 replacer
  replacer_->RecordAccess(new_frame_id);
  replacer_->SetEvictable(new_frame_id, false);
  // 更新 buffer pool
  disk_manager_->ReadPage(page_id, pages_[new_frame_id].GetData());
  pages_[new_frame_id].page_id_ = page_id;
  pages_[new_frame_id].pin_count_ = 1;
  pages_[new_frame_id].is_dirty_ = false;
  //std::cout << "replacer success ! target_frame_id :" << target_frame_id << std::endl;
  return &pages_[new_frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  
  // return false if the page could not be found in the page table
  frame_id_t target_frame_id;
  if(page_table_->Find(page_id, target_frame_id) == false) {
    return false;
  }
  //auto cur_page = pages_[target_frame_id];
  if(pages_[target_frame_id].pin_count_ <= 0) { return false; }
  pages_[target_frame_id].pin_count_--;
  //std::cout << "pages_[target_frame_id].pin_count_" << pages_[target_frame_id].pin_count_ << std::endl;
  if(pages_[target_frame_id].pin_count_ == 0) {replacer_->SetEvictable(target_frame_id, true);}
  // cannot true to false
  if(is_dirty == true){pages_[target_frame_id].is_dirty_ = is_dirty;}
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);

  // return false if the page could not be found in the page table
  frame_id_t target_frame_id;
  if(page_table_->Find(page_id, target_frame_id) == false) {
    return false;
  }
  // 若 dirty 刷盘, 同上
  //auto evict_page = pages_[target_frame_id];
  if(pages_[target_frame_id].IsDirty() == true){
    pages_[target_frame_id].is_dirty_ = false;
    disk_manager_->WritePage(pages_[target_frame_id].GetPageId(), pages_[target_frame_id].GetData());
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);

  for(size_t i = 0; i < pool_size_; i++){
    //auto cur_page = pages_[static_cast<int>(i)];
    if(pages_[static_cast<int>(i)].GetPageId() != INVALID_PAGE_ID && pages_[static_cast<int>(i)].IsDirty() == true){
      pages_[static_cast<int>(i)].is_dirty_ = false;
      disk_manager_->WritePage(pages_[static_cast<int>(i)].GetPageId(), pages_[static_cast<int>(i)].GetData());
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);

  // return true if the page could not be found in the page table
  frame_id_t target_frame_id;
  if(page_table_->Find(page_id, target_frame_id) == false) {
    return true;
  }
  //auto cur_page = pages_[target_frame_id];
  if(pages_[target_frame_id].GetPinCount() > 0) { return false; }
  // delete
  // 更新 hashtable
  page_table_->Remove(page_id);
  // 更新 replacer
  replacer_->Remove(page_id);
  // 更新 buffer pool
  free_list_.emplace_back(target_frame_id);
  pages_[target_frame_id].ResetMemory();
  pages_[target_frame_id].is_dirty_ = false;
  pages_[target_frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[target_frame_id].pin_count_ = 0;
  // disk
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
