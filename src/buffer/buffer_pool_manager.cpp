//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);
  // single_frame_lock = new std::mutex[pool_size];
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
  // delete[] single_frame_lock;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  Page *return_address = nullptr;
  std::scoped_lock<std::mutex> lk{latch_};
  // page_table_lock.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {  // the page does not exit in the page table.
                                  //  we need to read it from disk and get a frame to hold it
    // free_list_lock.lock();
    if (!free_list_.empty()) {  // get a frame from the free list
      auto frame_to_use = *free_list_.begin();
      free_list_.pop_front();
      // free_list_lock.unlock();  // now the frame is not free anymore, no one else can acquire it
      pages_[frame_to_use].ResetMemory();
      // pages_[frame_to_use].WLatch();
      disk_manager_->ReadPage(page_id, pages_[frame_to_use].GetData());
      // pages_[frame_to_use].WUnlatch();
      pages_[frame_to_use].page_id_ = page_id;
      pages_[frame_to_use].is_dirty_ = false;
      pages_[frame_to_use].pin_count_ = 1;
      replacer_->Pin(frame_to_use);
      page_table_.insert(std::pair<int, int>{page_id, frame_to_use});
      // page_table_lock.unlock();  // prepare it fully and release the page table lock for lookup
      return_address = &(pages_[frame_to_use]);
    } else {  // no free frames; need to evict one. this one may be dirty
              // being dirty does not mean it has to stay and can not be a victim
      // free_list_lock.unlock();
      frame_id_t victim_frame = 0;
      bool victim_success = replacer_->Victim(&victim_frame);
      if (victim_success) {
        page_table_.erase(pages_[victim_frame].GetPageId());
        if (pages_[victim_frame].IsDirty()) {
          // pages_[victim_frame].RLatch();
          disk_manager_->WritePage(pages_[victim_frame].GetPageId(), pages_[victim_frame].GetData());
          // pages_[victim_frame].RUnlatch();
        }
        pages_[victim_frame].ResetMemory();
        // pages_[victim_frame].WLatch();
        disk_manager_->ReadPage(page_id, pages_[victim_frame].GetData());
        // pages_[victim_frame].WUnlatch();
        pages_[victim_frame].page_id_ = page_id;
        pages_[victim_frame].is_dirty_ = false;
        pages_[victim_frame].pin_count_ = 1;
        replacer_->Pin(victim_frame);
        page_table_.insert(std::pair<int, int>{page_id, victim_frame});
        return_address = &(pages_[victim_frame]);
      } else {
        return_address = nullptr;  // if can not find a victim
      }
      // page_table_lock.unlock();
    }
  } else {
    if (pages_[it->second].pin_count_ == 0) {
      replacer_->Pin(it->second);
    }
    pages_[it->second].pin_count_++;
    return_address = &(pages_[it->second]);
    // page_table_lock.unlock();
  }
  return return_address;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  bool return_flag;

  // page_table_lock.lock();
  std::scoped_lock<std::mutex> lk{latch_};
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {  //  the page to unpin does not exist
    // page_table_lock.unlock();
    return_flag = true;
  } else {
    auto frame_to_unpin = it->second;  // find the frame to unpin
    // page_table_lock.unlock();
    // pages_[frame_to_unpin].is_dirty_=is_dirty;
    // 我的理解是这个地方的is_dirty是解除占用之后，告诉我们这个page是否被写过了
    // 所以我直接把它写入到磁盘也行。毕竟反正下一次用这个frame的时候也必须写入磁盘。
    // single_frame_lock[frame_to_unpin].lock();
    pages_[frame_to_unpin].is_dirty_ = pages_[frame_to_unpin].is_dirty_ || is_dirty;
    if (pages_[frame_to_unpin].pin_count_ <= 0) {
      replacer_->Unpin(frame_to_unpin);
      // single_frame_lock[frame_to_unpin].unlock();
      return_flag = false;
    } else {
      // 这个地方要不要把整个数组都锁住呢？如果有个threads来使用这个函数，可能有问题
      // 我是给每个数组元素赋予了一个独立的锁，这样更加方便
      pages_[frame_to_unpin].pin_count_--;
      if (pages_[frame_to_unpin].pin_count_ == 0) {
        replacer_->Unpin(frame_to_unpin);
      }
      return_flag = true;
      // single_frame_lock[frame_to_unpin].unlock();
    }
  }
  return return_flag;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  // page_table_lock.lock();
  std::scoped_lock<std::mutex> lk{latch_};
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  auto frame_to_flush = it->second;
  // page_table_lock.unlock();
  // pages_[frame_to_flush].RLatch();
  disk_manager_->WritePage(page_id, pages_[frame_to_flush].GetData());
  // pages_[frame_to_flush].RUnlatch();
  pages_[frame_to_flush].is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  Page *return_address = nullptr;
  std::scoped_lock<std::mutex> lk{latch_};
  //  free_list_lock.lock();
  if (!free_list_.empty()) {
    auto it = free_list_.begin();
    auto frame_id = *it;
    free_list_.pop_front();
    //  free_list_lock.unlock();
    *page_id = disk_manager_->AllocatePage();
    // page_table_lock.lock();
    page_table_.insert(std::pair<int, int>{*page_id, frame_id});
    // page_table_lock.unlock();

    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 1;
    return_address = &(pages_[frame_id]);
  } else {
    //    free_list_lock.unlock();
    frame_id_t victim_frame;
    bool victim_success = replacer_->Victim(&victim_frame);
    if (victim_success) {
      *page_id = disk_manager_->AllocatePage();
      // page_table_lock.lock();
      page_table_.erase(pages_[victim_frame].GetPageId());
      page_table_.insert(std::pair<int, int>{*page_id, victim_frame});
      // page_table_lock.unlock();

      // single_frame_lock[victim_frame].lock();
      if (pages_[victim_frame].IsDirty()) {
        // pages_[victim_frame].RLatch();
        disk_manager_->WritePage(pages_[victim_frame].GetPageId(), pages_[victim_frame].GetData());
        // pages_[victim_frame].RUnlatch();
      }
      pages_[victim_frame].ResetMemory();
      pages_[victim_frame].page_id_ = *page_id;
      pages_[victim_frame].is_dirty_ = false;
      pages_[victim_frame].pin_count_ = 1;
      replacer_->Pin(victim_frame);

      // single_frame_lock[victim_frame].unlock();
      return_address = &(pages_[victim_frame]);
    } else {
      return_address = nullptr;
    }
  }
  return return_address;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  bool return_value = false;
  // page_table_lock.lock();
  std::scoped_lock<std::mutex> lk{latch_};
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    //    page_table_lock.unlock();
    return_value = true;
  } else {
    auto frame_id = it->second;
    // page_table_lock.unlock();
    // single_frame_lock[frame_id].lock();
    if (pages_[frame_id].GetPinCount() != 0) {
      // single_frame_lock[frame_id].unlock();
      return_value = false;
    } else {
      disk_manager_->DeallocatePage(page_id);
      pages_[frame_id].ResetMemory();
      pages_[frame_id].page_id_ = INVALID_PAGE_ID;
      pages_[frame_id].is_dirty_ = false;
      pages_[frame_id].pin_count_ = 0;
      replacer_->Pin(frame_id);
      // single_frame_lock->unlock();
      //    free_list_lock.lock();
      free_list_.push_back(it->second);
      //  free_list_lock.unlock();
      // page_table_lock.lock();
      page_table_.erase(page_id);
      // page_table_lock.unlock();
      return_value = true;
    }
  }
  return return_value;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  // page_table_lock.lock();
  std::scoped_lock<std::mutex> lk{latch_};
  for (auto it : page_table_) {
    if (pages_[it.second].IsDirty()) {
      // pages_[it.second].RLatch();
      disk_manager_->WritePage(pages_[it.second].GetPageId(), pages_[it.second].GetData());
      // pages_[it.second].RUnlatch();
      pages_[it.second].is_dirty_ = false;
    }
  }
  // page_table_lock.unlock();
}

}  // namespace bustub
