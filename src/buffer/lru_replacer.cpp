//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  unpinned_size = 0;
  iterlookup = new std::list<frame_status>::iterator[num_pages];
  flag = std::prev(unpinned_frames.begin());
  for (size_t i = 0; i < num_pages; i++) {
    iterlookup[i] = flag;
  }
  cursor = unpinned_frames.begin();
}

LRUReplacer::~LRUReplacer() { delete[] iterlookup; }

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  bool find_one = false;
  // unpinned_frames_lock.lock();
  // cursor_lock.lock();
  std::scoped_lock<std::mutex> lk{latch_};
  if (unpinned_size != 0) {
    auto initial_position = std::next(cursor);
    for (; !find_one && cursor != unpinned_frames.end();) {
      if (!cursor->ref) {
        *frame_id = cursor->frame_id;
        auto to_be_erased = cursor;
        cursor = std::next(cursor);
        if (cursor == unpinned_frames.end()) {
          cursor = unpinned_frames.begin();
        }
        unpinned_frames.erase(to_be_erased);
        unpinned_size--;
        iterlookup[*frame_id] = flag;
        find_one = true;
      } else {
        cursor->ref = false;
        cursor = std::next(cursor);
      }
    }
    if (!find_one) {
      for (cursor = unpinned_frames.begin(); !find_one && cursor != initial_position;) {
        if (!cursor->ref) {
          *frame_id = cursor->frame_id;
          auto to_be_erased = cursor;
          cursor = std::next(cursor);
          if (cursor == unpinned_frames.end()) {
            cursor = unpinned_frames.begin();
          }
          unpinned_frames.erase(to_be_erased);
          unpinned_size--;
          iterlookup[*frame_id] = flag;
          find_one = true;
        } else {
          cursor->ref = true;
          cursor = std::next(cursor);
        }
      }
      if (cursor == unpinned_frames.end()) {
        cursor = unpinned_frames.begin();
      }
    }
  }
  return find_one;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // unpinned_frames_lock.lock();
  std::scoped_lock<std::mutex> lk{latch_};
  if (iterlookup[frame_id] != flag) {  // if not pinned
    unpinned_size--;
    if (iterlookup[frame_id] == cursor) {
      cursor = std::next(cursor);
      if (cursor == unpinned_frames.end()) {
        cursor = unpinned_frames.begin();
      }
      unpinned_frames.erase(iterlookup[frame_id]);
    } else {
      unpinned_frames.erase(iterlookup[frame_id]);
    }
    iterlookup[frame_id] = flag;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  // unpinned_frames_lock.lock();
  std::scoped_lock<std::mutex> lk{latch_};
  // bool already_exist = false;
  if (iterlookup[frame_id] == flag) {
    unpinned_frames.emplace_back(frame_status(frame_id, true));
    // 这里（104行），我在一个page新unpin的时候（正好从pin转换到unpin的状态），是
    // 把它的ref bit设置为1了的。但是实际上设置为0仍然能够通过测试并且满分
    // 而且ref bit设置为0好像速度更快
    // 实际上测试可能仅仅是按照先来后到顺序进行LRU， ref bit根本没用
    // 就算你设计一个更好的LRU算法，仍然没法通过测试。因为测试里面有检测victim出来的frames先后顺序问题
    // 万一你victim出来的和先来后到出来的不一样就通不过
    iterlookup[frame_id] = std::prev(unpinned_frames.end());
    unpinned_size++;
    if (unpinned_size == 1) {
      cursor = unpinned_frames.begin();
    }
  }
}

size_t LRUReplacer::Size() {
  // unpinned_frames_lock.lock();
  std::scoped_lock<std::mutex> lk{latch_};
  // unpinned_frames_lock.unlock();
  return unpinned_size;
}

}  // namespace bustub
