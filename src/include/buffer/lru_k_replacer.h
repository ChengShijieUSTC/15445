//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <iostream>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

using timestamp_t = std::list<size_t>;//记录单个页时间戳的列表

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  auto EvictInstance(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

  void Print_mem(){
    std::cout << "new_frame_:" << std::endl;
    for (auto it = new_frame_.begin(); it != new_frame_.end(); it++){
      std::cout << *it << "(" << frame_k_[(*it)] << std::endl;
    }
    std::cout << "cache_frame_:" << std::endl;
    for (auto it = cache_frame_.begin(); it != cache_frame_.end(); it++){
      std::cout << (*it).first << "(" << frame_k_[(*it).first] << std::endl;
    }
    std::cout<< "curr_size_:" << std::endl;
    std::cout<< curr_size_ << std::endl;
    std::cout<< "replacer_size_:" << std::endl;
    std::cout<< replacer_size_ << std::endl;
  }

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  // 当前时间戳，每次recode后+1
  size_t current_timestamp_{0};
  // 当前存放的可驱逐页面数量
  size_t curr_size_{0};
  // 最大可驱逐页面数量。初始为主存最多可存页面数
  size_t replacer_size_;
  // lru-k的k
  size_t k_;
  // latch
  std::mutex latch_;
  // 记录每个frame的 timestamp 列表(只维护k个，这样.begin()就可以直接获取倒数第k次)
  std::unordered_map<frame_id_t, timestamp_t> frame_timestamp_;
  // 记录每个frame的 evictable 
  std::unordered_map<frame_id_t, bool> frame_evictable_;
  // 记录每个frame的 访问次数
  std::unordered_map<frame_id_t, size_t> frame_k_;
  // 未满 k 次的 LRU （头插尾删）
  std::list<frame_id_t> new_frame_;
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> new_frame_umap_;
  // 达到 k 次的 LRU -- pair(frame_id, last k-th timestamp)
  std::list<std::pair<frame_id_t, size_t>> cache_frame_;
  std::unordered_map<frame_id_t, std::list<std::pair<frame_id_t, size_t>>::iterator> cache_frame_umap_;
};

}  // namespace bustub
