//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);
    return EvictInstance(frame_id);
 }

auto LRUKReplacer::EvictInstance(frame_id_t *frame_id) -> bool { 
    //std::unique_lock<std::mutex> lock(latch_);
    //Print_mem();
    // there are no evictable frames
    if (curr_size_ == 0) { return false; }
    // 在 new_frame（+inf）里面淘汰
    for (auto it = new_frame_.rbegin(); it != new_frame_.rend(); it++){
        // evictable
        if (frame_evictable_[(*it)]){
            *frame_id = *it;
            // delete
            frame_timestamp_.erase(*it);
            frame_evictable_.erase(*it);
            frame_k_.erase(*it);
            new_frame_umap_.erase(*it);
            new_frame_.remove(*it);
            curr_size_--;
            return true;
        }
    }
    // 在 cache_frame 里面淘汰
    for (auto it = cache_frame_.begin(); it != cache_frame_.end(); it++){
        // evictable
        if (frame_evictable_[(*it).first]){
            *frame_id = (*it).first;
            // delete
            frame_timestamp_.erase((*it).first);
            frame_evictable_.erase((*it).first);
            frame_k_.erase((*it).first);
            cache_frame_umap_.erase((*it).first);
            cache_frame_.remove(*it);
            curr_size_--;
            return true;
        }
    }
    return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    if(frame_k_.find(frame_id) == frame_k_.end()){ frame_k_[frame_id] = 0; }
    frame_k_[frame_id]++;
    frame_timestamp_[frame_id].push_back(current_timestamp_);
    current_timestamp_++;
    auto cnt = frame_k_[frame_id];
    // if 新加入
    if (cnt == 1) {
        // 满，驱逐
        if(curr_size_ == replacer_size_){
            frame_id_t tmp_frame;
            EvictInstance(&tmp_frame);
        }
        // 未满，加入 new_
        frame_evictable_[frame_id] = true; // 默认可驱逐
        curr_size_++;
        new_frame_.push_front(frame_id);
        new_frame_umap_[frame_id] = new_frame_.begin();
        return;
    }
    // 达到 k次，从 new_中加入到 cache_中
    if (cnt == k_) {
        new_frame_.remove(frame_id);
        new_frame_umap_.erase(frame_id);
        auto kth_timestamp = frame_timestamp_[frame_id].front();
        auto new_cache = std::make_pair(frame_id, kth_timestamp);
        auto it = std::upper_bound(cache_frame_.begin(),cache_frame_.end(),new_cache
            ,[&](const std::pair<frame_id_t, size_t>& a, const std::pair<frame_id_t, size_t>& b) {
                return a.second < b.second;
            });
        it = cache_frame_.insert(it,new_cache);
        cache_frame_umap_[frame_id] = it;
        return;
    }
    // >k 次，在 cache_中调整
    if (cnt > k_) {
        // 删除时间戳冗余项
        frame_timestamp_[frame_id].erase(frame_timestamp_[frame_id].begin());
        // 除旧
        cache_frame_.erase(cache_frame_umap_[frame_id]);
        // 填新
        auto kth_timestamp = frame_timestamp_[frame_id].front();
        auto new_cache = std::make_pair(frame_id, kth_timestamp);
        auto it = std::upper_bound(cache_frame_.begin(),cache_frame_.end(),new_cache
            ,[&](const std::pair<frame_id_t, size_t>& a, const std::pair<frame_id_t, size_t>& b) {
                return a.second < b.second;
            });
        it = cache_frame_.insert(it,new_cache);
        cache_frame_umap_[frame_id] = it;
        return;
    }
    // <k 次，不需要处理
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) { 
    std::scoped_lock<std::mutex> lock(latch_);
    if(frame_evictable_.find(frame_id) == frame_evictable_.end()) return;
    if(frame_evictable_[frame_id] == set_evictable) return;
    frame_evictable_[frame_id] = set_evictable;
    if(set_evictable){curr_size_++; replacer_size_++;}
    else{curr_size_--; replacer_size_--;}
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    // 防止重复remove
    if(frame_evictable_.find(frame_id) == frame_evictable_.end()) return;
    if (frame_evictable_[frame_id]) { curr_size_--; }
    else { // non-evictable, cannot be removed
        // replacer_size_++; 
        return;
    }
    frame_timestamp_.erase(frame_id);
    frame_evictable_.erase(frame_id);
    frame_k_.erase(frame_id);
    for (auto it = new_frame_.begin(); it != new_frame_.end(); it++){
        if (*it == frame_id) {
            new_frame_umap_.erase(*it);
            new_frame_.remove(*it);
            return;
        }
    }
    for (auto it = cache_frame_.begin(); it != cache_frame_.end(); it++){
        if((*it).first == frame_id) {
            cache_frame_umap_.erase((*it).first);
            cache_frame_.remove(*it);
            return;
        }
    }
}

auto LRUKReplacer::Size() -> size_t { std::scoped_lock<std::mutex> lock(latch_); return curr_size_; }

}  // namespace bustub
