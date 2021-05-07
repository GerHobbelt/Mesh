// -*- mode: c++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright 2019 The Mesh Authors. All rights reserved.
// Use of this source code is governed by the Apache License,
// Version 2.0, that can be found in the LICENSE file.

#pragma once
#ifndef MESH_ARENA_H
#define MESH_ARENA_H

#include <algorithm>
#include <array>
#include <mutex>
#include <atomic>

#include "internal.h"
#include "meshable_arena.h"
#include "mini_heap.h"
#include "epoch_lock.h"

#include "heaplayers.h"

using namespace HL;

namespace mesh {

class CentralCache {
public:
  CentralCache() {
  }
  void init() {
    _readIndex = _writeIndex = 0;
  }

  inline uint32_t size() const {
    lock_guard<internal::SpinLockType> lock(_lock);
    return _writeIndex - _readIndex;
  }

  inline bool full() const {
    return size() >= kMaxCentralCacheLength;
  }
  inline bool empty() const {
    lock_guard<internal::SpinLockType> lock(_lock);
    return _readIndex == _writeIndex;
  }

  bool push(void *head) {
    lock_guard<internal::SpinLockType> lock(_lock);
    if (_writeIndex - _readIndex >= kMaxCentralCacheLength) {
      return false;
    }
    const auto writeIndex = _writeIndex % kMaxCentralCacheLength;
    ++_writeIndex;
    MeshEntry &cache = _cache[writeIndex];
    cache.head = head;
    cache.timestamp = time::now_milliseconds();
    return true;
  }

  bool pop(void *&head) {
    lock_guard<internal::SpinLockType> lock(_lock);
    if (_readIndex == _writeIndex) {
      return false;
    }
    d_assert(_writeIndex > _readIndex);
    --_writeIndex;
    const auto popIndex = _writeIndex % kMaxCentralCacheLength;
    MeshEntry &cache = _cache[popIndex];
    head = cache.head;
    return true;
  }

  bool pop_timeout(void *&head, uint64_t timeout) {
    lock_guard<internal::SpinLockType> lock(_lock);
    if (_writeIndex - _readIndex < kMinCentralCacheLength) {
      return false;
    }
    d_assert(_writeIndex > _readIndex);
    const auto popIndex = _readIndex % kMaxCentralCacheLength;
    MeshEntry &cache = _cache[popIndex];
    const auto deadline = time::now_milliseconds() - std::max(timeout / (_writeIndex - _readIndex), 100ul);
    if (cache.timestamp > deadline) {
      return false;
    }
    ++_readIndex;
    if (_readIndex >= kMaxCentralCacheLength) {
      _readIndex -= kMaxCentralCacheLength;
      _writeIndex -= kMaxCentralCacheLength;
    }
    head = cache.head;
    return true;
  }

private:
  struct MeshEntry {
    void *head;
    uint64_t timestamp;
  };
  MeshEntry _cache[kMaxCentralCacheLength];
  uint32_t _readIndex{0};
  uint32_t _writeIndex{0};
  mutable internal::SpinLockType _lock{};
} CACHELINE_ALIGNED;

class MiniheapFreeList {
public:
  MiniheapFreeList() {
  }
  class FreeListEntry {
  public:
    MiniHeapListEntry head{list::Head, list::Head};
    size_t size{0};
  };

  inline FreeListEntry *freelistFor(uint8_t freelistId) {
    switch (freelistId) {
    case list::Empty:
      return &_empty;
    case list::Partial:
      return &_partial;
    case list::PartialFull:
      return &_partialFull;
    case list::Full:
      return &_full;
    }
    // remaining case is 'attached', for which there is no freelist
    return nullptr;
  }

  inline bool postFreeLocked(MiniHeap *mh, int sizeClass, size_t inUse) {
    const auto currFreelistId = mh->freelistId();
    auto currFreelist = freelistFor(currFreelistId);
    const auto max = mh->maxCount();

    FreeListEntry *list;
    uint8_t newListId;

    if (inUse == 0) {
      // if the miniheap is already in the right list there is nothing to do
      if (currFreelistId == list::Empty) {
        return false;
      }
      newListId = list::Empty;
      list = &_empty;
    } else if (inUse == max) {
      if (currFreelistId == list::Full) {
        return false;
      }
      newListId = list::Full;
      list = &_full;
    } else {
      if (inUse < _occupancyCutoff) {
        newListId = list::Partial;
        list = &_partial;
      } else {
        newListId = list::PartialFull;
        list = &_partialFull;
      }
      if (currFreelistId == newListId) {
        return false;
      }
    }

    MiniHeapListEntry *currListHead = nullptr;
    if (mh->getFreelist()->next().hasValue()) {
      currListHead = &currFreelist->head;
      d_assert(currFreelist->size > 0);
      currFreelist->size--;
    }
    list->head.add(currListHead, newListId, list::Head, mh);
    list->size++;

    return _empty.size > kBinnedTrackerMinEmpty;
  }

  void remove(MiniHeap *mh) {
    auto currFreelist = freelistFor(mh->freelistId());
    d_assert(currFreelist && currFreelist->size > 0);
    mh->getFreelist()->remove(&currFreelist->head);
    currFreelist->size--;
  }

  float occupancyCutoff() const {
    return _occupancyCutoff;
  }

  void setOccupancyCutoff(uint32_t occupancyCutoff) {
    _occupancyCutoff = occupancyCutoff;
  }

  void calcOccupancyCutoff(size_t sizeClass) {
    auto size = _partial.size;
    size /= 1024;
    float cutoff = 0.8 - 0.03 * size;
    if (cutoff < 0.3) {
      cutoff = 0.3;
    }
    uint32_t objectCount = SizeMap::ObjectCountForClass(sizeClass);
    _occupancyCutoff = objectCount * cutoff;
  }

public:
  auto &emptyList() {
    return _empty;
  }
  auto &partialList() {
    return _partial;
  }
  auto &partialFullList() {
    return _partialFull;
  }
  auto &fullList() {
    return _full;
  }

  auto &meshEpoch() {
    return _meshEpoch;
  }

  auto &lock() {
    return _lock;
  }

  auto lastSize() {
    return _lastSize;
  }
  void setLastSize() {
    _lastSize = _partial.size + _partialFull.size;
  }

private:
  FreeListEntry _empty{};
  FreeListEntry _partial{};
  FreeListEntry _partialFull{};
  FreeListEntry _full{};

  mutable mutex _lock{};

  EpochLock _meshEpoch{};
  uint32_t _occupancyCutoff{0};
  uint32_t _lastSize{0};
} CACHELINE_ALIGNED;

class GlobalHeap;

static constexpr size_t kMaxSizeClassCache = kPageSize / sizeof(uint64_t);
static constexpr size_t kSizeClassCacheMask = (1ul << 27) - 1;

class Arena {
private:
  DISALLOW_COPY_AND_ASSIGN(Arena);

public:
  Arena() {
    for (size_t sizeClass = 0; sizeClass < kClassSizesMax; ++sizeClass) {
      uint32_t objectCount = SizeMap::ObjectCountForClass(sizeClass);
      if (SizeMap::ByteSizeForClass(sizeClass) < kPageSize) {
        _freelist[sizeClass].setOccupancyCutoff(static_cast<uint32_t>(objectCount * kOccupancyCutoff));
      } else {
        _freelist[sizeClass].setOccupancyCutoff(static_cast<uint32_t>(objectCount + 1));
      }
    }
  }

  void Init(uint32_t id, GlobalHeap *global);

  inline void flushAllBins() {
    for (size_t sizeClass = 0; sizeClass < kNumBins; sizeClass++) {
      lock_guard<mutex> lock(_freelist[sizeClass].lock());
      flushBinLocked(sizeClass);
    }
  }

  void untrackMiniheapLocked(MiniHeap *mh) {
    // mesh::debug("%p (%u) untracked!\n", mh, GetMiniHeapID(mh));
    _freelist[mh->sizeClass()].remove(mh);
  }

  void freeFor(MiniHeap *mh, void *ptr, const void *arenaBegin);

  inline bool postFreeLocked(MiniHeap *mh, int sizeClass, size_t inUse) {
    // its possible we raced between reading isAttached + grabbing a lock.
    // just check here to avoid having to play whack-a-mole at each call site.
    // if (mh->isAttached()) {
    //   return false;
    // }
    d_assert(!mh->isMeshed());
    d_assert(id() == mh->arenaId());
    if (_freelist[sizeClass].postFreeLocked(mh, sizeClass, inUse)) {
      flushBinLocked(sizeClass);
    }
    return true;
  }

  bool fillFromList(size_t sizeClass, size_t objectCount, MiniheapFreeList::FreeListEntry &freelist,
                    internal::RelaxedFixedBitmap &localBits, uintptr_t &spanStart) {
    MiniHeapID nextId = freelist.head.prev();
    if (nextId == list::Head) {
      return false;
    }
    auto mh = GetMiniHeap(nextId);
    d_assert(!mh->isFull());
    d_assert(!mh->isMeshed());
    internal::RelaxedFixedBitmap newBitmap{objectCount};
    newBitmap.setAll(objectCount);
    if (sizeClass > 7ul) {
      mh->bitmap().setAndExchange1(localBits.mut_bits(), newBitmap.bits());
    } else {
      mh->bitmap().setAndExchangeAll(localBits.mut_bits(), newBitmap.bits());
    }
    spanStart = mh->getSpanStart(_arenaBegin);
    postFreeLocked(mh, mh->sizeClass(), mh->inUseCount());
    return true;
  }

  bool selectForReuse(size_t sizeClass, size_t objectCount, internal::RelaxedFixedBitmap &localBits,
                      uintptr_t &spanStart) {
    auto &freelist = _freelist[sizeClass];
    if (freelist.partialList().size % 2) {
      if (fillFromList(sizeClass, objectCount, freelist.partialList(), localBits, spanStart)) {
        return true;
      }
      if (fillFromList(sizeClass, objectCount, freelist.partialFullList(), localBits, spanStart)) {
        return true;
      }
    } else {
      if (fillFromList(sizeClass, objectCount, freelist.partialFullList(), localBits, spanStart)) {
        return true;
      }
      if (fillFromList(sizeClass, objectCount, freelist.partialList(), localBits, spanStart)) {
        return true;
      }
    }
    return fillFromList(sizeClass, objectCount, freelist.emptyList(), localBits, spanStart);
  }

  bool allocFromCentralCache(int sizeClass, void *&head, uint32_t &size);
  void releaseToCentralCache(int sizeClass, void *head, uint32_t size, pid_t current);

  size_t freePtrList(void *head);
  void flushCentralCache();
  size_t flushCentralCache(size_t sizeClass, size_t limit);

  void allocCentralCacheSlowly(size_t sizeClass, size_t objectCount, internal::RelaxedFixedBitmap &localBits,
                               uintptr_t &spanStart);

  void freeMiniheapAfterMeshLocked(size_t sizeClass, MiniHeap *mh);
  void freeMiniheapLocked(size_t sizeClass, MiniHeap *mh);
  MiniHeap *allocMiniheapLocked(size_t sizeClass, size_t arenaId, size_t pageCount, size_t objectCount,
                                size_t objectSize, size_t pageAlignment = 1);

  void flushBinLocked(size_t sizeClass);

  void dumpMiniHeaps(const char *prefix, size_t sizeClass, const MiniHeapListEntry *miniheaps, int level);
  void dumpFreeList(int level);

  auto &freelist(size_t sizeClass) {
    return _freelist[sizeClass];
  }

  inline uint32_t id() const {
    return _id;
  }

  inline size_t refcount() const {
    return _refcount.load();
  }

  inline void incRefcount() {
    _refcount++;
  }
  inline void decRefcount() {
    _refcount--;
  }

  inline size_t lastMeshClass() const {
    return _lastMeshClass;
  }

  inline void setLastMeshClass(size_t last) {
    _lastMeshClass = last;
  }

private:
  uint32_t _id{0};
  uint32_t _pad{0};

  atomic_size_t _refcount{0};
  GlobalHeap *_global{nullptr};
  void *_arenaBegin;

  uint64_t _flushCentralCacheDelay{kFlushCentralCacheDelay};
  atomic_size_t _lastFreeClass{0};
  size_t _lastMeshClass{0};
  MiniheapFreeList _freelist[kNumBins];
  CentralCache _cache[kNumBins];
};

}  // namespace mesh

#endif  // MESH_ARENA_H
