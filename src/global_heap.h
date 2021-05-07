// -*- mode: c++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright 2019 The Mesh Authors. All rights reserved.
// Use of this source code is governed by the Apache License,
// Version 2.0, that can be found in the LICENSE file.

#pragma once
#ifndef MESH_GLOBAL_HEAP_H
#define MESH_GLOBAL_HEAP_H

#include <algorithm>
#include <array>
#include <mutex>

#include "internal.h"
#include "meshable_arena.h"
#include "mini_heap.h"

#include "heaplayers.h"

#include "arena.h"

using namespace HL;

namespace mesh {

static constexpr std::pair<MiniHeapListEntry, size_t> Head{MiniHeapListEntry{list::Head, list::Head}, 0};

class GlobalHeapStats {
public:
  atomic_size_t meshCount;
};

class GlobalHeap : public MeshableArena {
private:
  DISALLOW_COPY_AND_ASSIGN(GlobalHeap);
  typedef MeshableArena Super;

  static_assert(HL::gcd<MmapHeap::Alignment, Alignment>::value == Alignment,
                "expected MmapHeap to have 16-byte alignment");

public:
  enum { Alignment = 16 };

  GlobalHeap() : Super(), _maxObjectSize(SizeMap::ByteSizeForClass(kNumBins - 1)), _lastMesh{time::now()} {
    for (uint32_t i = 0; i < kMaxArena; ++i) {
      _arena[i].Init(i, this);
    }
  }

  inline void dumpStrings() const {
    mesh::debug("TODO: reimplement printOccupancy\n");
    // for (size_t i = 0; i < kNumBins; i++) {
    //   _littleheaps[i].printOccupancy();
    // }
  }

  inline void flushAllBins() {
  }

  void scavenge() {
    Super::scavenge();
  }

  void dumpStats(int level, bool beDetailed) const;

  // must be called with exclusive _mhRWLock held
  inline MiniHeap *ATTRIBUTE_ALWAYS_INLINE allocMiniheapLocked(size_t sizeClass, size_t arenaId, size_t pageCount,
                                                               size_t objectCount, size_t objectSize,
                                                               size_t pageAlignment = 1) {
    auto mh = Super::allocMiniheap(sizeClass, arenaId, pageCount, objectCount, objectSize, pageAlignment);
    // mesh::debug("%p (%u) created!\n", mh, GetMiniHeapID(mh));
    return mh;
  }

  inline void *pageAlignedAlloc(size_t pageAlignment, size_t pageCount) {
    // if given a very large allocation size (e.g. (uint64_t)-8), it is possible
    // the pageCount calculation overflowed.  An allocation that big is impossible
    // to satisfy anyway, so just fail early.
    if (unlikely(pageCount == 0)) {
      return nullptr;
    }

    MiniHeap *mh = allocMiniheapLocked(kClassSizesMax, 0ul, pageCount, 1, pageCount * kPageSize, pageAlignment);

    d_assert(mh->isLargeAlloc());
    d_assert(mh->spanSize() == pageCount * kPageSize);
    // d_assert(mh->objectSize() == pageCount * kPageSize);

    void *ptr = reinterpret_cast<void *>(mh->getSpanStart(arenaBegin()));

    return ptr;
  }

  inline bool postFreeLocked(MiniHeap *mh, int sizeClass, size_t inUse) {
    // its possible we raced between reading isAttached + grabbing a lock.
    // just check here to avoid having to play whack-a-mole at each call site.
    d_assert(!mh->isMeshed());
    return _arena[mh->arenaId()].postFreeLocked(mh, sizeClass, inUse);
  }

  // large, page-multiple allocations
  void *ATTRIBUTE_NEVER_INLINE malloc(size_t sz);

  inline MiniHeap *ATTRIBUTE_ALWAYS_INLINE miniheapFor(const void *ptr) const {
    auto mh = reinterpret_cast<MiniHeap *>(Super::lookupMiniheap(ptr));
    return mh;
  }

  inline MiniHeap *ATTRIBUTE_ALWAYS_INLINE miniheapForID(const MiniHeapID id) const {
    auto mh = reinterpret_cast<MiniHeap *>(_mhAllocator.ptrFromOffset(id.value()));
    __builtin_prefetch(mh, 1, 2);
    return mh;
  }

  inline MiniHeapID miniheapIDFor(const MiniHeap *mh) const {
    return MiniHeapID{_mhAllocator.offsetFor(mh)};
  }

  inline auto &mhAllocator() {
    return _mhAllocator;
  }

  void untrackMiniheapLocked(MiniHeap *mh) {
    // mesh::debug("%p (%u) untracked!\n", mh, GetMiniHeapID(mh));
    _arena[mh->arenaId()].untrackMiniheapLocked(mh);
  }

  void freeLargeMiniheap(MiniHeap *mh) {
    Super::freeMiniheap(mh, internal::Dirty);
  }

  void freeMiniheapLocked(MiniHeap *mh) {
    MiniHeap *toFree[kMaxMeshes];
    size_t last = 0;

    // memset(toFree, 0, sizeof(*toFree) * kMaxMeshes);

    // avoid use after frees while freeing
    mh->forEachMeshed([&](MiniHeap *mh) {
      toFree[last++] = mh;
      return false;
    });

    for (size_t i = 0; i < last; i++) {
      MiniHeap *mh = toFree[i];
      const bool isMeshed = mh->isMeshed();
      if (isMeshed) {
        mh->setMeshedLeader(MiniHeapID{});
      }
      const auto type = isMeshed ? internal::PageType::Meshed : internal::PageType::Dirty;
      Super::freeMiniheap(mh, type);
    }

    mh = nullptr;
  }

  void ATTRIBUTE_NEVER_INLINE free(void *ptr, uint32_t arenaId, MiniHeap *mh);

  void ATTRIBUTE_NEVER_INLINE free(void *ptr);

  inline size_t getSize(uint32_t mhId) const {
    if (unlikely(mhId == 0))
      return 0;

    auto mh = miniheapForID(MiniHeapID{mhId});
    if (likely(mh)) {
      return mh->objectSize();
    } else {
      return 0;
    }
  }

  int mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen);

  size_t getAllocatedMiniheapCount() const {
    return 0;
  }

  void setMeshPeriodMs(std::chrono::milliseconds period) {
    _meshPeriodMs = period;
  }

  void setFlushCentralCacheDelay(uint64_t delay) {
    _flushCentralCacheDelay = delay;
  }

  void lock() {
    _miniheapLock.lock();
    _spanLock.lock();
    // internal::Heap().lock();
  }

  void unlock() {
    // internal::Heap().unlock();
    _spanLock.unlock();
    _miniheapLock.unlock();
  }

  // PUBLIC ONLY FOR TESTING
  // after call to meshLocked() completes src is a nullptr
  void ATTRIBUTE_NEVER_INLINE meshLocked(MiniHeap *dst, MiniHeap *&src){};
  void ATTRIBUTE_NEVER_INLINE meshLocked(MiniHeap *dst, MiniHeap *&src, internal::vector<Span> &fCmdSpans);

  inline void ATTRIBUTE_ALWAYS_INLINE maybeMesh(uint32_t arenaId) {
    if (!kMeshingEnabled) {
      return;
    }

    // if child , we don't mesh.
    if (!_needCOWScan) {
      return;
    }

    const auto now = time::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(now - _lastMesh);

    if (likely(duration < _meshPeriodMs)) {
      return;
    }

    lock_guard<mutex> lock(_miniheapLock);

    {
      // ensure if two threads tried to grab the mesh lock at the same
      // time, the second one bows out gracefully without meshing
      // twice in a row.
      const auto lockedNow = time::now();
      auto duration = chrono::duration_cast<chrono::milliseconds>(lockedNow - _lastMesh);

      if (unlikely(duration < _meshPeriodMs)) {
        return;
      }
    }

    _lastMesh = now;

    if (_isCOWRunning && _needCOWScan) {
      processCOWPage();
      return;
    }

    if (_meshPeriod == 0) {
      return;
    }

    if (_meshPeriodMs == kZeroMs) {
      return;
    }

    meshAllSizeClassesLocked(arenaId);
  }

  inline bool okToProceed(void *ptr) const {
    if (ptr == nullptr) {
      return false;
    }

    lock_guard<mutex> lock(_miniheapLock);
    auto mh = miniheapFor(ptr);
    return mh != nullptr;
  }

  inline bool tryCopyOnWrite(void *ptr) {
    if (ptr == nullptr) {
      return false;
    }

    lock_guard<mutex> lock(_miniheapLock);

    MiniHeap *mh = miniheapFor(ptr);

    if (mh == nullptr) {
      return false;
    }

    return moveMiniHeapToNewFile(mh, ptr);
  }

  inline internal::vector<MiniHeap *> meshingCandidatesLocked(int sizeClass) const {
    // FIXME: duplicated with code in halfSplit
    internal::vector<MiniHeap *> bucket{};
    return bucket;
  }

  void dumpMiniHeaps(const char *prefix, size_t sizeClass, const MiniHeapListEntry *miniheaps, int level);
  void dumpList(int level);

  size_t unboundMeshSlowly(MiniHeap *mh);

  Arena *allocArena();

private:
  // check for meshes in all size classes -- must be called LOCKED
  void meshAllSizeClassesLocked(uint32_t arenaId);
  // meshSizeClassLocked returns the number of merged sets found
  size_t meshSizeClassLocked(uint32_t areaneId, size_t sizeClass, MergeSetArray &mergeSets, SplitArray &all);

  void processCOWPage();

  const size_t _maxObjectSize;
  atomic_size_t _meshPeriod{kDefaultMeshPeriod};
  std::chrono::milliseconds _meshPeriodMs{kMeshPeriodMs};

  mutable mutex _miniheapLock{};

  GlobalHeapStats _stats{};

  // XXX: should be atomic, but has exception spec?
  time::time_point _lastMesh;
  size_t _lastMeshClass{0};

  uint64_t _flushCentralCacheDelay{kFlushCentralCacheDelay};

  Arena _arena[kMaxArena];

  atomic_size_t _lastFreeClass{0};
};

static_assert(kNumBins == 37, "if this changes, add more 'Head's above");
static_assert(sizeof(std::array<MiniHeapListEntry, kNumBins>) == kNumBins * 8, "list size is right");
static_assert(sizeof(GlobalHeap) < (kNumBins * 8 * 3 + 64 * 7 + 100000000), "gh small enough");
}  // namespace mesh

#endif  // MESH_GLOBAL_HEAP_H
