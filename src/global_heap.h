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

using namespace HL;

namespace mesh {

static constexpr std::pair<MiniHeapListEntry, size_t> Head{MiniHeapListEntry{list::Head, list::Head}, 0};

class EpochLock {
private:
  DISALLOW_COPY_AND_ASSIGN(EpochLock);

public:
  EpochLock() {
  }

  inline size_t ATTRIBUTE_ALWAYS_INLINE current() const noexcept {
    return _epoch.load(std::memory_order::memory_order_seq_cst);
  }

  inline size_t ATTRIBUTE_ALWAYS_INLINE isSame(size_t startEpoch) const noexcept {
    return current() == startEpoch;
  }

  inline void ATTRIBUTE_ALWAYS_INLINE lock() noexcept {
    // make sure that the previous epoch was even
    const auto old = _epoch.fetch_add(1, std::memory_order::memory_order_seq_cst);
    hard_assert(old % 2 == 0);
  }

  inline void ATTRIBUTE_ALWAYS_INLINE unlock() noexcept {
#ifndef NDEBUG
    // make sure that the previous epoch was odd
    const auto old = _epoch.fetch_add(1, std::memory_order::memory_order_seq_cst);
    d_assert(old % 2 == 1);
#else
    _epoch.fetch_add(1, std::memory_order::memory_order_seq_cst);
#endif
  }

private:
  atomic_size_t _epoch{0};
};

class MiniHeapAliveStats {
public:
  double totalAlive{0};
  size_t maxAlive{0};
  size_t freeCount{0};
  internal::SpinLockType lock{};

  void Record(uint32_t alive) {
    lock_guard<internal::SpinLockType> lockg(lock);
    totalAlive += alive;
    ++freeCount;
    if (alive > maxAlive) {
      maxAlive = alive;
    }
  }
};

class GlobalHeapStats {
public:
  atomic_size_t meshCount;
  atomic_size_t mhFreeCount;
  atomic_size_t mhAllocCount;
  atomic_size_t mhHighWaterMark;
  MiniHeapAliveStats alives[kNumBins + 1];
};

class CentralCache {
public:
  CentralCache() {
  }
  void init() {
    _readIndex = _writeIndex = 0;
  }

  size_t totalCache() const {
    size_t sum = 0;
    for (uint32_t i = _readIndex; i < _writeIndex; ++i) {
      sum += _cache[i % kMaxCentralCacheLength].size;
    }
    return sum;
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

  bool push(void *head, void *tail, uint32_t size) {
    lock_guard<internal::SpinLockType> lock(_lock);
    if (_writeIndex - _readIndex >= kMaxCentralCacheLength) {
      return false;
    }
    const auto writeIndex = _writeIndex % kMaxCentralCacheLength;
    ++_writeIndex;
    MeshEntry &cache = _cache[writeIndex];
    cache.head = head;
    cache.tail = tail;
    cache.timestamp = time::now_milliseconds();
    cache.size = size;
    return true;
  }

  bool pop(void *&head, void *&tail, uint32_t &size) {
    lock_guard<internal::SpinLockType> lock(_lock);
    if (_readIndex == _writeIndex) {
      return false;
    }
    d_assert(_writeIndex > _readIndex);
    --_writeIndex;
    const auto popIndex = _writeIndex % kMaxCentralCacheLength;
    MeshEntry &cache = _cache[popIndex];
    head = cache.head;
    tail = cache.tail;
    size = cache.size;
    return true;
  }

  bool pop_timeout(void *&head, void *&tail, uint32_t &size, uint64_t timeout) {
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
    tail = cache.tail;
    size = cache.size;
    return true;
  }

private:
  struct MeshEntry {
    void *head;
    void *tail;
    uint64_t timestamp;
    uint32_t size;
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
      return &empty;
    case list::Partial:
      return &partial;
    case list::PartialFull:
      return &partialFull;
    case list::Full:
      return &full;
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
      list = &empty;
    } else if (inUse == max) {
      if (currFreelistId == list::Full) {
        return false;
      }
      newListId = list::Full;
      list = &full;
    } else {
      if (inUse < SizeMap::OccupancyCutoffForClass(sizeClass)) {
        newListId = list::Partial;
        list = &partial;
      } else {
        newListId = list::PartialFull;
        list = &partialFull;
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

    return empty.size > kBinnedTrackerMaxEmpty;
  }

  FreeListEntry empty{};
  FreeListEntry partial{};
  FreeListEntry partialFull{};
  FreeListEntry full{};

  mutable mutex lock{};
} CACHELINE_ALIGNED;

class GlobalHeap : public MeshableArena {
private:
  DISALLOW_COPY_AND_ASSIGN(GlobalHeap);
  typedef MeshableArena Super;

  static_assert(HL::gcd<MmapHeap::Alignment, Alignment>::value == Alignment,
                "expected MmapHeap to have 16-byte alignment");

public:
  enum { Alignment = 16 };

  GlobalHeap() : Super(), _maxObjectSize(SizeMap::ByteSizeForClass(kNumBins - 1)), _lastMesh{time::now()} {
    for (int i = 0; i < kNumBins; ++i) {
      _cache[i].init();
    }
  }

  inline bool maybeMeshing() const {
    return _meshEpoch.current() % 2 == 1;
  }

  inline void dumpStrings() const {
    mesh::debug("TODO: reimplement printOccupancy\n");
    // for (size_t i = 0; i < kNumBins; i++) {
    //   _littleheaps[i].printOccupancy();
    // }
  }

  inline void flushAllBins() {
    for (size_t sizeClass = 0; sizeClass < kNumBins; sizeClass++) {
      lock_guard<mutex> lock(_freelist[sizeClass].lock);
      flushBinLocked(sizeClass);
    }
  }

  void scavenge(bool force = false) {
    Super::scavenge(force);
  }

  void dumpStats(int level, bool beDetailed) const;

  // must be called with exclusive _mhRWLock held
  inline MiniHeap *ATTRIBUTE_ALWAYS_INLINE allocMiniheapLocked(size_t sizeClass, size_t pageCount, size_t objectCount,
                                                               size_t objectSize, size_t pageAlignment = 1) {
    d_assert(0 < pageCount);

    void *buf = _mhAllocator.alloc();
    d_assert(buf != nullptr);

    // allocate out of the arena
    Span span{0, 0};
    char *__attribute__((__unused__)) spanBegin = Super::pageAlloc(span, pageCount, pageAlignment);
    d_assert(spanBegin != nullptr);
    d_assert((reinterpret_cast<uintptr_t>(spanBegin) / kPageSize) % pageAlignment == 0);

    MiniHeap *mh = new (buf) MiniHeap(arenaBegin(), span, objectCount, objectSize);

    Super::trackMiniHeap(span, mh, sizeClass);

    // mesh::debug("%p (%u) created!\n", mh, GetMiniHeapID(mh));

    _miniheapCount++;
    _stats.mhAllocCount++;
    if (_stats.mhHighWaterMark < _miniheapCount) {
      _stats.mhHighWaterMark = getAllocatedMiniheapCount();
    }

    return mh;
  }

  inline void *pageAlignedAlloc(size_t pageAlignment, size_t pageCount) {
    // if given a very large allocation size (e.g. (uint64_t)-8), it is possible
    // the pageCount calculation overflowed.  An allocation that big is impossible
    // to satisfy anyway, so just fail early.
    if (unlikely(pageCount == 0)) {
      return nullptr;
    }

    MiniHeap *mh = allocMiniheapLocked(kClassSizesMax, pageCount, 1, pageCount * kPageSize, pageAlignment);

    d_assert(mh->isLargeAlloc());
    d_assert(mh->spanSize() == pageCount * kPageSize);
    // d_assert(mh->objectSize() == pageCount * kPageSize);

    void *ptr = mh->mallocAt(arenaBegin(), 0);

    return ptr;
  }

  inline MiniheapFreeList::FreeListEntry *freelistFor(uint8_t freelistId, size_t sizeClass) {
    return _freelist[sizeClass].freelistFor(freelistId);
  }

  inline bool postFreeLocked(MiniHeap *mh, int sizeClass, size_t inUse) {
    // its possible we raced between reading isAttached + grabbing a lock.
    // just check here to avoid having to play whack-a-mole at each call site.
    if (mh->isAttached()) {
      return false;
    }
    d_assert(!mh->isMeshed());
    return _freelist[sizeClass].postFreeLocked(mh, sizeClass, inUse);
  }

  inline void releaseMiniheapLocked(MiniHeap *mh, int sizeClass) {
    // ensure this flag is always set with the miniheap lock held
    mh->unsetAttached();
    if (mh->isPartialFree()) {
      unboundMeshSlowly(mh);
    }
    const auto inUse = mh->inUseCount();
    postFreeLocked(mh, sizeClass, inUse);
  }

  template <uint32_t Size>
  inline void releaseMiniheaps(size_t sizeClass, FixedArray<MiniHeap, Size> &miniheaps) {
    if (miniheaps.size() == 0) {
      return;
    }

    lock_guard<mutex> lock(_freelist[sizeClass].lock);
    for (auto mh : miniheaps) {
      releaseMiniheapLocked(mh, sizeClass);
    }
    miniheaps.clear();
  }

  template <uint32_t Size>
  void fillFromList(FixedArray<MiniHeap, Size> &miniheaps, pid_t current, MiniheapFreeList::FreeListEntry &freelist,
                    uint32_t maxSize) {
    if (freelist.head.empty()) {
      return;
    }

    d_assert(maxSize <= Size);

    auto nextId = freelist.head.next();
    while (nextId != list::Head && miniheaps.size() < maxSize) {
      auto mh = GetMiniHeap(nextId);
      d_assert(mh != nullptr);
      nextId = mh->getFreelist()->next();

      d_assert(!mh->isFull());
      d_assert(!mh->isMeshed());

      d_assert(!mh->isAttached());
      mh->setAttached(current, &freelist.head);
      d_assert(mh->isAttached());
      miniheaps.append(mh);
      d_assert(freelist.size > 0);
      --freelist.size;
    }

    return;
  }

  template <uint32_t Size>
  void selectForReuse(int sizeClass, FixedArray<MiniHeap, Size> &miniheaps, pid_t current) {
    auto &freelist = _freelist[sizeClass];

    if (freelist.partial.size % 2) {
      fillFromList(miniheaps, current, freelist.partialFull, Size);
      if (miniheaps.full()) {
        return;
      }
      fillFromList(miniheaps, current, freelist.partial, Size);
      if (miniheaps.size() >= 2 || miniheaps.full()) {
        return;
      }
    } else {
      fillFromList(miniheaps, current, freelist.partial, Size);
      if (miniheaps.size() >= 2 || miniheaps.full()) {
        return;
      }
      fillFromList(miniheaps, current, freelist.partialFull, Size);
      if (miniheaps.full()) {
        return;
      }
    }

    // we've exhausted all of our partially full MiniHeaps, but there
    // might still be empty ones we could reuse.
    fillFromList(miniheaps, current, freelist.empty, miniheaps.size() + 1);
  }

  bool allocFromCentralCache(int sizeClass, void *&head, void *&tail, uint32_t &size);
  void releaseToCentralCache(int sizeClass, void *head, void *tail, uint32_t size, pid_t current);
  void flushCentralCache();
  size_t flushCentralCache(size_t sizeClass, size_t limit);

  template <uint32_t Size>
  inline void allocSmallMiniheaps(int sizeClass, uint32_t objectSize, FixedArray<MiniHeap, Size> &miniheaps,
                                  pid_t current) {
    lock_guard<mutex> lock(_freelist[sizeClass].lock);

    d_assert(objectSize <= _maxObjectSize);

#ifndef NDEBUG
    const size_t classMaxSize = SizeMap::ByteSizeForClass(sizeClass);

    d_assert_msg(objectSize == classMaxSize, "sz(%zu) shouldn't be greater than %zu (class %d)", objectSize,
                 classMaxSize, sizeClass);
#endif
    d_assert(sizeClass >= 0);
    d_assert(sizeClass < kNumBins);
    d_assert(miniheaps.size() == 0);

    // check our bins for a miniheap to reuse
    selectForReuse(sizeClass, miniheaps, current);
    if (miniheaps.size() >= 2) {
      return;
    }

    // if we have objects bigger than the size of a page, allocate
    // multiple pages to amortize the cost of creating a
    // miniheap/globally locking the heap.  For example, asking for
    // 2048 byte objects would allocate 4 4KB pages.
    const size_t pageCount = static_cast<size_t>(SizeMap::SizeClassToPageCount(sizeClass));
    const size_t objectCount = pageCount * kPageSize / objectSize;

    auto mh = allocMiniheapLocked(sizeClass, pageCount, objectCount, objectSize);
    d_assert(!mh->isAttached());
    mh->setAttached(current, nullptr);
    d_assert(mh->isAttached());
    miniheaps.append(mh);

    return;
  }

  // large, page-multiple allocations
  void *ATTRIBUTE_NEVER_INLINE malloc(size_t sz);

  inline MiniHeap *ATTRIBUTE_ALWAYS_INLINE miniheapForWithEpoch(const void *ptr, size_t &currentEpoch) const {
    currentEpoch = _meshEpoch.current();
    return miniheapFor(ptr);
  }

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

  void untrackMiniheapLocked(MiniHeap *mh) {
    // mesh::debug("%p (%u) untracked!\n", mh, GetMiniHeapID(mh));
    _stats.mhAllocCount -= 1;
    auto currFreelist = freelistFor(mh->freelistId(), mh->sizeClass());
    d_assert(currFreelist && currFreelist->size > 0);
    mh->getFreelist()->remove(&currFreelist->head);
    currFreelist->size--;
  }

  void freeFor(MiniHeap *mh, void *ptr, size_t startEpoch);

  // called with lock held
  void freeMiniheapAfterMeshLocked(MiniHeap *mh, bool untrack = true) {
    // don't untrack a meshed miniheap -- it has already been untracked
    if (untrack && !mh->isMeshed()) {
      untrackMiniheapLocked(mh);
    }

    if (kEnableRecordMiniheapAlive) {
      _stats.alives[mh->sizeClass()].Record(time::now_ticks() - mh->age());
    }

    d_assert(!mh->getFreelist()->prev().hasValue());
    d_assert(!mh->getFreelist()->next().hasValue());
    mh->MiniHeap::~MiniHeap();
    // memset(reinterpret_cast<char *>(mh), 0x77, sizeof(MiniHeap));
    _mhAllocator.free(mh);
    _miniheapCount--;
  }

  void freeMiniheap(MiniHeap *&mh, bool untrack = true) {
    freeMiniheapLocked(mh, untrack);
  }

  void freeMiniheapLocked(MiniHeap *&mh, bool untrack) {
    const auto spanSize = mh->spanSize();
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
      Super::free(reinterpret_cast<void *>(mh->getSpanStart(arenaBegin())), spanSize, type);
      freeMiniheapAfterMeshLocked(mh, untrack);
    }
    _stats.mhFreeCount += last;

    mh = nullptr;
  }

  // flushBinLocked empties _emptyFreelist[sizeClass]
  inline void flushBinLocked(size_t sizeClass) {
    // mesh::debug("flush bin %zu\n", sizeClass);

    auto &empty = _freelist[sizeClass].empty;
    d_assert(!empty.head.empty());
    if (empty.head.next() == list::Head) {
      return;
    }

    MiniHeapID nextId = empty.head.next();
    while (nextId != list::Head) {
      auto mh = GetMiniHeap(nextId);
      nextId = mh->getFreelist()->next();
      freeMiniheapLocked(mh, true);
    }

    d_assert(empty.size == 0);
    d_assert(empty.head.next() == list::Head);
    d_assert(empty.head.prev() == list::Head);
  }

  void ATTRIBUTE_NEVER_INLINE free(void *ptr);

  inline size_t getSize(void *ptr) const {
    if (unlikely(ptr == nullptr))
      return 0;

    auto mh = miniheapFor(ptr);
    if (likely(mh)) {
      return mh->objectSize();
    } else {
      return 0;
    }
  }

  int mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen);

  size_t getAllocatedMiniheapCount() const {
    return _miniheapCount;
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

  inline void ATTRIBUTE_ALWAYS_INLINE maybeMesh() {
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

    meshAllSizeClassesLocked();
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

    auto nextId = _freelist[sizeClass].partial.head.next();
    while (nextId != list::Head) {
      auto mh = GetMiniHeap(nextId);
      if (mh->isMeshingCandidate() && (mh->fullness() < kOccupancyCutoff)) {
        bucket.emplace_back(mh);
      }
      nextId = mh->getFreelist()->next();
    }

    return bucket;
  }

  void dumpMiniHeaps(const char *prefix, size_t sizeClass, const MiniHeapListEntry *miniheaps, int level);
  void dumpList(int level);

private:
  // check for meshes in all size classes -- must be called LOCKED
  void meshAllSizeClassesLocked();
  size_t unboundMeshSlowly(MiniHeap *mh);
  // meshSizeClassLocked returns the number of merged sets found
  size_t meshSizeClassLocked(size_t sizeClass, MergeSetArray &mergeSets, SplitArray &all);

  void processCOWPage();

  const size_t _maxObjectSize;
  atomic_size_t _meshPeriod{kDefaultMeshPeriod};
  std::chrono::milliseconds _meshPeriodMs{kMeshPeriodMs};

  atomic_size_t ATTRIBUTE_ALIGNED(CACHELINE_SIZE) _lastMeshEffective{0};

  // we want this on its own cacheline
  EpochLock ATTRIBUTE_ALIGNED(CACHELINE_SIZE) _meshEpoch{};

  // cachline aligned to avoid sharing cacheline with _meshEpoch
  atomic_size_t ATTRIBUTE_ALIGNED(CACHELINE_SIZE) _miniheapCount{0};

  MiniheapFreeList _freelist[kNumBins];

  mutable mutex _miniheapLock{};

  GlobalHeapStats _stats{};

  // XXX: should be atomic, but has exception spec?
  time::time_point _lastMesh;
  size_t _lastMeshClass{0};

  CentralCache _cache[kNumBins];
  uint64_t _flushCentralCacheDelay{kFlushCentralCacheDelay};

  atomic_size_t _lastFreeClass{0};
};

static_assert(kNumBins == 37, "if this changes, add more 'Head's above");
static_assert(sizeof(std::array<MiniHeapListEntry, kNumBins>) == kNumBins * 8, "list size is right");
static_assert(sizeof(GlobalHeap) < (kNumBins * 8 * 3 + 64 * 7 + 100000000), "gh small enough");
}  // namespace mesh

#endif  // MESH_GLOBAL_HEAP_H
