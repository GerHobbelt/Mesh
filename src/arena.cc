// -*- mode: c++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright 2019 The Mesh Authors. All rights reserved.
// Use of this source code is governed by the Apache License,
// Version 2.0, that can be found in the LICENSE file.

#include <utility>

#include "arena.h"
#include "global_heap.h"

#include "meshing.h"
#include "runtime.h"

namespace mesh {

void Arena::Init(uint32_t id, GlobalHeap *global) {
  _id = id;
  _global = global;
  _arenaBegin = _global->arenaBegin();
  for (int i = 0; i < kNumBins; ++i) {
    _cache[i].init();
  }
}

bool Arena::allocFromCentralCache(int sizeClass, void *&head, uint32_t &size) {
  CentralCache &cache = _cache[sizeClass];
  if (cache.pop(head)) {
    size = SizeMap::NumToMoveForClass(sizeClass);
    return true;
  }
  return false;
}

void Arena::releaseToCentralCache(int sizeClass, void *head, uint32_t size, pid_t current) {
  if (!size) {
    return;
  }
  if (size == SizeMap::NumToMoveForClass(sizeClass)) {
    CentralCache &cache = _cache[sizeClass];
    if (cache.push(head)) {
      return;
    }
    flushCentralCache(sizeClass, 100u);
    if (cache.push(head)) {
      return;
    }
  }
  if (size > 0) {
    freePtrList(head);
  }
}

void Arena::flushCentralCache() {
  size_t totalSize = 0;
  bool shouldMesh = false;
  for (int i = 0; i < 5 && totalSize < 1000; ++i) {
    const auto old = _lastFreeClass.fetch_add(1);
    size_t sizeClass = old % kNumBins;
    totalSize += flushCentralCache(sizeClass, 1000u);
    if (old % 8 == 0) {
      shouldMesh = true;
    }
  }
  if (shouldMesh || totalSize) {
    _global->maybeMesh(id());
  }
}

size_t Arena::flushCentralCache(size_t sizeClass, size_t limit) {
  void *head = nullptr;
  CentralCache &cache = _cache[sizeClass];
  size_t totalSize = 0;
  while (totalSize < limit && cache.pop_timeout(head, _flushCentralCacheDelay)) {
    size_t size = SizeMap::NumToMoveForClass(sizeClass);
    freePtrList(head);
    totalSize += size;
  }
  return totalSize;
}

void Arena::freeFor(MiniHeap *mh, void *ptr, const void *arenaBegin) {
  d_assert(mh->maxCount() > 1);
  auto sizeClass = mh->sizeClass();
  auto &freelist = _freelist[sizeClass];
  size_t startEpoch = freelist.meshEpoch().current();

  const auto off = mh->getOff(arenaBegin, ptr, SizeMap::ObjectSizeReciprocalForClass(sizeClass));

  bool partialFree = false;
  if (mh->isMeshed()) {
    auto leader = mh->meshedLeader();
    d_assert(leader != nullptr);
    if (mh->meshedFree(off)) {
      partialFree = true;
    }
    mh = leader;
  }

  auto freelistId = mh->freelistId();
  // read inUseCount before calling free to avoid stalling after the
  // LOCK CMPXCHG in mh->free
  auto remaining = mh->inUseCount() - 1;
  auto createEpoch = mh->createEpoch();

  // here can't call mh->free(arenaBegin(), ptr), because in consume takeBitmap always clear the bitmap,
  // if clearIfNotFree after takeBitmap
  // it alwasy return false, but in this case, you need to free again.
  auto wasSet = mh->clearIfNotFree(off);

  // bool shouldMesh = false;

  // the epoch will be odd if a mesh was in progress when we looked up
  // the miniheap; if that is true, or a meshing started between then
  // and now we can't be sure the above free was successful
  if (partialFree || startEpoch % 2 == 1 || !freelist.meshEpoch().isSame(startEpoch)) {
    // a mesh was started in between when we looked up our miniheap
    // and now.  synchronize to avoid races
    // lock_guard<mutex> lock(_freelist[sizeClass].lock);
    lock_guard<mutex> lock(freelist.lock());

    if (mh->createEpoch() != createEpoch) {
      return;
    }
    auto origMh = mh;
    mh = mh->meshedLeader();

    if (mh == nullptr) {
      mh = origMh;
    } else {
      if (unlikely(mh != origMh)) {
        hard_assert(!mh->isMeshed());
        if (origMh->meshedFree(off)) {
          partialFree = true;
        }
        if (!wasSet) {
          // we have confirmation that we raced with meshing, so free the pointer
          // on the new miniheap
          d_assert(sizeClass == mh->sizeClass());
          mh->freeOff(off);
        } else {
          // our MiniHeap is unrelated to whatever is here in memory now - get out of here.
          return;
        }
      }
    }
    if (partialFree) {
      _global->unboundMeshSlowly(mh);
    }

    remaining = mh->inUseCount();
    freelistId = mh->freelistId();

    if ((remaining == 0 || freelistId == list::Full ||
         (freelistId == list::PartialFull && remaining < freelist.occupancyCutoff()))) {
      // this may free the miniheap -- we can't safely access it after
      // this point.
      postFreeLocked(mh, sizeClass, remaining);
    }
  } else {
    // the free went through ok; if we _were_ full, or now _are_ empty,
    // make sure to update the littleheaps
    if ((remaining == 0 || freelistId == list::Full ||
         (freelistId == list::PartialFull && remaining < freelist.occupancyCutoff()))) {
      lock_guard<mutex> lock(freelist.lock());

      if (mh->createEpoch() != createEpoch) {
        return;
      }
      if (mh->isMeshed()) {
        d_assert(wasSet);
        return;
      }
      // a lot could have happened between when we read this without
      // the lock held and now; just recalculate it.
      remaining = mh->inUseCount();
      postFreeLocked(mh, sizeClass, remaining);
      return;
    }
  }
}

size_t Arena::freePtrList(void *head) {
  void *tmp{nullptr};
  size_t size = 0;
  while (head) {
    tmp = *reinterpret_cast<void **>(head);
    _global->free(head);
    head = tmp;
    ++size;
  }
  return size;
}

void Arena::allocCentralCacheSlowly(size_t sizeClass, size_t objectCount, internal::RelaxedFixedBitmap &localBits,
                                    uintptr_t &spanStart) {
  {
    lock_guard<mutex> lock(_freelist[sizeClass].lock());
    if (selectForReuse(sizeClass, objectCount, localBits, spanStart)) {
      return;
    }
  }
  // if we have objects bigger than the size of a page, allocate
  // multiple pages to amortize the cost of creating a
  // miniheap/globally locking the heap.  For example, asking for
  // 2048 byte objects would allocate 4 4KB pages.

  const size_t objectSize = SizeMap::ByteSizeForClass(sizeClass);
  const size_t pageCount = static_cast<size_t>(SizeMap::SizeClassToPageCount(sizeClass));

  auto mh = _global->allocMiniheapLocked(sizeClass, id(), pageCount, objectCount, objectSize);
  internal::RelaxedFixedBitmap newBitmap{objectCount};
  newBitmap.setAll(objectCount);
  mh->bitmap().setAndExchangeAll(localBits.mut_bits(), newBitmap.bits());
  spanStart = mh->getSpanStart(_arenaBegin);

  lock_guard<mutex> lock(_freelist[sizeClass].lock());
  postFreeLocked(mh, sizeClass, mh->inUseCount());
}

void Arena::flushBinLocked(size_t sizeClass) {
  auto &empty = _freelist[sizeClass].emptyList();
  d_assert(!empty.head.empty());
  if (empty.head.next() == list::Head) {
    return;
  }

  MiniHeapID nextId = empty.head.next();
  while (nextId != list::Head) {
    auto mh = GetMiniHeap(nextId);
    nextId = mh->getFreelist()->next();
    --empty.size;
    mh->getFreelist()->remove(&empty.head);
    _global->freeMiniheapLocked(mh);
  }

  d_assert(empty.size == 0);
  d_assert(empty.head.next() == list::Head);
  d_assert(empty.head.prev() == list::Head);
}

void Arena::dumpMiniHeaps(const char *prefix, size_t sizeClass, const MiniHeapListEntry *miniheaps, int level) {
  size_t total = 0;
  size_t hasMeshed = 0;
  size_t totalMesh = 0;
  size_t maxMeshes = 0;
  size_t release = 0;
  constexpr size_t kCap = 11;
  size_t fullness[kCap] = {0};

  MiniHeapID mhId = miniheaps->next();
  while (mhId != list::Head) {
    auto mh = GetMiniHeap(mhId);
    mhId = mh->getFreelist()->next();
    ++total;

    ++fullness[mh->inUseCount() * 10 / mh->maxCount()];
    auto meshCount = mh->meshCount();
    if (meshCount > 1) {
      ++hasMeshed;
      totalMesh += meshCount;
      if (meshCount > maxMeshes) {
        maxMeshes = meshCount;
      }
      release += _global->unboundMeshSlowly(mh);
    }
  }
  debug(
      "[%u]%s class:%-2zu, MHTotalCount:%zu, MHHasMeshedCount:%zu(%.2lf%%), MHTotalMeshedCount:%zu, "
      "MaxMeshes:%zu, AvgMeshes:%.2lf, release:%zu",
      id(), prefix, sizeClass, total, hasMeshed, hasMeshed * 100.0 / std::max(total, 1ul), totalMesh, maxMeshes,
      double(totalMesh) / std::max(hasMeshed, 1ul), release);
  for (size_t i = 0; i < kCap; ++i) {
    debug("[%u]%s class:%-2zu, fullness %3zu - %3zu: %6zu (%.1f%%)", id(), prefix, sizeClass, i * 10,
          (i < kCap - 1 ? i * 10 + 9 : i * 10), fullness[i], fullness[i] * 100.0 / total);
  }
}

void Arena::dumpFreeList(int level) {
  debug("Mesh Arena: %2u, refcount:%zu", id(), _refcount.load());
  for (size_t sizeClass = 0; sizeClass < kNumBins; ++sizeClass) {
    const auto pages = SizeMap::SizeClassToPageCount(sizeClass);

    auto &freelist = _freelist[sizeClass];
    {
      lock_guard<mutex> lock(freelist.lock());
      const auto &list = freelist.partialList();
      if (list.size) {
        if (level > 0) {
          dumpMiniHeaps("MeshPartialList ----", sizeClass, &list.head, level);
        }
        debug("[%u]MeshPartialList ++++++ class:%-2zu, length:%-6zu(%.2f(MB))", id(), sizeClass, list.size,
              list.size * pages * 4.0 / 1024.0);
      }
    }
    {
      lock_guard<mutex> lock(freelist.lock());
      const auto &list = freelist.partialFullList();
      if (list.size) {
        if (level > 0) {
          dumpMiniHeaps("MeshParFullList ----", sizeClass, &list.head, level);
        }
        debug("[%u]MeshParfullList ++++++ class:%-2zu, length:%-6zu(%.2f(MB))", id(), sizeClass, list.size,
              list.size * pages * 4.0 / 1024.0);
      }
    }
    {
      lock_guard<mutex> lock(freelist.lock());
      const auto &list = freelist.fullList();
      if (list.size) {
        if (level > 0) {
          dumpMiniHeaps("MeshFullList -------", sizeClass, &list.head, level);
        }
        debug("[%u]MeshFullList +++++++++ class:%-2zu, length:%-6zu(%.2f(MB))", id(), sizeClass, list.size,
              list.size * pages * 4.0 / 1024.0);
      }
    }
    {
      lock_guard<mutex> lock(freelist.lock());
      const auto &list = freelist.emptyList();
      if (list.size) {
        debug("[%u]MeshEmptyList ++++++++ class:%-2zu, length:%-6zu(%.2f(MB))", id(), sizeClass, list.size,
              list.size * pages * 4.0 / 1024.0);
      }
    }
  }
  if (level > 0) {
    for (size_t i = 0; i < kNumBins; ++i) {
      debug("[%u]MeshCentralCache --+++++ class:%-2zu, length: %-3zu (%.2f(MB))", id(), i, _cache[i].size(),
            _cache[i].size() * SizeMap::NumToMoveForClass(i) * SizeMap::ByteSizeForClass(i) / 1024.0 / 1024.0);
    }
  }
}

}  // namespace mesh
