// -*- mode: c++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright 2019 The Mesh Authors. All rights reserved.
// Use of this source code is governed by the Apache License,
// Version 2.0, that can be found in the LICENSE file.

#include <utility>

#include "global_heap.h"

#include "meshing.h"
#include "runtime.h"

namespace mesh {

MiniHeap *GetMiniHeap(const MiniHeapID id) {
  hard_assert(id.hasValue() && id != list::Head);

  return runtime().heap().miniheapForID(id);
}

MiniHeapID GetMiniHeapID(const MiniHeap *mh) {
  if (unlikely(mh == nullptr)) {
    d_assert(false);
    return MiniHeapID{0};
  }

  return runtime().heap().miniheapIDFor(mh);
}

void *GlobalHeap::malloc(size_t sz) {
#ifndef NDEBUG
  if (unlikely(sz <= kMaxSize)) {
    abort();
  }
#endif

  const auto pageCount = PageCount(sz);

  return pageAlignedAlloc(1, pageCount);
}

void GlobalHeap::free(void *ptr, uint32_t arenaId, MiniHeap *mh) {
  if (mh->isLargeAlloc()) {
    freeLargeMiniheap(mh);
    return;
  }
  d_assert(arenaId == mh->arenaId());
  d_assert(arenaId < kMaxArena);
  _arena[arenaId].freeFor(mh, ptr, arenaBegin());
}

void GlobalHeap::free(void *ptr) {
  if (unlikely(ptr == nullptr)) {
    return;
  }

  auto mh = miniheapFor(ptr);
  if (unlikely(!mh)) {
#ifndef NDEBUG
    if (ptr != nullptr) {
      debug("FIXME: free of untracked ptr %p", ptr);
    }
#endif
    return;
  }
  if (mh->isLargeAlloc()) {
    freeLargeMiniheap(mh);
    return;
  }
  auto arenaId = mh->arenaId();
  d_assert(arenaId < kMaxArena);
  _arena[arenaId].freeFor(mh, ptr, arenaBegin());
}

int GlobalHeap::mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen) {
  unique_lock<mutex> lock(_miniheapLock);

  if (!oldp || !oldlenp || *oldlenp < sizeof(size_t))
    return -1;

  auto statp = reinterpret_cast<size_t *>(oldp);

  if (strcmp(name, "mesh.check_period") == 0) {
    *statp = _meshPeriod;
    if (!newp || newlen < sizeof(size_t))
      return -1;
    auto newVal = reinterpret_cast<size_t *>(newp);
    _meshPeriod = *newVal;
    // resetNextMeshCheck();
  } else if (strcmp(name, "mesh.scavenge") == 0) {
    lock.unlock();
    scavenge();
    lock.lock();
  } else if (strcmp(name, "mesh.compact") == 0) {
    meshAllSizeClassesLocked(0);
    lock.unlock();
    scavenge();
    lock.lock();
  } else if (strcmp(name, "arena") == 0) {
    // not sure what this should do
  } else if (strcmp(name, "stats.resident") == 0) {
    auto pss = internal::measurePssKiB();
    // mesh::debug("measurePssKiB: %zu KiB", pss);

    *statp = pss * 1024;  // originally in KB
  } else if (strcmp(name, "stats.active") == 0) {
    // all miniheaps at least partially full
    size_t sz = 0;
    // for (size_t i = 0; i < kNumBins; i++) {
    //   const auto count = _littleheaps[i].nonEmptyCount();
    //   if (count == 0)
    //     continue;
    //   sz += count * _littleheaps[i].objectSize() * _littleheaps[i].objectCount();
    // }
    *statp = sz;
  } else if (strcmp(name, "stats.allocated") == 0) {
    // TODO: revisit this
    // same as active for us, for now -- memory not returned to the OS
    size_t sz = 0;
    for (size_t i = 0; i < kNumBins; i++) {
      // const auto &bin = _littleheaps[i];
      // const auto count = bin.nonEmptyCount();
      // if (count == 0)
      //   continue;
      // sz += bin.objectSize() * bin.allocatedObjectCount();
    }
    *statp = sz;
  }
  return 0;
}

void GlobalHeap::meshLocked(MiniHeap *dst, MiniHeap *&src, internal::vector<Span> &fCmdSpans) {
  // mesh::debug("mesh dst:%p <- src:%p\n", dst, src);
  // dst->dumpDebug();
  // src->dumpDebug();
  d_assert(src->freelistId() == list::Partial);
  untrackMiniheapLocked(src);

  const size_t dstSpanSize = dst->spanSize();
  const auto dstSpanStart = reinterpret_cast<void *>(dst->getSpanStart(arenaBegin()));

  src->forEachMeshed([&](const MiniHeap *mh) {
    // marks srcSpans read-only
    const auto srcSpan = reinterpret_cast<void *>(mh->getSpanStart(arenaBegin()));
    Super::beginMesh(dstSpanStart, srcSpan, dstSpanSize);
    return false;
  });

  // does the copying of objects and updating of span metadata
  dst->consume(arenaBegin(), src);

  const MiniHeapID dstID = miniheapIDFor(dst);
  src->forEachMeshed([&](MiniHeap *mh) {
    if (mh == src) {
      mh->setMeshed(dstID);
    } else {
      d_assert(mh->isMeshed());
      mh->setMeshedLeader(dstID);
    }
    const auto srcSpan = reinterpret_cast<void *>(mh->getSpanStart(arenaBegin()));
    // frees physical memory + re-marks srcSpans as read/write
    hard_assert(!isCOWRunning());
    // if (isCOWRunning()) {
    //   Super::trackCOWed(mh->span());
    // }

    Super::finalizeMesh(dstSpanStart, srcSpan, dstSpanSize);
    return false;
  });
  // Super::freePhys(reinterpret_cast<void *>(src->getSpanStart(arenaBegin())), dstSpanSize);
  fCmdSpans.emplace_back(src->span());

  Super::trackMeshedMiniHeap(src->span(), src);

  Super::incMeshedSpanCount(dst->span().length);
  // make sure we adjust what bin the destination is in -- it might
  // now be full and not a candidate for meshing
  postFreeLocked(dst, dst->sizeClass(), dst->inUseCount());
}

size_t GlobalHeap::unboundMeshSlowly(MiniHeap *mh) {
  if (!mh->hasMeshed()) {
    return 0;
  }
  MiniHeap *toFree[kMaxMeshes];
  size_t last = 0;

  MiniHeap *prev = mh;
  auto nextId = mh->nextMeshed();
  while (nextId.hasValue()) {
    mh = GetMiniHeap(nextId);
    nextId = mh->nextMeshed();
    if (mh->isMeshedFull()) {
      prev->setNextMeshed(nextId);
      toFree[last++] = mh;
    } else {
      prev = mh;
    }
  }
  prev->setNextMeshed(nextId);

  for (size_t i = 0; i < last; i++) {
    MiniHeap *mh = toFree[i];
    d_assert(mh->isMeshed());
    mh->setMeshedLeader(MiniHeapID{});
    Super::freeMiniheap(mh, internal::PageType::Meshed);
  }
  return last;
}

Arena *GlobalHeap::allocArena() {
  uint32_t index = 0;
  uint32_t refcount = -1u;
  for (uint32_t i = 0; i < kMaxArena; ++i) {
    if (_arena[i].refcount() < refcount) {
      index = i;
      refcount = _arena[i].refcount();
    }
  }
  return &_arena[index];
}

size_t GlobalHeap::meshSizeClassLocked(uint32_t arenaId, size_t sizeClass, MergeSetArray &mergeSets, SplitArray &all) {
  // debug("mesh class = %d", sizeClass);
  size_t mergeSetCount = 0;
  // memset(reinterpret_cast<void *>(&mergeSets), 0, sizeof(mergeSets));
  // memset(&left, 0, sizeof(left));
  // memset(&right, 0, sizeof(right));

  // more change to reuse, other than meshed
  // if (_partialFreelist[sizeClass].second < _fullList[sizeClass].second / 15) {
  //   return mergeSetCount;
  // }
  //

  auto &arena = _arena[arenaId];

  arena.flushCentralCache(sizeClass, 5000ul);

  auto &freelist = arena.freelist(sizeClass);
  lock_guard<mutex> lock(freelist.lock());

  auto meshFound =
      function<bool(std::pair<MiniHeap *, MiniHeap *> &&)>([&](std::pair<MiniHeap *, MiniHeap *> &&miniheaps) {
        d_assert(miniheaps.first->isMeshingCandidate());
        d_assert(miniheaps.second->isMeshingCandidate());
        mergeSets[mergeSetCount] = std::move(miniheaps);
        mergeSetCount++;
        return mergeSetCount < kMaxMergeSets;
      });

  method::shiftedSplitting(_fastPrng, &freelist.partialList().head, all, meshFound);

  if (mergeSetCount == 0) {
    // debug("nothing to mesh. sizeClass = %d", sizeClass);
    return 0;
  }

  lock_guard<EpochLock> epochLock(freelist.meshEpoch());
  size_t meshCount = 0;

  internal::FreeCmd *fCommand = new internal::FreeCmd(internal::FreeCmd::FREE_PAGE);

  for (size_t i = 0; i < mergeSetCount; i++) {
    std::pair<MiniHeap *, MiniHeap *> &mergeSet = mergeSets[i];
    MiniHeap *dst = mergeSet.first;
    MiniHeap *src = mergeSet.second;
    d_assert(dst != nullptr);
    d_assert(src != nullptr);

    // final check: if one of these miniheaps is now empty
    // (e.g. because a parallel thread is freeing a bunch of objects
    // in a row) save ourselves some work by just tracking this as a
    // regular postFree
    auto oneEmpty = false;
    auto dstUseCount = dst->inUseCount();
    if (dstUseCount == 0) {
      arena.postFreeLocked(dst, sizeClass, 0);
      oneEmpty = true;
    }
    auto srcUseCount = src->inUseCount();
    if (srcUseCount == 0) {
      arena.postFreeLocked(src, sizeClass, 0);
      oneEmpty = true;
    }
    // merge _into_ the one with a larger mesh count, potentially
    // swapping the order of the pair
    auto dstCount = dst->meshCount();
    auto srcCount = src->meshCount();

    if (dstCount + srcCount > kMaxMeshes) {
      continue;
    }

    if (!oneEmpty && !aboveMeshThreshold()) {
      // if (dstCount < srcCount || (dstCount == srcCount && dstUseCount > srcUseCount)) {
      dstUseCount += dstCount;
      srcUseCount += srcCount;
      if (dstUseCount > srcUseCount || (dstUseCount == srcUseCount && dst->age() < src->age())) {
        std::swap(dst, src);
      }
      meshLocked(dst, src, fCommand->spans);
      meshCount++;
    }
  }

  tryAndSendToFree(fCommand);
  // flush things once more (since we may have called postFree instead
  // of mesh above)
  arena.flushBinLocked(sizeClass);
  freelist.calcOccupancyCutoff(sizeClass);
  return meshCount;
}

void GlobalHeap::meshAllSizeClassesLocked(uint32_t arenaId) {
  static MergeSetArray PAGE_ALIGNED MergeSets;
  static_assert(sizeof(MergeSets) == sizeof(void *) * 2 * 4096, "array too big");
  d_assert((reinterpret_cast<uintptr_t>(&MergeSets) & (kPageSize - 1)) == 0);

  static SplitArray PAGE_ALIGNED All;
  static_assert(sizeof(All) == sizeof(void *) * kMaxSplitListSize * 2, "array too big");

  d_assert((reinterpret_cast<uintptr_t>(&All) & (kPageSize - 1)) == 0);

  // if we have freed but not reset meshed mappings, this will reset
  // them to the identity mapping, ensuring we don't blow past our VMA
  // limit (which is why we set the force flag to true)
  if (Super::aboveMeshThreshold()) {
    Super::scavenge();
    return;
  }

  auto &arena = _arena[arenaId];

  size_t lastMeshClass = arena.lastMeshClass();

  // const auto start = time::now();

  // first, clear out any free memory we might have
  ++lastMeshClass;

  if (SizeMap::ByteSizeForClass(lastMeshClass) >= kPageSize) {
    lastMeshClass = 0;
  }

  d_assert(lastMeshClass < kNumBins);

  // flushBin(lastMeshClass);
  size_t totalMeshCount = 0;

  while (SizeMap::ByteSizeForClass(lastMeshClass) < kPageSize) {
    size_t meshCount = 0;
    meshCount += meshSizeClassLocked(arenaId, lastMeshClass, MergeSets, All);
    if (meshCount > 0) {
      totalMeshCount += meshCount;
      break;
    } else {
      ++lastMeshClass;
    }
  }

  arena.setLastMeshClass(lastMeshClass);

  _stats.meshCount += totalMeshCount;

  Super::scavenge();

  _lastMesh = time::now();

  // const std::chrono::duration<double> duration = _lastMesh - start;
  // debug("mesh took %f, found %zu", duration.count(), totalMeshCount);
}

void GlobalHeap::processCOWPage() {
  Super::moveRemainPages();
}

void GlobalHeap::dumpStats(int level, bool beDetailed) const {
  if (level < 1)
    return;

  const auto meshedPage = meshedPageCount();
  const auto meshedPageHWM = meshedPageHighWaterMark();

  debug("MESH COUNT:         %zu\n", (size_t)_stats.meshCount);
  debug("Meshed MB (total):  %.1f\n", (size_t)_stats.meshCount * 4096.0 / 1024.0 / 1024.0);
  debug("Meshed pages:       %zu\n", meshedPage);
  debug("Meshed pages HWM:   %zu\n", meshedPageHWM);
  debug("Meshed MB HWM:      %.1f\n", meshedPageHWM * 4096.0 / 1024.0 / 1024.0);
  if (level > 1) {
    // for (size_t i = 0; i < kNumBins; i++) {
    //   _littleheaps[i].dumpStats(beDetailed);
    // }
  }
}

void GlobalHeap::dumpList(int level) {
  lock_guard<mutex> lock(_miniheapLock);
  for (uint32_t arenaId = 0; arenaId < kMaxArena; ++arenaId) {
    _arena[arenaId].dumpFreeList(level);
  }
  if (level > 0) {
    dumpSpans();
  }
}

namespace method {

void ATTRIBUTE_NEVER_INLINE halfSplit(MWC &prng, MiniHeapListEntry *miniheaps, SplitArray &all, size_t &leftSize,
                                      size_t &rightSize) noexcept {
  d_assert(leftSize == 0);
  d_assert(rightSize == 0);

  size_t allSize = 0;
  MiniHeapID mhId = miniheaps->next();
  while (mhId != list::Head && allSize < kMaxSplitListSize * 2) {
    auto mh = GetMiniHeap(mhId);
    mhId = mh->getFreelist()->next();
    d_assert(mh->isMeshingCandidate());
    all[allSize] = mh;
    ++allSize;
  }

  internal::mwcShuffle(&all[0], &all[allSize], prng);

  rightSize = allSize / 2;
  leftSize = allSize - rightSize;
}

void ATTRIBUTE_NEVER_INLINE
shiftedSplitting(MWC &prng, MiniHeapListEntry *miniheaps, SplitArray &all,
                 const function<bool(std::pair<MiniHeap *, MiniHeap *> &&)> &meshFound) noexcept {
  constexpr size_t t = 64;

  if (miniheaps->empty()) {
    return;
  }

  MiniHeap **left = nullptr;
  MiniHeap **right = nullptr;

  size_t leftSize = 0;
  size_t rightSize = 0;

  halfSplit(prng, miniheaps, all, leftSize, rightSize);

  left = &all[0];
  right = left + leftSize;

  d_assert(left);
  d_assert(right);

  if (leftSize == 0 || rightSize == 0) {
    return;
  }

  constexpr size_t nBytes = 32;
  const size_t limit = rightSize < t ? rightSize : t;
  d_assert(nBytes == (*left)->bitmap().byteCount());

  size_t foundCount = 0;
  for (size_t j = 0; j < leftSize; j++) {
    const size_t idxLeft = j;
    size_t idxRight = j;

    for (size_t i = 0; i < limit; i++, idxRight++) {
      if (unlikely(idxRight >= rightSize)) {
        idxRight %= rightSize;
      }
      auto h1 = *(left + idxLeft);
      auto h2 = *(right + idxRight);

      if (h1 == nullptr || h2 == nullptr)
        continue;

      const auto bitmap1 = h1->bitmap().bits();
      const auto bitmap2 = h2->bitmap().bits();

      const bool areMeshable = mesh::bitmapsMeshable(bitmap1, bitmap2, nBytes);

      if (unlikely(areMeshable)) {
        std::pair<MiniHeap *, MiniHeap *> heaps{h1, h2};
        bool shouldContinue = meshFound(std::move(heaps));
        *(left + idxLeft) = nullptr;
        *(right + idxRight) = nullptr;
        foundCount++;
        if (unlikely(foundCount > kMaxMeshesPerIteration || !shouldContinue)) {
          return;
        }
      }
    }
  }
}

}  // namespace method
}  // namespace mesh
