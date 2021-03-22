// -*- mode: c++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright 2019 The Mesh Authors. All rights reserved.
// Use of this source code is governed by the Apache License,
// Version 2.0, that can be found in the LICENSE file.

#pragma once
#ifndef MESH_MESHABLE_ARENA_H
#define MESH_MESHABLE_ARENA_H

#if defined(_WIN32)
#error "TODO"
#include <windows.h>
#else
// UNIX
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#if defined(__APPLE__) || defined(__FreeBSD__)
#include <copyfile.h>
#else
#include <sys/sendfile.h>
#endif

#include <new>

#include "internal.h"

#include "cheap_heap.h"

#include "bitmap.h"

#include "mmap_heap.h"

#ifndef MADV_DONTDUMP
#define MADV_DONTDUMP 0
#endif

#ifndef MADV_DODUMP
#define MADV_DODUMP 0
#endif

namespace mesh {

class MeshableArena : public mesh::OneWayMmapHeap {
private:
  DISALLOW_COPY_AND_ASSIGN(MeshableArena);
  typedef OneWayMmapHeap SuperHeap;

public:
  enum { Alignment = kPageSize };

  explicit MeshableArena();

  inline bool contains(const void *ptr) const {
    auto arena = reinterpret_cast<uintptr_t>(_arenaBegin);
    auto end = reinterpret_cast<uintptr_t>(_arenaEnd);
    auto ptrval = reinterpret_cast<uintptr_t>(ptr);
    return arena <= ptrval && ptrval < end;
  }

  char *pageAlloc(Span &result, size_t pageCount, size_t pageAlignment = 1);

  void free(void *ptr, size_t sz, internal::PageType type);

  inline void trackMiniHeap(const Span span, MiniHeap *mh, int sizeClass) {
    // now that we know they are available, set the empty pages to
    // in-use.  This is safe because this whole function is called
    // under the GlobalHeap lock, so there is no chance of concurrent
    // modification between the loop above and the one below.
    //
    size_t index = ((size_t)(mh)) | ((size_t)(sizeClass) << kPtrOffset);
    for (size_t i = 0; i < span.length; i++) {
#ifndef NDEBUG
// auto mh = reinterpret_cast<MiniHeap *>(miniheapForArenaOffset(span.offset + i));
// mh->dumpDebug();
#endif
      setIndex(span.offset + i, index);
    }
  }

  inline void trackMeshedMiniHeap(const Span span, MiniHeap *mh) {
    // make meshed mh sizeClass == 0
    // so ThreadLocalCache won't cache it when free
    size_t index = reinterpret_cast<size_t>(mh);
    for (size_t i = 0; i < span.length; i++) {
      setIndex(span.offset + i, index);
    }
  }

  inline void *ATTRIBUTE_ALWAYS_INLINE miniheapForArenaOffset(Offset arenaOff) const {
    return reinterpret_cast<void *>(_mhIndex[arenaOff].load(std::memory_order_acquire) & kPtrMask);
  }

  inline int ATTRIBUTE_ALWAYS_INLINE sizeClassForArenaOffset(Offset arenaOff) const {
    size_t val = _mhIndex[arenaOff].load(std::memory_order_acquire);
    return static_cast<int>(val >> kPtrOffset);
  }

  inline void *ATTRIBUTE_ALWAYS_INLINE lookupMiniheap(const void *ptr) const {
    const uintptr_t ptrval = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t arena = reinterpret_cast<uintptr_t>(_arenaBegin);
    const uintptr_t end = reinterpret_cast<uintptr_t>(_arenaEnd);

    d_assert(ptrval >= arena);

    if (unlikely(ptrval < arena || ptrval >= end)) {
      return nullptr;
    }

    // we've already checked contains, so we know this offset is
    // within bounds
    const auto arenaOff = (ptrval - arena) / kPageSize;
    return miniheapForArenaOffset(arenaOff);
  }

  inline int ATTRIBUTE_ALWAYS_INLINE lookupSizeClass(const void *ptr) const {
    const uintptr_t ptrval = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t arena = reinterpret_cast<uintptr_t>(_arenaBegin);
    const uintptr_t end = reinterpret_cast<uintptr_t>(_arenaEnd);

    d_assert(ptrval >= arena);

    if (unlikely(ptrval < arena || ptrval >= end)) {
      return kClassSizesMax;
    }

    // we've already checked contains, so we know this offset is
    // within bounds
    const auto arenaOff = (ptrval - arena) / kPageSize;
    return sizeClassForArenaOffset(arenaOff);
  }

  void beginMesh(void *keep, void *remove, size_t sz);
  void finalizeMesh(void *keep, void *remove, size_t sz);

  inline bool aboveMeshThreshold() const {
    return _meshedSpanCount > _maxMeshCount;
  }

  inline void setMaxMeshCount(size_t maxMeshCount) {
    // debug("setting max map count: %zu", maxMeshCount);
    _maxMeshCount = maxMeshCount;
  }

  inline size_t maxMeshCount() const {
    return _maxMeshCount;
  }

  // protected:
  // public for testing
  void scavenge(bool force);
  // like a scavenge, but we only MADV_FREE
  void partialScavenge();

  // return the maximum number of pages we've had meshed (and thus our
  // savings) at any point in time.
  inline size_t meshedPageHighWaterMark() const {
    return _meshedPageCountHWM;
  }

  inline size_t RSSAtHighWaterMark() const {
    return _rssKbAtHWM;
  }

  char *arenaBegin() const {
    return reinterpret_cast<char *>(_arenaBegin);
  }
  void *arenaEnd() const {
    return reinterpret_cast<char *>(_arenaEnd);
  }

  void doAfterForkChild();

  void freePhys(const Span &span);
  void freePhys(void *ptr, size_t sz);

  inline void resetSpanMapping(const Span &span) {
    auto ptr = ptrFromOffset(span.offset);
    auto sz = span.byteLength();
    mmap(ptr, sz, HL_MMAP_PROTECTION_MASK, kMapShared | MAP_FIXED, _fd, span.offset * kPageSize);
  }

  inline void incMeshedSpanCount(size_t pageCount) {
    lock_guard<mutex> lock(_spanLock);
    ++_meshedSpanCount;
    _meshedPageCount += pageCount;
    if (unlikely(_meshedPageCount > _meshedPageCountHWM)) {
      _meshedPageCountHWM = _meshedPageCount;
    }
  }
  inline void decMeshedSpanCount(size_t pageCount) {
    --_meshedSpanCount;
    _meshedPageCount -= pageCount;
  }

  bool isCOWRunning() const {
    return _isCOWRunning;
  }

private:
  void expandArena(size_t minPagesAdded);
  bool findPages(size_t pageCount, Span &result, internal::PageType &type);
  bool findPagesInnerFast(internal::vector<Span> freeSpans[kSpanClassCount], size_t i, size_t pageCount, Span &result);
  bool ATTRIBUTE_NEVER_INLINE findPagesInner(internal::vector<Span> freeSpans[kSpanClassCount], size_t i,
                                             size_t pageCount, Span &result);
  Span reservePages(size_t pageCount, size_t pageAlignment);
  internal::RelaxedBitmap allocatedBitmap(bool includeDirty = true) const;

  void *malloc(size_t sz) = delete;

  inline bool isAligned(const Span &span, const size_t pageAlignment) const {
    return ptrvalFromOffset(span.offset) % (pageAlignment * kPageSize) == 0;
  }

  static constexpr size_t indexSize() {
    // one pointer per page in our arena
    return sizeof(size_t) * (kArenaSize / kPageSize);
  }

  inline void clearIndex(const Span &span) {
    for (size_t i = 0; i < span.length; i++) {
      // clear the miniheap pointers we were tracking
      setIndex(span.offset + i, 0u);
    }
  }

  void moveBiggerTofirst(internal::vector<Span> &spans) {
    if (spans.size() > 1) {
      if (spans[0].length > spans[spans.size() - 1].length) {
        std::swap(spans[0], spans[spans.size() - 1]);
      }
    }
  }

  inline void freeSpan(const Span &span, const internal::PageType flags) {
    if (span.length == 0) {
      return;
    }

    // this happens when we are trying to get an aligned allocation
    // and returning excess back to the arena
    if (flags == internal::PageType::Clean) {
      _clean[span.spanClass()].push_back(span);
      // moveBiggerTofirst(_clean[span.spanClass()]);
      d_assert(_cowBitmap.isSet(span.offset));
      return;
    }

    clearIndex(span);

    if (flags == internal::PageType::Dirty) {
      d_assert(span.length > 0);

      if (_isCOWRunning) {
        trackCOWed(span);
        resetSpanMapping(span);
      }

      _dirty[span.spanClass()].push_back(span);
      // moveBiggerTofirst(_dirty[span.spanClass()]);
      _dirtyPageCount += span.length;

      if (_dirtyPageCount > kMaxDirtyPageThreshold) {
        partialScavenge();
      }
    } else if (flags == internal::PageType::Meshed) {
      // delay restoring the identity mapping
      decMeshedSpanCount(span.length);

      if (_isCOWRunning) {
        trackCOWed(span);
      }

      _toReset.push_back(span);
    }
  }

  int openShmSpanFile(size_t sz);
  int openSpanFile(size_t sz);
  char *openSpanDir(int pid);

  // pointer must already have been checked by `contains()` for bounds
  inline Offset offsetFor(const void *ptr) const {
    const uintptr_t ptrval = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t arena = reinterpret_cast<uintptr_t>(_arenaBegin);

    d_assert(ptrval >= arena);

    return (ptrval - arena) / kPageSize;
  }

  inline uintptr_t ptrvalFromOffset(size_t off) const {
    return reinterpret_cast<uintptr_t>(_arenaBegin) + off * kPageSize;
  }

  inline void *ptrFromOffset(size_t off) const {
    return reinterpret_cast<void *>(ptrvalFromOffset(off));
  }

  inline void setIndex(size_t off, size_t val) {
    d_assert(off < indexSize());
    _mhIndex[off].store(val, std::memory_order_release);
  }

  static void staticAtExit();
  static void staticPrepareForFork();
  static void staticAfterForkParent();
  static void staticAfterForkChild();

  void exit() {
    // FIXME: do this from the destructor, and test that destructor is
    // called.  Also don't leak _spanDir
    if (_spanDir != nullptr) {
      rmdir(_spanDir);
      _spanDir = nullptr;
    }
  }

  void prepareForFork();
  void afterForkParent();
  void afterForkChild();
  void afterForkParentAndChild();

  void *_arenaBegin{nullptr};
  void *_arenaEnd{nullptr};
  // indexed by page offset.
  atomic<size_t> *_mhIndex{nullptr};

protected:
  inline void trackCOWed(const Span &span) {
    for (size_t i = 0; i < span.length; i++) {
      _cowBitmap.tryToSet(span.offset + i);
    }
  }
  void getSpansFromBg(bool wait = false);
  void tryAndSendToFree(internal::FreeCmd *fCommand);
  void tryAndSendToFreeLocked(internal::FreeCmd *fCommand);
  bool moveMiniHeapToNewFile(MiniHeap *mh, void *ptr);
  void moveRemainPages();
  void dumpSpans();
  CheapHeap<64, kArenaSize / kPageSize> _mhAllocator{};
  MWC _fastPrng;
  bool _isCOWRunning{false};
  bool _needCOWScan{true};

  mutable mutex _spanLock{};

private:
  Offset _end{};  // in pages
  Offset _COWend{};
  Offset _lastFlushBegin{};
  Offset _lastCOW{};
  // spans that had been meshed, have been freed, and need to be reset
  // to identity mappings in the page tables.
  internal::vector<Span> _toReset;

  internal::vector<Span> _clean[kSpanClassCount];
  internal::vector<Span> _dirty[kSpanClassCount];

  size_t _dirtyPageCount{0};

  internal::RelaxedBitmap _cowBitmap{
      kArenaSize / kPageSize,
      reinterpret_cast<char *>(OneWayMmapHeap().malloc(bitmap::representationSize(kArenaSize / kPageSize))), false};

  size_t _meshedSpanCount{0};
  size_t _meshedPageCount{0};
  size_t _meshedPageCountHWM{0};
  size_t _rssKbAtHWM{0};
  size_t _maxMeshCount{kDefaultMaxMeshCount};

  int _fd{-1};
  int _prefd{-1};

  char *_spanDir{nullptr};
  char *_preSpanDir{nullptr};
};
}  // namespace mesh

#endif  // MESH_MESHABLE_ARENA_H
