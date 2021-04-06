// -*- mode: c++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright 2019 The Mesh Authors. All rights reserved.
// Use of this source code is governed by the Apache License,
// Version 2.0, that can be found in the LICENSE file.

#pragma once
#ifndef MESH_MINI_HEAP_H
#define MESH_MINI_HEAP_H

#include <pthread.h>

#include <atomic>
#include <random>

#include "bitmap.h"
#include "fixed_array.h"
#include "internal.h"

#include "rng/mwc.h"

#include "heaplayers.h"

namespace mesh {

class MiniHeap;

class Flags {
private:
  DISALLOW_COPY_AND_ASSIGN(Flags);

  static inline constexpr uint32_t ATTRIBUTE_ALWAYS_INLINE getSingleBitMask(uint32_t pos) {
    return 1UL << pos;
  }
  static constexpr uint32_t SizeClassShift = 0;
  static constexpr uint32_t FreelistIdShift = 6;
  static constexpr uint32_t MaxCountShift = 9;
  static constexpr uint32_t EpochShift = 18;
  static constexpr uint32_t AttachedOffset = 29;
  static constexpr uint32_t MeshedOffset = 30;
  static constexpr uint32_t PartialFreeOffset = 31;

  inline void ATTRIBUTE_ALWAYS_INLINE setMasked(uint32_t mask, uint32_t newVal) {
    uint32_t oldFlags = _flags.load(std::memory_order_relaxed);
    while (!atomic_compare_exchange_weak_explicit(&_flags,
                                                  &oldFlags,                   // old val
                                                  (oldFlags & mask) | newVal,  // new val
                                                  std::memory_order_release,   // success mem model
                                                  std::memory_order_relaxed)) {
    }
  }

public:
  explicit Flags(uint32_t maxCount, uint32_t sizeClass, uint32_t freelistId) noexcept
      : _flags{(maxCount << MaxCountShift) + (sizeClass << SizeClassShift) + (freelistId << FreelistIdShift) +
               (((_flags >> EpochShift) & 0x1f) << EpochShift)} {
    d_assert((freelistId & 0x7) == freelistId);
    d_assert((sizeClass & ((1 << FreelistIdShift) - 1)) == sizeClass);
    // d_assert(svOffset < 255);
    d_assert_msg(sizeClass < 255, "sizeClass: %u", sizeClass);
    d_assert(maxCount <= 256);
    d_assert(this->maxCount() == maxCount);
  }

  inline uint32_t freelistId() const {
    return (_flags.load(std::memory_order_seq_cst) >> FreelistIdShift) & 0x7;
  }

  inline void setFreelistId(uint32_t freelistId) {
    static_assert(list::Max <= (1 << FreelistIdShift), "expected max < 4");
    d_assert(freelistId < list::Max);
    uint32_t mask = ~(static_cast<uint32_t>(0x7) << FreelistIdShift);
    uint32_t newVal = (static_cast<uint32_t>(freelistId) << FreelistIdShift);
    setMasked(mask, newVal);
  }

  inline uint32_t maxCount() const {
    // XXX: does this assume little endian?
    return (_flags.load(std::memory_order_seq_cst) >> MaxCountShift) & 0x1ff;
  }

  inline uint32_t sizeClass() const {
    return (_flags.load(std::memory_order_seq_cst) >> SizeClassShift) & 0x3f;
  }

  inline uint32_t createEpoch() const {
    return (_flags.load(std::memory_order_seq_cst) >> EpochShift) & 0x1f;
  }

  inline void incCreateEpoch() {
    uint32_t epoch = (createEpoch() + 1) & 0x1f;
    uint32_t mask = ~(static_cast<uint32_t>(0x1f) << EpochShift);
    uint32_t newVal = (static_cast<uint32_t>(epoch) << EpochShift);
    setMasked(mask, newVal);
  }

  inline void setMeshed() {
    set(MeshedOffset);
  }

  inline void unsetMeshed() {
    unset(MeshedOffset);
  }

  inline bool ATTRIBUTE_ALWAYS_INLINE isMeshed() const {
    return is(MeshedOffset);
  }

  inline void setPartialFree() {
    set(PartialFreeOffset);
  }

  inline void unsetPartialFree() {
    unset(PartialFreeOffset);
  }

  inline bool ATTRIBUTE_ALWAYS_INLINE isPartialFree() const {
    return is(PartialFreeOffset);
  }

  inline void setAttached() {
    set(AttachedOffset);
  }

  inline void unsetAttached() {
    unset(AttachedOffset);
  }

  inline bool ATTRIBUTE_ALWAYS_INLINE isAttached() const {
    return is(AttachedOffset);
  }

private:
  inline bool ATTRIBUTE_ALWAYS_INLINE is(size_t offset) const {
    const auto mask = getSingleBitMask(offset);
    return (_flags.load(std::memory_order_acquire) & mask) == mask;
  }

  inline void set(size_t offset) {
    const uint32_t mask = getSingleBitMask(offset);

    uint32_t oldFlags = _flags.load(std::memory_order_relaxed);
    while (!atomic_compare_exchange_weak_explicit(&_flags,
                                                  &oldFlags,                  // old val
                                                  oldFlags | mask,            // new val
                                                  std::memory_order_release,  // success mem model
                                                  std::memory_order_relaxed)) {
    }
  }

  inline void unset(size_t offset) {
    const uint32_t mask = getSingleBitMask(offset);

    uint32_t oldFlags = _flags.load(std::memory_order_relaxed);
    while (!atomic_compare_exchange_weak_explicit(&_flags,
                                                  &oldFlags,                  // old val
                                                  oldFlags & ~mask,           // new val
                                                  std::memory_order_release,  // success mem model
                                                  std::memory_order_relaxed)) {
    }
  }

  std::atomic<uint32_t> _flags;
};

class MiniHeap {
private:
  DISALLOW_COPY_AND_ASSIGN(MiniHeap);

public:
  MiniHeap(void *arenaBegin, Span span, size_t objectCount, size_t objectSize)
      : _bitmap(objectCount),
        _span(span),
        _age(time::now_ticks()),
        _flags(objectCount, objectCount > 1 ? SizeMap::SizeClass(objectSize) : kClassSizesMax, list::Attached),
        _objectSizeReciprocal(1.0 / (float)objectSize) {
    // debug("sizeof(MiniHeap): %zu", sizeof(MiniHeap));

    d_assert(_bitmap.inUseCount() == 0);

    const auto __attribute__((__unused__)) expectedSpanSize = _span.byteLength();
    d_assert_msg(expectedSpanSize == spanSize(), "span size %zu == %zu (%u, %u)", expectedSpanSize, spanSize(),
                 maxCount(), this->objectSize());

    // d_assert_msg(spanSize == static_cast<size_t>(_spanSize), "%zu != %hu", spanSize, _spanSize);
    // d_assert_msg(objectSize == static_cast<size_t>(objectSize()), "%zu != %hu", objectSize, _objectSize);

    d_assert(!_nextMeshed.hasValue());

    // debug("new:\n");
    // dumpDebug();
  }

  ~MiniHeap() {
    _flags.incCreateEpoch();
    // debug("destruct:\n");
    // dumpDebug();
  }

  inline const Span &span() const {
    return _span;
  }

  void printOccupancy() const {
    mesh::debug("{\"name\": \"%p\", \"object-size\": %d, \"length\": %d, \"mesh-count\": %d, \"bitmap\": \"%s\"}\n",
                this, objectSize(), maxCount(), meshCount(), _bitmap.to_string(maxCount()).c_str());
  }

  inline void ATTRIBUTE_ALWAYS_INLINE free(void *arenaBegin, void *ptr) {
    // the logic in globalFree is
    // updated to allow the 'race' between lock-free freeing and
    // meshing
    // d_assert(!isMeshed());
    const ssize_t off = getOff(arenaBegin, ptr);
    if (unlikely(off < 0)) {
      d_assert(false);
      return;
    }

    freeOff(off);
  }

  inline bool meshedFree(const uint8_t off) {
    _bitmap.tryToSet(off);
    return isMeshedFull();
  }

  inline bool clearIfNotFree(const uint8_t off) {
    const auto notWasSet = _bitmap.unset(off);
    const auto wasSet = !notWasSet;
    return wasSet;
  }

  inline void ATTRIBUTE_ALWAYS_INLINE freeOff(size_t off) {
    d_assert_msg(_bitmap.isSet(off), "MiniHeap(%p) expected bit %zu to be set", this, off);
    _bitmap.unset(off);
  }

  /// Copies (for meshing) the contents of src into our span.
  inline void consume(const void *arenaBegin, MiniHeap *src) {
    // this would be bad
    d_assert(src != this);
    d_assert(objectSize() == src->objectSize());

    const auto srcSpan = src->getSpanStart(arenaBegin);
    const auto objectSize = this->objectSize();

    // this both avoids the need to call `freeOff` in the loop
    // below, but it ensures we will be able to check for bitmap
    // setting races in GlobalHeap::freeFor
    auto srcBitmap = src->takeBitmap();

    // for each object in src, copy it to our backing span + update
    // our bitmap and in-use count
    for (auto const &off : srcBitmap) {
      d_assert(off < maxCount());
      d_assert(!_bitmap.isSet(off));

      void *srcObject = reinterpret_cast<void *>(srcSpan + off * objectSize);
      // need to ensure we update the bitmap and in-use count
      void *dstObject = mallocAt(arenaBegin, off);
      // debug("meshing: %zu (%p <- %p)\n", off, dstObject, srcObject);
      d_assert(dstObject != nullptr);
      _mesh_memcpy_aligned(dstObject, srcObject, objectSize);
      // debug("\t'%s'\n", dstObject);
      // debug("\t'%s'\n", srcObject);
    }

    src->bitmap().setBits(srcBitmap.bits());
    src->forEachMeshed([&](const MiniHeap *mh) {
      if (mh != src) {
        src->bitmap().setBits(mh->bitmap().bits());
      }
      return false;
    });

    trackMeshedSpan(GetMiniHeapID(src));
  }

  inline size_t spanSize() const {
    return _span.byteLength();
  }

  inline uint32_t ATTRIBUTE_ALWAYS_INLINE maxCount() const {
    return _flags.maxCount();
  }

  inline bool ATTRIBUTE_ALWAYS_INLINE isLargeAlloc() const {
    return maxCount() == 1;
  }

  inline size_t objectSize() const {
    if (likely(!isLargeAlloc())) {
      // this doesn't handle all the corner cases of roundf(3),
      // but it does work for all of our small object size classes
      return static_cast<size_t>(1 / _objectSizeReciprocal + 0.5);
    } else {
      return _span.length * kPageSize;
    }
  }

  inline int sizeClass() const {
    return _flags.sizeClass();
  }

  inline uint32_t createEpoch() const {
    return _flags.createEpoch();
  }

  inline uintptr_t getSpanStart(const void *arenaBegin) const {
    const auto beginval = reinterpret_cast<uintptr_t>(arenaBegin);
    return beginval + _span.offset * kPageSize;
  }

  inline bool ATTRIBUTE_ALWAYS_INLINE isEmpty() const {
    return _bitmap.inUseCount() == 0;
  }

  inline bool ATTRIBUTE_ALWAYS_INLINE isFull() const {
    return _bitmap.inUseCount() == maxCount();
  }

  inline uint32_t ATTRIBUTE_ALWAYS_INLINE inUseCount() const {
    return _bitmap.inUseCount();
  }

  inline uint32_t ATTRIBUTE_ALWAYS_INLINE inUseCount1() const {
    return _bitmap.inUseCount1();
  }

  inline bool isMeshedFull() const {
    return _bitmap.inUseCount() == _bitmap.bitCount();
  }

  inline size_t bytesFree() const {
    return (maxCount() - inUseCount()) * objectSize();
  }

  inline void setMeshed(MiniHeapID leader) {
    _freelist.setPrev(leader);
    _flags.setMeshed();
  }

  inline void setAttached(pid_t current, MiniHeapListEntry *listHead) {
    // mesh::debug("MiniHeap(%p:%5zu): current <- %u\n", this, objectSize(), current);
    if (listHead != nullptr) {
      _freelist.remove(listHead);
    }
    this->setFreelistId(list::Attached);
    d_assert(!_flags.isAttached());
    _flags.setAttached();
  }

  inline uint8_t freelistId() const {
    return _flags.freelistId();
  }

  inline void setFreelistId(uint8_t id) {
    _flags.setFreelistId(id);
  }

  inline uint32_t age() const {
    return _age;
  }

  inline void unsetAttached() {
    // mesh::debug("MiniHeap(%p:%5zu): current <- UNSET\n", this, objectSize());
    // _current.store(0, std::memory_order::memory_order_release);
    _flags.unsetAttached();
  }

  inline bool isAttached() const {
    return _flags.isAttached();
  }

  inline bool ATTRIBUTE_ALWAYS_INLINE isMeshed() const {
    return _flags.isMeshed();
  }

  inline bool ATTRIBUTE_ALWAYS_INLINE hasMeshed() const {
    return _nextMeshed.hasValue();
  }

  inline void setPartialFree() {
    _flags.setPartialFree();
  }

  inline void unsetPartialFree() {
    _flags.unsetPartialFree();
  }

  inline bool ATTRIBUTE_ALWAYS_INLINE isPartialFree() const {
    return _flags.isPartialFree();
  }

  inline bool isMeshingCandidate() const {
    return !isAttached() && objectSize() < kPageSize;
  }

  /// Returns the fraction full (in the range [0, 1]) that this miniheap is.
  inline double fullness() const {
    return static_cast<double>(inUseCount()) / static_cast<double>(maxCount());
  }

  internal::RelaxedFixedBitmap takeBitmap() {
    const auto capacity = this->maxCount();
    internal::RelaxedFixedBitmap zero{capacity};
    internal::RelaxedFixedBitmap result{capacity};
    _bitmap.setAndExchangeAll(result.mut_bits(), zero.bits());
    return result;
  }

  internal::Bitmap &bitmap() {
    return _bitmap;
  }

  const internal::Bitmap &bitmap() const {
    return _bitmap;
  }

  internal::Bitmap &writableBitmap() {
    return _bitmap;
  }

  void trackMeshedSpan(MiniHeapID id) {
    hard_assert(id.hasValue());

    if (!_nextMeshed.hasValue()) {
      _nextMeshed = id;
    } else {
      GetMiniHeap(_nextMeshed)->trackMeshedSpan(id);
    }
  }

public:
  inline const MiniHeapID nextMeshed() const {
    return _nextMeshed;
  }

  inline void setNextMeshed(MiniHeapID id) {
    _nextMeshed = id;
  }

  inline void setMeshedLeader(MiniHeapID leader) {
    _freelist.setPrev(leader);
  }

  inline MiniHeap *meshedLeader() {
    if (isMeshed()) {
      return GetMiniHeap(_freelist.prev());
    }
    return nullptr;
  }

  template <class Callback>
  inline void forEachMeshed(Callback cb) const {
    if (cb(this))
      return;

    if (_nextMeshed.hasValue()) {
      const auto mh = GetMiniHeap(_nextMeshed);
      mh->forEachMeshed(cb);
    }
  }

  template <class Callback>
  inline void forEachMeshed(Callback cb) {
    if (cb(this))
      return;

    if (_nextMeshed.hasValue()) {
      auto mh = GetMiniHeap(_nextMeshed);
      mh->forEachMeshed(cb);
    }
  }

  bool isRelated(MiniHeap *other) const {
    auto otherFound = false;
    this->forEachMeshed([&](const MiniHeap *eachMh) {
      const auto found = eachMh == other;
      otherFound = found;
      return found;
    });
    return otherFound;
  }

  size_t meshCount() const {
    size_t count = 0;

    const MiniHeap *mh = this;
    while (mh != nullptr) {
      count++;

      auto next = mh->_nextMeshed;
      mh = next.hasValue() ? GetMiniHeap(next) : nullptr;
    }

    return count;
  }

  MiniHeapListEntry *getFreelist() {
    return &_freelist;
  }

  /// public for meshTest only
  inline void *mallocAt(const void *arenaBegin, size_t off) {
    if (!_bitmap.tryToSet(off)) {
      mesh::debug("%p: MA %u", this, off);
      dumpDebug();
      return nullptr;
    }

    return ptrFromOffset(arenaBegin, off);
  }

  inline void *ptrFromOffset(const void *arenaBegin, size_t off) {
    return reinterpret_cast<void *>(getSpanStart(arenaBegin) + off * objectSize());
  }

  inline bool operator<(MiniHeap *&rhs) noexcept {
    return this->inUseCount() < rhs->inUseCount();
  }

  void dumpDebug() const {
    const auto heapPages = spanSize() / HL::CPUInfo::PageSize;
    const size_t inUseCount = this->inUseCount();
    const size_t meshCount = this->meshCount();
    mesh::debug(
        "MiniHeap(%p:%5zu): %3zu objects on %2zu pages (inUse: %zu, spans: %zu)\t%p-%p\tFreelist{prev:%u, next:%u}\n",
        this, objectSize(), maxCount(), heapPages, inUseCount, meshCount, _span.offset * kPageSize,
        _span.offset * kPageSize + spanSize(), _freelist.prev(), _freelist.next());
    mesh::debug("\t%s\n", _bitmap.to_string(maxCount()).c_str());
  }

  // this only works for unmeshed miniheaps
  inline uint8_t ATTRIBUTE_ALWAYS_INLINE getOff(const void *arenaBegin, void *ptr) const {
    const auto ptrval = reinterpret_cast<uintptr_t>(ptr);

    uintptr_t span = reinterpret_cast<uintptr_t>(arenaBegin) + _span.offset * kPageSize;
    d_assert(span != 0);

    const size_t off = (ptrval - span) * _objectSizeReciprocal;
    d_assert(off < maxCount());

    return off;
  }

protected:
  internal::Bitmap _bitmap;       // 32 bytes 32
  const Span _span;               // 8        40
  MiniHeapListEntry _freelist{};  // 8        48
  // atomic<pid_t> _current{0};       // 4        52
  uint32_t _age{0};                   // 4        52
  Flags _flags;                       // 4        56
  const float _objectSizeReciprocal;  // 4        60
  MiniHeapID _nextMeshed{};           // 4        64
};

typedef FixedArray<MiniHeap, 63> MiniHeapArray2;

static_assert(sizeof(pid_t) == 4, "pid_t not 32-bits!");
static_assert(sizeof(mesh::internal::Bitmap) == 32, "Bitmap too big!");
static_assert(sizeof(MiniHeap) == 64, "MiniHeap too big!");
static_assert(sizeof(MiniHeapArray2) == 64 * sizeof(void *), "MiniHeapArray too big!");
}  // namespace mesh

#endif  // MESH_MINI_HEAP_H
