// -*- mode: c++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright 2019 The Mesh Authors. All rights reserved.
// Use of this source code is governed by the Apache License,
// Version 2.0, that can be found in the LICENSE file.

#pragma once
#ifndef MESH_SHUFFLE_VECTOR_H
#define MESH_SHUFFLE_VECTOR_H

#include <iterator>
#include <random>
#include <utility>

#include "rng/mwc.h"

#include "internal.h"

#include "mini_heap.h"

using mesh::debug;

namespace mesh {

typedef FixedArray<MiniHeap, 4> MiniHeapArray;

class ShuffleCache {
public:
  inline void *ATTRIBUTE_ALWAYS_INLINE malloc() {
    d_assert(!isExhausted());
    return pop();
  }

  inline void ATTRIBUTE_ALWAYS_INLINE push(void *ptr) {
    *reinterpret_cast<void **>(ptr) = _ptr;
    _ptr = ptr;
    ++_length;
  }

  inline void ATTRIBUTE_ALWAYS_INLINE push_list(void *head, void *tail, uint32_t length) {
    *reinterpret_cast<void **>(tail) = _ptr;
    _ptr = head;
    _length += length;
  }

  inline void *ATTRIBUTE_ALWAYS_INLINE pop() {
    void *val = _ptr;
    _ptr = *reinterpret_cast<void **>(_ptr);
    --_length;
    return val;
  }

  inline uint32_t pop_list(uint32_t size, void **head, void **tail) {
    *head = _ptr;
    void *tmp = nullptr;
    uint32_t i = 0;
    while (_ptr && i < size) {
      d_assert(_length > 0);
      ++i;
      --_length;
      tmp = _ptr;
      _ptr = *reinterpret_cast<void **>(_ptr);
    }
    if (tmp) {
      *reinterpret_cast<void **>(tmp) = nullptr;
    }
    *tail = tmp;
    return i;
  }

  inline void ATTRIBUTE_ALWAYS_INLINE free(void *ptr) {
    push(ptr);
  }

  inline bool isFull() const {
    return _length >= _maxCount;
  }

  inline bool isExhausted() const {
    return _ptr == nullptr;
  }

  inline uint32_t length() const {
    return _length;
  }

  inline uint32_t maxCount() const {
    return _maxCount;
  }

  inline void setMaxCount(uint32_t maxCount) {
    _maxCount = maxCount;
  }

private:
  void *_ptr{nullptr};
  uint32_t _length{0};
  uint32_t _maxCount{0};
};

class ShuffleVector {
private:
  DISALLOW_COPY_AND_ASSIGN(ShuffleVector);

public:
  ShuffleVector() : _prng(internal::seed(), internal::seed()) {
    // set initialized = false;
  }

  ~ShuffleVector() {
    // d_assert(_attachedMiniheaps.size() == 0);
  }

  // post: list has the index of all bits set to 1 in it, in a random order
  inline uint32_t ATTRIBUTE_ALWAYS_INLINE refillFrom(uintptr_t start, internal::Bitmap &bitmap) {
    const uint32_t objectCount = _objectCount;
    const uint32_t objectSize = _objectSize;

    internal::RelaxedFixedBitmap newBitmap{objectCount};
    newBitmap.setAll(objectCount);

    internal::RelaxedFixedBitmap localBits{objectCount};
    bitmap.setAndExchangeAll(localBits.mut_bits(), newBitmap.bits());
    localBits.invert();

    uint32_t allocCount = 0;

    for (auto const &i : localBits) {
      // FIXME: this incredibly lurky conditional is because
      // RelaxedFixedBitmap iterates over all 256 bits it has,
      // regardless of the _maxCount set in the constructor -- we
      // should fix that.
      if (i >= objectCount) {
        break;
      }
      _cache->push(reinterpret_cast<void *>(start + i * objectSize));
      ++allocCount;
    }

    return allocCount;
  }

  inline uint32_t maxCount() const {
    return _maxCount;
  }

  inline uint32_t objectCount() const {
    return _objectCount;
  }

  inline bool ATTRIBUTE_ALWAYS_INLINE localRefill(MiniHeapArray &miniheaps) {
    uint32_t addedCapacity = 0;
    const auto miniheapCount = miniheaps.size();
    const auto maxRefill = _cache->maxCount() / 3;
    for (uint32_t i = 0; i < miniheapCount && _cache->length() < maxRefill; i++) {
      auto mh = miniheaps[i];
      if (mh->isFull()) {
        continue;
      }
      const auto allocCount = refillFrom(mh->getSpanStart(_arenaBegin), mh->writableBitmap());
      addedCapacity |= allocCount;
    }

    if (addedCapacity > 0) {
      return true;
    }

    return false;
  }

  // an attach takes ownership of the reference to mh
  inline void reinit(MiniHeapArray &miniheaps) {
    internal::mwcShuffle(miniheaps.array_begin(), miniheaps.array_end(), _prng);
    const bool addedCapacity = localRefill(miniheaps);
    d_assert(addedCapacity);
  }

  inline size_t getSize() {
    return _objectSize;
  }

  // called once, on initialization of ThreadLocalHeap
  inline void initialInit(const char *arenaBegin, size_t sizeClass, ShuffleCache *cache) {
    uint32_t sz = SizeMap::ByteSizeForClass(sizeClass);
    _arenaBegin = arenaBegin;
    _objectSize = sz;
    _objectCount = SizeMap::SizeClassToPageCount(sizeClass) * kPageSize / sz;
    _maxCount = _objectCount * 6;
    _cache = cache;
    _cache->setMaxCount(_maxCount);
    hard_assert(_maxCount > 0);
  }

private:
  MWC _prng;
  ShuffleCache *_cache{nullptr};
  const char *_arenaBegin;
  uint32_t _maxCount{0};
  uint32_t _objectCount{0};
  uint32_t _objectSize{0};
} CACHELINE_ALIGNED;

static_assert(HL::gcd<sizeof(ShuffleVector), CACHELINE_SIZE>::value == CACHELINE_SIZE,
              "ShuffleVector not multiple of cacheline size!");
// FIXME should fit in 640
// static_assert(sizeof(ShuffleVector) == 704, "ShuffleVector not expected size!");
}  // namespace mesh

#endif  // MESH_SHUFFLE_VECTOR_H
