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
    *reinterpret_cast<void **>(ptr) = _head;
    _head = ptr;
    if (_tail == nullptr) {
      _tail = ptr;
    }
    if (unlikely(_length == SizeMap::NumToMoveForClass(_sizeClass))) {
      _anchor = _head;
    }
    ++_length;
  }

  inline void ATTRIBUTE_ALWAYS_INLINE push_list(void *head, void *tail, uint32_t length) {
    *reinterpret_cast<void **>(tail) = _head;
    _head = head;
    _length += length;
    if (!_tail) {
      _tail = tail;
    }
    _anchor = nullptr;
  }

  inline void *ATTRIBUTE_ALWAYS_INLINE pop() {
    void *val = _head;
    _head = *reinterpret_cast<void **>(_head);
    --_length;
    if (unlikely(val == _anchor)) {
      _anchor = nullptr;
      d_assert(_length == SizeMap::NumToMoveForClass(_sizeClass));
    }
    if (_head == nullptr) {
      _tail = nullptr;
    }
    return val;
  }

  uint32_t pop_list(uint32_t size, void *&head, void *&tail) {
    if (_length > size) {
      d_assert(size == SizeMap::NumToMoveForClass(_sizeClass));
      d_assert(_anchor != nullptr);
      d_assert(_length >= size);

      head = *reinterpret_cast<void **>(_anchor);
      *reinterpret_cast<void **>(_anchor) = nullptr;
      tail = _tail;
      _tail = _anchor;
      _anchor = nullptr;
      _length -= size;

      d_assert(_length == lengthSlowly(_head));
      d_assert(size == lengthSlowly(head));
      return size;
    } else {
      d_assert(_length <= size);
      head = _head;
      tail = _tail;
      size = _length;
      _head = _tail = _anchor = nullptr;
      _length = 0;
      return size;
    }
  }

  inline void ATTRIBUTE_ALWAYS_INLINE free(void *ptr) {
    push(ptr);
  }

  inline bool isFull() const {
    return _length >= maxCount();
  }

  inline bool isExhausted() const {
    return _head == nullptr;
  }

  inline uint32_t length() const {
    return _length;
  }

  uint32_t lengthSlowly(void *p) const {
    uint32_t length = 0;
    while (p) {
      ++length;
      p = *reinterpret_cast<void **>(p);
    }
    return length;
  }

  inline uint32_t maxCount() const {
    return SizeMap::MaxCacheForClass(_sizeClass);
  }

  void setSizeClass(uint32_t sizeClass) {
    _sizeClass = sizeClass;
  }

private:
  void *_head{nullptr};
  void *_tail{nullptr};
  void *_anchor{nullptr};
  uint32_t _length{0};
  uint32_t _sizeClass{0};
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
    const bool __attribute__((__unused__)) addedCapacity = localRefill(miniheaps);
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
    _objectCount = SizeMap::ObjectCountForClass(sizeClass);
    _cache = cache;
    _cache->setSizeClass(sizeClass);
  }

private:
  MWC _prng;
  ShuffleCache *_cache{nullptr};
  const char *_arenaBegin;
  uint32_t _objectCount{0};
  uint32_t _objectSize{0};
} CACHELINE_ALIGNED;

static_assert(HL::gcd<sizeof(ShuffleVector), CACHELINE_SIZE>::value == CACHELINE_SIZE,
              "ShuffleVector not multiple of cacheline size!");
// FIXME should fit in 640
// static_assert(sizeof(ShuffleVector) == 704, "ShuffleVector not expected size!");
}  // namespace mesh

#endif  // MESH_SHUFFLE_VECTOR_H
