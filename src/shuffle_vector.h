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

class ShuffleCache {
public:
  inline void *ATTRIBUTE_ALWAYS_INLINE malloc() {
    d_assert(!isExhausted());
    return pop();
  }

  inline void ATTRIBUTE_ALWAYS_INLINE push(void *ptr) {
    *reinterpret_cast<void **>(ptr) = _head;
    if (unlikely(_length == numToMove())) {
      _anchor = ptr;
    }
    _head = ptr;
    ++_length;
  }

  inline void ATTRIBUTE_ALWAYS_INLINE push_list(void *head, uint32_t length) {
    d_assert(_head == nullptr);
    d_assert(_anchor == nullptr);
    d_assert(_length == 0);
    _head = head;
    _length = length;
  }

  inline void *ATTRIBUTE_ALWAYS_INLINE pop() {
    void *val = _head;
    --_length;
    _head = *reinterpret_cast<void **>(val);
    if (unlikely(val == _anchor)) {
      _anchor = nullptr;
      d_assert(_length == numToMove());
    }
    return val;
  }

  uint32_t pop_list(uint32_t size, void *&head) {
    if (_length > size) {
      d_assert(size == numToMove());
      d_assert(_anchor != nullptr);

      head = *reinterpret_cast<void **>(_anchor);
      *reinterpret_cast<void **>(_anchor) = nullptr;
      _anchor = nullptr;
      _length -= size;

      d_assert(_length == lengthSlowly(_head));
      d_assert(size == lengthSlowly(head));
      return size;
    } else {
      d_assert(_length <= size);
      head = _head;
      size = _length;
      _head = _anchor = nullptr;
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

  inline uint32_t numToMove() const {
    return _maxCount / 2 + 1;
  }

  inline uint32_t maxCount() const {
    return _maxCount;
  }

  void setMaxCount(uint32_t maxCount) {
    _maxCount = maxCount;
  }

private:
  void *_head{nullptr};
  void *_anchor{nullptr};
  uint32_t _length{0};
  uint32_t _maxCount{0};
};

static constexpr size_t kShuffleVectorCache = 128;
class ShuffleVector {
private:
  DISALLOW_COPY_AND_ASSIGN(ShuffleVector);

public:
  ShuffleVector() : _prng(internal::seed(), internal::seed()) {
    // set initialized = false;
  }

  ~ShuffleVector() {
  }

  // post: list has the index of all bits set to 1 in it, in a random order
  inline uint32_t ATTRIBUTE_ALWAYS_INLINE refillFrom(uintptr_t start, internal::RelaxedFixedBitmap &localBits) {
    const uint32_t objectCount = _objectCount;
    const uint32_t objectSize = _objectSize;

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
      void *p = reinterpret_cast<void *>(start + i * objectSize);
      _cache->push(p);
      ++allocCount;
    }
    return allocCount;
  }

  inline uint32_t objectCount() const {
    return _objectCount;
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
    _cache->setMaxCount(SizeMap::MaxCacheForClass(sizeClass));
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
