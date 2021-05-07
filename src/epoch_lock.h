// -*- mode: c++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright 2019 The Mesh Authors. All rights reserved.
// Use of this source code is governed by the Apache License,
// Version 2.0, that can be found in the LICENSE file.

#pragma once
#ifndef MESH_EPOCH_LOCK_H
#define MESH_EPOCH_LOCK_H

#include "internal.h"

namespace mesh {

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

}  // namespace mesh

#endif  // MESH_EPOCH_LOCK_H
