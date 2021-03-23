// -*- mode: c++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright 2019 The Mesh Authors. All rights reserved.
// Use of this source code is governed by the Apache License,
// Version 2.0, that can be found in the LICENSE file.

#include "thread_local_heap.h"

namespace mesh {

#ifdef MESH_HAVE_TLS
__thread ThreadLocalHeap *ThreadLocalHeap::_threadLocalHeap ATTR_INITIAL_EXEC CACHELINE_ALIGNED;
#endif
ThreadLocalHeap *ThreadLocalHeap::_threadLocalHeaps{nullptr};
bool ThreadLocalHeap::_tlhInitialized{false};
pthread_key_t ThreadLocalHeap::_heapKey{0};

void ThreadLocalHeap::DestroyThreadLocalHeap(void *ptr) {
  if (ptr != nullptr) {
#ifdef MESH_HAVE_TLS
    _threadLocalHeap = nullptr;
#endif
    DeleteHeap(reinterpret_cast<ThreadLocalHeap *>(ptr));
  }
}

// similar to what TCMalloc does; the constructor initializes our
// pthread key and does this in such a way that we aren't recursively
// calling into pthread code (and deadlocking).
class MeshMallocGuard {
public:
  MeshMallocGuard() {
    ThreadLocalHeap::InitTLH();
  }
};

static MeshMallocGuard module_initialization_hook;

void ThreadLocalHeap::InitTLH() {
  hard_assert(!_tlhInitialized);
  pthread_key_create(&_heapKey, DestroyThreadLocalHeap);
  _tlhInitialized = true;
}

ThreadLocalHeap *ThreadLocalHeap::NewHeap(pthread_t current) {
  // we just allocate out of our internal heap
  void *buf = mesh::internal::Heap().malloc(sizeof(ThreadLocalHeap));
  static_assert(sizeof(ThreadLocalHeap) < 4096 * 32, "tlh should have a reasonable size");
  hard_assert(buf != nullptr);
  hard_assert(reinterpret_cast<uintptr_t>(buf) % CACHELINE_SIZE == 0);

  auto heap = new (buf) ThreadLocalHeap(&mesh::runtime().heap(), current);

  heap->_prev = nullptr;
  heap->_next = _threadLocalHeaps;
  if (_threadLocalHeaps != nullptr) {
    _threadLocalHeaps->_prev = heap;
  }
  _threadLocalHeaps = heap;

  return heap;
}

ThreadLocalHeap *ThreadLocalHeap::CreateHeapIfNecessary() {
#ifdef MESH_HAVE_TLS
  const bool maybeReentrant = !_tlhInitialized;
  // check to see if we really need to create the heap
  if (_tlhInitialized && _threadLocalHeap != nullptr) {
    return _threadLocalHeap;
  }
#else
  const bool maybeReentrant = true;
#endif

  ThreadLocalHeap *heap = nullptr;

  {
    std::lock_guard<GlobalHeap> lock(mesh::runtime().heap());

    const pthread_t current = pthread_self();

    if (maybeReentrant) {
      for (ThreadLocalHeap *h = _threadLocalHeaps; h != nullptr; h = h->_next) {
        if (h->_pthreadCurrent == current) {
          heap = h;
          break;
        }
      }
    }

    if (heap == nullptr) {
      heap = NewHeap(current);
    }
  }

  if (!heap->_inSetSpecific && _tlhInitialized) {
    heap->_inSetSpecific = true;
#ifdef MESH_HAVE_TLS
    _threadLocalHeap = heap;
#endif
    pthread_setspecific(_heapKey, heap);
    heap->_inSetSpecific = false;
  }

  return heap;
}

void ThreadLocalHeap::DeleteHeap(ThreadLocalHeap *heap) {
  if (heap == nullptr) {
    return;
  }

  // manually invoke the destructor
  heap->ThreadLocalHeap::~ThreadLocalHeap();

  {
    std::lock_guard<GlobalHeap> lock(mesh::runtime().heap());
    if (heap->_next != nullptr) {
      heap->_next->_prev = heap->_prev;
    }
    if (heap->_prev != nullptr) {
      heap->_prev->_next = heap->_next;
    }
    if (_threadLocalHeaps == heap) {
      _threadLocalHeaps = heap->_next;
    }
  }

  mesh::internal::Heap().free(reinterpret_cast<void *>(heap));
}

void ThreadLocalHeap::releaseAll() {
  for (size_t i = 1; i < kNumBins; i++) {
    ShuffleCache &shuffleCache = _shuffleCache[i];
    void *head, *tail;
    uint32_t size = shuffleCache.pop_list(shuffleCache.length(), &head, &tail);
    d_assert(shuffleCache.isExhausted());
    d_assert(!shuffleCache.length());
    _global->releaseToCentralCache(i, head, tail, size, _current);
  }
}

void CACHELINE_ALIGNED_FN ThreadLocalHeap::releaseToCentralCache(size_t sizeClass) {
  if (unlikely(_global->maybeMeshing())) {
    return;
  }
  ShuffleCache &shuffleCache = _shuffleCache[sizeClass];
  void *head, *tail;
  uint32_t size = shuffleCache.pop_list(shuffleCache.maxCount() / 3, &head, &tail);
  if (size) {
    _global->releaseToCentralCache(sizeClass, head, tail, size, _current);
    _freeCount = 0;
  }
}

void CACHELINE_ALIGNED_FN ThreadLocalHeap::flushCentralCache() {
  _global->flushCentralCache(0);
  _freeCount = 0;
}

// we get here if the shuffleVector is exhausted
void *CACHELINE_ALIGNED_FN ThreadLocalHeap::smallAllocSlowpath(size_t sizeClass) {
  ShuffleCache &shuffleCache = _shuffleCache[sizeClass];
  void *head, *tail;
  uint32_t size;
  mesh::MiniHeapArray miniheaps{};
  if (_global->allocCache(sizeClass, head, tail, size, miniheaps, _current)) {
    shuffleCache.push_list(head, tail, size);
    return shuffleCache.malloc();
  } else {
    ShuffleVector &shuffleVector = _shuffleVector[sizeClass];
    shuffleCache.setMaxCount(shuffleVector.maxCount());
    shuffleVector.reinit(miniheaps);
    _global->releaseMiniheaps(sizeClass, miniheaps);
    return shuffleCache.malloc();
  }
}

}  // namespace mesh
