// -*- mode: c++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright 2019 The Mesh Authors. All rights reserved.
// Use of this source code is governed by the Apache License,
// Version 2.0, that can be found in the LICENSE file.

#include <stdlib.h>
#include <unistd.h>
#include "runtime.h"
#include "thread_local_heap.h"

using namespace mesh;

#if defined(_WIN32) && (defined(_M_IX86) || defined(_M_X64))
#include <intrin.h>
bool _mesh_cpu_has_fsrm = false;

static void mesh_detect_cpu_features(void) {
  // FSRM for fast rep movsb support (AMD Zen3+ (~2020) or Intel Ice Lake+ (~2017))
  int32_t cpu_info[4];
  __cpuid(cpu_info, 7);
  _mesh_cpu_has_fsrm =
      ((cpu_info[3] & (1 << 4)) !=
       0);  // bit 4 of EDX : see <https ://en.wikipedia.org/wiki/CPUID#EAX=7,_ECX=0:_Extended_Features>
}
#else
static void mesh_detect_cpu_features(void) {
  // nothing
}
#endif

static __attribute__((constructor)) void libmesh_init() {
  mesh_detect_cpu_features();
  mesh::real::init();

  runtime().createSignalFd();
  runtime().installSegfaultHandler();
  runtime().initMaxMapCount();

  char *meshPeriodStr = getenv("MESH_PERIOD_MS");
  if (meshPeriodStr) {
    long period = strtol(meshPeriodStr, nullptr, 10);
    if (period < 0) {
      period = 0;
    }
    runtime().setMeshPeriodMs(std::chrono::milliseconds{period});
  }

  char *bgThread = getenv("MESH_BACKGROUND_THREAD");
  if (bgThread) {
    int shouldThread = atoi(bgThread);
    if (shouldThread) {
      runtime().startBgThread();
    }
  }

  char *bgFreePhysThread = getenv("MESH_FREEPHYS_THREAD");
  if (bgFreePhysThread) {
    int shouldFreeThread = atoi(bgFreePhysThread);
    if (shouldFreeThread) {
      runtime().startFreePhysThread();
    }
  }
}

static __attribute__((destructor)) void libmesh_fini() {
  char *mstats = getenv("MALLOCSTATS");
  if (!mstats)
    return;

  int mlevel = atoi(mstats);
  if (mlevel < 0)
    mlevel = 0;
  else if (mlevel > 2)
    mlevel = 2;

  runtime().heap().dumpStats(mlevel, false);
}

namespace mesh {

inline void *touch_addr(void *addr) {
  if (addr) {
    volatile char c = *((char *)addr);
    *((char *)addr) = c;
  }
  return addr;
}

inline void *touch_fast(void *addr) {
  if (addr) {
    *((char *)addr) = 0;
  }
  return addr;
}

ATTRIBUTE_NEVER_INLINE
static void *allocSlowpath(size_t sz) {
  ThreadLocalHeap *localHeap = ThreadLocalHeap::GetHeap();
  return touch_fast(localHeap->malloc(sz));
}

ATTRIBUTE_NEVER_INLINE
static void *cxxNewSlowpath(size_t sz) {
  ThreadLocalHeap *localHeap = ThreadLocalHeap::GetHeap();
  return localHeap->cxxNew(sz);
}

ATTRIBUTE_NEVER_INLINE
static void freeSlowpath(void *ptr) {
  // instead of instantiating a thread-local heap on free, just free
  // to the global heap directly
  runtime().heap().free(ptr);
}

ATTRIBUTE_NEVER_INLINE
static void *reallocSlowpath(void *oldPtr, size_t newSize) {
  ThreadLocalHeap *localHeap = ThreadLocalHeap::GetHeap();
  return localHeap->realloc(oldPtr, newSize);
}

ATTRIBUTE_NEVER_INLINE
static void *callocSlowpath(size_t count, size_t size) {
  ThreadLocalHeap *localHeap = ThreadLocalHeap::GetHeap();
  return localHeap->calloc(count, size);
}

ATTRIBUTE_NEVER_INLINE
static size_t usableSizeSlowpath(void *ptr) {
  ThreadLocalHeap *localHeap = ThreadLocalHeap::GetHeap();
  return localHeap->getSize(ptr);
}

ATTRIBUTE_NEVER_INLINE
static void *memalignSlowpath(size_t alignment, size_t size) {
  ThreadLocalHeap *localHeap = ThreadLocalHeap::GetHeap();
  return touch_fast(localHeap->memalign(alignment, size));
}
}  // namespace mesh

extern "C" MESH_EXPORT CACHELINE_ALIGNED_FN void *mesh_malloc(size_t sz) {
  ThreadLocalHeap *localHeap = ThreadLocalHeap::GetHeapIfPresent();
  if (unlikely(localHeap == nullptr)) {
    return touch_fast(mesh::allocSlowpath(sz));
  }

  return touch_fast(localHeap->malloc(sz));
}
#define xxmalloc mesh_malloc

extern "C" MESH_EXPORT CACHELINE_ALIGNED_FN void mesh_free(void *ptr) {
  ThreadLocalHeap *localHeap = ThreadLocalHeap::GetHeapIfPresent();
  if (unlikely(localHeap == nullptr)) {
    mesh::freeSlowpath(ptr);
    return;
  }

  return localHeap->free(ptr);
}
#define xxfree mesh_free

extern "C" MESH_EXPORT CACHELINE_ALIGNED_FN void mesh_sized_free(void *ptr, size_t sz) {
  ThreadLocalHeap *localHeap = ThreadLocalHeap::GetHeapIfPresent();
  if (unlikely(localHeap == nullptr)) {
    mesh::freeSlowpath(ptr);
    return;
  }

  return localHeap->sizedFree(ptr, sz);
}

extern "C" MESH_EXPORT CACHELINE_ALIGNED_FN void *mesh_realloc(void *oldPtr, size_t newSize) {
  ThreadLocalHeap *localHeap = ThreadLocalHeap::GetHeapIfPresent();
  if (unlikely(localHeap == nullptr)) {
    return mesh::reallocSlowpath(oldPtr, newSize);
  }

  return localHeap->realloc(oldPtr, newSize);
}

extern "C" MESH_EXPORT CACHELINE_ALIGNED_FN size_t mesh_malloc_usable_size(void *ptr) {
  ThreadLocalHeap *localHeap = ThreadLocalHeap::GetHeapIfPresent();
  if (unlikely(localHeap == nullptr)) {
    return mesh::usableSizeSlowpath(ptr);
  }

  return localHeap->getSize(ptr);
}
#define xxmalloc_usable_size mesh_malloc_usable_size

extern "C" MESH_EXPORT CACHELINE_ALIGNED_FN void *mesh_memalign(size_t alignment, size_t size)
#if !defined(__FreeBSD__) && !defined(__SVR4)
    throw()
#endif
{
  ThreadLocalHeap *localHeap = ThreadLocalHeap::GetHeapIfPresent();
  if (unlikely(localHeap == nullptr)) {
    return touch_fast(mesh::memalignSlowpath(alignment, size));
  }

  return touch_fast(localHeap->memalign(alignment, size));
}

extern "C" MESH_EXPORT CACHELINE_ALIGNED_FN void *mesh_calloc(size_t count, size_t size) {
  ThreadLocalHeap *localHeap = ThreadLocalHeap::GetHeapIfPresent();
  if (unlikely(localHeap == nullptr)) {
    return mesh::callocSlowpath(count, size);
  }

  return localHeap->calloc(count, size);
}

extern "C" {
#ifdef __linux__
size_t MESH_EXPORT mesh_usable_size(void *ptr) __attribute__((weak, alias("mesh_malloc_usable_size")));
#else
// aliases are not supported on darwin
size_t MESH_EXPORT mesh_usable_size(void *ptr) {
  return mesh_malloc_usable_size(ptr);
}
#endif  // __linux__

// ensure we don't concurrently allocate/mess with internal heap data
// structures while forking.  This is not normally invoked when
// libmesh is dynamically linked or LD_PRELOADed into a binary.
void MESH_EXPORT xxmalloc_lock(void) {
  mesh::runtime().lock();
}

// ensure we don't concurrently allocate/mess with internal heap data
// structures while forking.  This is not normally invoked when
// libmesh is dynamically linked or LD_PRELOADed into a binary.
void MESH_EXPORT xxmalloc_unlock(void) {
  mesh::runtime().unlock();
}

int MESH_EXPORT sigaction(int signum, const struct sigaction *act, struct sigaction *oldact) MESH_THROW {
  return mesh::runtime().sigaction(signum, act, oldact);
}

int MESH_EXPORT sigprocmask(int how, const sigset_t *set, sigset_t *oldset) MESH_THROW {
  return mesh::runtime().sigprocmask(how, set, oldset);
}

// we need to wrap pthread_create and pthread_exit so that we can
// install our segfault handler and cleanup thread-local heaps.
int MESH_EXPORT pthread_create(pthread_t *thread, const pthread_attr_t *attr, mesh::PthreadFn startRoutine,
                               void *arg) MESH_THROW {
  return mesh::runtime().createThread(thread, attr, startRoutine, arg);
}

void MESH_EXPORT ATTRIBUTE_NORETURN pthread_exit(void *retval) {
  mesh::runtime().exitThread(retval);
}

// Same API as je_mallctl, allows a program to query stats and set
// allocator-related options.
int MESH_EXPORT mesh_mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen) {
  return mesh::runtime().heap().mallctl(name, oldp, oldlenp, newp, newlen);
}

ssize_t MESH_EXPORT read(int fd, void *buf, size_t count) {
  if (unlikely(mesh::real::read == nullptr))
    mesh::real::init();

  touch_addr(buf);
  return mesh::real::read(fd, buf, count);
}

size_t MESH_EXPORT fread(void *ptr, size_t size, size_t nmemb, FILE *stream) {
  if (unlikely(mesh::real::fread == nullptr))
    mesh::real::init();

  // debug("ptr=%p  _IO_buf_base=%p", ptr, stream->_IO_buf_base);

  touch_addr(ptr);
  touch_addr(stream->_IO_buf_base);
  return mesh::real::fread(ptr, size, nmemb, stream);
}

char *getcwd(char *buf, size_t size) {
  if (unlikely(mesh::real::getcwd == nullptr))
    mesh::real::init();

  touch_addr(buf);
  return mesh::real::getcwd(buf, size);
}

ssize_t MESH_EXPORT recv(int sockfd, void *buf, size_t len, int flags) {
  touch_addr(buf);
  return mesh::real::recv(sockfd, buf, len, flags);
}

ssize_t MESH_EXPORT recvmsg(int sockfd, struct msghdr *msg, int flags) {
  for (size_t i = 0; i < msg->msg_iovlen; ++i) {
    auto ptr = msg->msg_iov[i].iov_base;
    if (ptr) {
      touch_addr(ptr);
    }
  }

  return mesh::real::recvmsg(sockfd, msg, flags);
}
}

#if defined(__linux__)
#include "gnu_wrapper.cc"
#elif defined(__APPLE__)
#include "mac_wrapper.cc"
#else
#error "only linux and macOS support for now"
#endif
