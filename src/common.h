// -*- mode: c++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright 2019 The Mesh Authors. All rights reserved.
// Use of this source code is governed by the Apache License,
// Version 2.0, that can be found in the LICENSE file.

#pragma once
#ifndef MESH_COMMON_H
#define MESH_COMMON_H

#include <cstddef>
#include <cstdint>
#include <ctime>

#include <fcntl.h>

#if !defined(_WIN32)
#ifdef __APPLE__
#define _DARWIN_C_SOURCE  // exposes MAP_ANONYMOUS and MAP_NORESERVE
#endif
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#if defined(_WIN32) && (defined(_M_IX86) || defined(_M_X64))
#include <intrin.h>
#endif

#include <string.h>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <random>
#include <unordered_map>
#include <vector>

// #include "config.h"

#include "static/log.h"

// from Heap Layers
#include "utility/ilog2.h"

#ifdef __linux__
#define MESH_THROW throw()
#else
#define MESH_THROW
#endif

// MESH_HAVE_TLS is defined to 1 when __thread should be supported.
// We assume __thread is supported on Linux when compiled with Clang or compiled
// against libstdc++ with _GLIBCXX_HAVE_TLS defined. (this check is from Abseil)
#ifdef MESH_HAVE_TLS
#error MESH_HAVE_TLS cannot be directly set
#endif
#if defined(__linux__) && (defined(__clang__) || defined(_GLIBCXX_HAVE_TLS))
#define MESH_HAVE_TLS 1
#else
#undef MESH_HAVE_TLS
#endif

namespace mesh {

static constexpr bool kMeshingEnabled = MESHING_ENABLED == 1;

#if defined(_WIN32)
// FIXME(EDB)
static constexpr int kMapShared = 1;
#else
static constexpr int kMapShared = kMeshingEnabled ? MAP_SHARED : MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
#endif

static constexpr size_t kPtrOffset = 48;
static constexpr size_t kPtrMask = (static_cast<size_t>(-1)) >> (64 - kPtrOffset);

static constexpr size_t kMinObjectSize = 16;
static constexpr size_t kMaxSize = 16384;
static constexpr size_t kClassSizesMax = 37;
static constexpr size_t kAlignment = 8;
static constexpr int kMinAlign = 16;
static constexpr uint64_t kPageSize = 4096;
static constexpr uint64_t kPageMask = kPageSize - 1;
static constexpr uint64_t kHugePageSize = 4096 * 512;
static constexpr size_t kMaxFastLargeSize = 256 * 1024;  // 256Kb

static constexpr size_t kMaxSplitListSize = 16384;
static constexpr size_t kMaxMergeSets = 4096;

static constexpr size_t kMaxCOWPage = 1000;
// cutoff to be considered for meshing
static constexpr double kOccupancyCutoff = .75;

// if we have, e.g. a kernel-imposed max_map_count of 2^16 (65k) we
// can only safely have about 30k meshes before we are at risk of
// hitting the max_map_count limit. Must smaller than 1/3, because
// when meshing, in the worst case, 1 map would become 3. (A high
// load server can easily trigger this worst case)
static constexpr double kMeshesPerMap = .33;

static constexpr size_t kDefaultMaxMeshCount = 30000;
static constexpr size_t kMaxMeshesPerIteration = 2500;

// maximum number of dirty pages to hold onto before we flush them
// back to the OS (via MeshableArena::scavenge()
static constexpr size_t kMaxDirtyPageThreshold = 1 << 15;  // 128 MB in pages
static constexpr size_t kMinDirtyPageThreshold = 1 << 13;  // 32  MB in pages

static constexpr uint32_t kSpanClassCount = 256;

static constexpr int kNumBins = 37;  // 16Kb max object size
static constexpr int kDefaultMeshPeriod = 10000;

static constexpr size_t kMinArenaExpansion = 4096;  // 16 MB in pages

// ensures we amortize the cost of going to the global heap enough
static constexpr uint64_t kMinStringLen = 8;
static constexpr size_t kMiniheapRefillGoalSize = 16 * 1024;
static constexpr size_t kMaxMiniheapsPerShuffleVector = 24;

// shuffle vector features
static constexpr int16_t kMaxShuffleVectorLength = 256;  // sizeof(uint8_t) << 8
static constexpr bool kEnableShuffleOnInit = SHUFFLE_ON_INIT == 1;
static constexpr bool kEnableShuffleOnFree = SHUFFLE_ON_FREE == 1;
static constexpr bool kEnableRecordMiniheapAlive = false;

static constexpr uint64_t kFlushCentralCacheDelay = 60 * 1000;
static constexpr size_t kMinCentralCacheLength = 8;
static constexpr size_t kMaxCentralCacheLength = 1024;

// madvise(DONTDUMP) the heap to make reasonable coredumps
static constexpr bool kAdviseDump = true;

// madvise(MADV_HUGEPAGE)
static constexpr bool kAdviseHugePage = true;

static constexpr std::chrono::milliseconds kZeroMs{0};
static constexpr std::chrono::milliseconds kMeshPeriodMs{100};  // 100 ms

// controls aspects of miniheaps
static constexpr size_t kMaxMeshes = 256;  // 1 per bit
#ifdef __APPLE__
static constexpr size_t kArenaSize = 32ULL * 1024ULL * 1024ULL * 1024ULL;  // 32 GB
#else
static constexpr size_t kArenaSize = 64ULL * 1024ULL * 1024ULL * 1024ULL;  // 24 GB
#endif
static constexpr size_t kAltStackSize = 16 * 1024UL;  // 16k sigaltstacks
#define SIGQUIESCE (SIGRTMIN + 7)
#define SIGDUMP (SIGRTMIN + 8)

static constexpr size_t kForkCopyFileSize = 32 * 1024 * 1024ul;

// BinnedTracker
static constexpr size_t kBinnedTrackerBinCount = 1;
static constexpr size_t kBinnedTrackerMaxEmpty = 64;

static inline constexpr size_t PageCount(size_t sz) {
  return (sz + (kPageSize - 1)) / kPageSize;
}

static inline constexpr size_t RoundUpToPage(size_t sz) {
  return kPageSize * PageCount(sz);
}

namespace powerOfTwo {
static constexpr size_t kMinObjectSize = 8;

inline constexpr size_t ByteSizeForClass(const int i) {
  return static_cast<size_t>(1ULL << (i + staticlog(kMinObjectSize)));
}

inline constexpr int ClassForByteSize(const size_t sz) {
  return static_cast<int>(HL::ilog2((sz < 8) ? 8 : sz) - staticlog(kMinObjectSize));
}
}  // namespace powerOfTwo

}  // namespace mesh

using std::condition_variable;
using std::function;
using std::lock_guard;
using std::mt19937_64;
using std::mutex;
// using std::shared_lock;
// using std::shared_mutex;
using std::unique_lock;

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#ifdef __GNUC__
#define GNUC_PREREQ(x, y) (__GNUC__ > x || (__GNUC__ == x && __GNUC_MINOR__ >= y))
#else
#define GNUC_PREREQ(x, y) 0
#endif

#ifdef __clang__
#define CLANG_PREREQ(x, y) (__clang_major__ > x || (__clang_major__ == x && __clang_minor__ >= y))
#else
#define CLANG_PREREQ(x, y) 0
#endif

#if GNUC_PREREQ(4, 2) || CLANG_PREREQ(3, 0)
#define HAVE_ASM_POPCNT
#endif

#if defined(HAVE_ASM_POPCNT) && defined(__x86_64__)

static inline uint64_t popcnt64(uint64_t x) {
  __asm__("popcnt %1, %0" : "=r"(x) : "0"(x));
  return x;
}
#define popcount64(x) popcnt64(x)
#else
#define popcount64(x) __builtin_popcountl(x)
#endif

#define ATTRIBUTE_UNUSED __attribute__((unused))
#define ATTRIBUTE_NEVER_INLINE __attribute__((noinline))
#define ATTRIBUTE_ALWAYS_INLINE __attribute__((always_inline))
#define ATTRIBUTE_NORETURN __attribute__((noreturn))
#define ATTRIBUTE_ALIGNED(s) __attribute__((aligned(s)))
#define CACHELINE_SIZE 64
#define CACHELINE_ALIGNED ATTRIBUTE_ALIGNED(CACHELINE_SIZE)
#define CACHELINE_ALIGNED_FN CACHELINE_ALIGNED
#define PAGE_ALIGNED ATTRIBUTE_ALIGNED(kPageSize)

#define MESH_EXPORT __attribute__((visibility("default")))

#define ATTR_INITIAL_EXEC __attribute__((tls_model("initial-exec")))

#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName &);              \
  void operator=(const TypeName &)

// runtime debug-level asserts
#ifndef NDEBUG
#define d_assert_msg(expr, fmt, ...) \
  ((likely(expr))                    \
       ? static_cast<void>(0)        \
       : mesh::internal::__mesh_assert_fail(#expr, __FILE__, __PRETTY_FUNCTION__, __LINE__, fmt, __VA_ARGS__))

#define d_assert(expr)                   \
  ((likely(expr)) ? static_cast<void>(0) \
                  : mesh::internal::__mesh_assert_fail(#expr, __FILE__, __PRETTY_FUNCTION__, __LINE__, ""))
#else
#define d_assert_msg(expr, fmt, ...)
#define d_assert(expr)
#endif

// like d_assert, but still executed in release builds
#define hard_assert_msg(expr, fmt, ...) \
  ((likely(expr))                       \
       ? static_cast<void>(0)           \
       : mesh::internal::__mesh_assert_fail(#expr, __FILE__, __PRETTY_FUNCTION__, __LINE__, fmt, __VA_ARGS__))
#define hard_assert(expr)                \
  ((likely(expr)) ? static_cast<void>(0) \
                  : mesh::internal::__mesh_assert_fail(#expr, __FILE__, __PRETTY_FUNCTION__, __LINE__, ""))

ATTRIBUTE_ALWAYS_INLINE inline void cpupause() {
#if defined(_MSC_VER)
#if defined(_M_AMD64) || defined(_M_IX86)
  _mm_pause();
#elif defined(_M_ARM64) || defined(_M_ARM)
  __yield();
#endif
#elif defined(__GNUC__)
#if defined(__i386__) || defined(__x86_64__)
  __asm__ __volatile__("pause;" : : : "memory");
#elif (defined(__ARM_ARCH) && __ARM_ARCH >= 8) || defined(__ARM_ARCH_8A__) || defined(__aarch64__)
  __asm__ __volatile__("yield;" : : : "memory");
#endif
#endif
}

namespace mesh {

// logging
void debug(const char *fmt, ...);

namespace internal {
// assertions that don't attempt to recursively malloc
void __attribute__((noreturn))
__mesh_assert_fail(const char *assertion, const char *file, const char *func, int line, const char *fmt, ...);

inline static mutex *getSeedMutex() {
  static char muBuf[sizeof(mutex)];
  static mutex *mu = new (muBuf) mutex();
  return mu;
}

// we must re-initialize our seed on program startup and after fork.
// Must be called with getSeedMutex() held
inline mt19937_64 *initSeed() {
  static char mtBuf[sizeof(mt19937_64)];

  static_assert(sizeof(mt19937_64::result_type) == sizeof(uint64_t), "expected 64-bit result_type for PRNG");

  // seed this Mersenne Twister PRNG with entropy from the host OS
  int fd = open("/dev/urandom", O_RDONLY);
  unsigned long buf;
  auto sz = pread(fd, (void *)&buf, sizeof(unsigned long), 0);
  hard_assert(sz == sizeof(unsigned long));
  close(fd);
  //  std::random_device rd;
  // return new (mtBuf) std::mt19937_64(rd());
  return new (mtBuf) std::mt19937_64(buf);
}

// cryptographically-strong thread-safe PRNG seed
inline uint64_t seed() {
  static mt19937_64 *mt = NULL;
  static mutex *m = getSeedMutex();

  lock_guard<mutex> lock(*m);

  if (unlikely(mt == nullptr))
    mt = initSeed();

  return (*mt)();
}
}  // namespace internal

#define PREDICT_TRUE likely

// from tcmalloc/gperftools
class SizeMap {
private:
  //-------------------------------------------------------------------
  // Mapping from size to size_class and vice versa
  //-------------------------------------------------------------------

  // Sizes <= 1024 have an alignment >= 8.  So for such sizes we have an
  // array indexed by ceil(size/8).  Sizes > 1024 have an alignment >= 128.
  // So for these larger sizes we have an array indexed by ceil(size/128).
  //
  // We flatten both logical arrays into one physical array and use
  // arithmetic to compute an appropriate index.  The constants used by
  // ClassIndex() were selected to make the flattening work.
  //
  // Examples:
  //   Size       Expression                      Index
  //   -------------------------------------------------------
  //   0          (0 + 7) / 8                     0
  //   1          (1 + 7) / 8                     1
  //   ...
  //   1024       (1024 + 7) / 8                  128
  //   1025       (1025 + 127 + (120<<7)) / 128   129
  //   ...
  //   32768      (32768 + 127 + (120<<7)) / 128  376
  static const int kMaxSmallSize = 1024;
  static const size_t kClassArraySize = ((kMaxSize + 127 + (120 << 7)) >> 7) + 1;
  static const unsigned char class_array_[kClassArraySize];

  static inline size_t SmallSizeClass(size_t s) {
    return (static_cast<uint32_t>(s) + 7) >> 3;
  }

  static inline size_t LargeSizeClass(size_t s) {
    return (static_cast<uint32_t>(s) + 127 + (120 << 7)) >> 7;
  }

  // If size is no more than kMaxSize, compute index of the
  // class_array[] entry for it, putting the class index in output
  // parameter idx and returning true. Otherwise return false.
  static inline bool ATTRIBUTE_ALWAYS_INLINE ClassIndexMaybe(size_t s, uint32_t *idx) {
    if (PREDICT_TRUE(s <= kMaxSmallSize)) {
      *idx = (static_cast<uint32_t>(s) + 7) >> 3;
      return true;
    } else if (s <= kMaxSize) {
      *idx = (static_cast<uint32_t>(s) + 127 + (120 << 7)) >> 7;
      return true;
    }
    return false;
  }

  // Compute index of the class_array[] entry for a given size
  static inline size_t ClassIndex(size_t s) {
    // Use unsigned arithmetic to avoid unnecessary sign extensions.
    d_assert(s <= kMaxSize);
    if (PREDICT_TRUE(s <= kMaxSmallSize)) {
      return SmallSizeClass(s);
    } else {
      return LargeSizeClass(s);
    }
  }

  // Mapping from size class to max size storable in that class
  static const uint32_t class_to_size_[kClassSizesMax];
  static const uint32_t class_to_page_[kClassSizesMax];

  static uint32_t class_max_cache_[kClassSizesMax];
  static uint32_t class_num_to_move_[kClassSizesMax];
  static uint32_t class_occupancy_cutoff_[kClassSizesMax];  // objectCount * kOccupancyCutoff

public:
  static constexpr size_t num_size_classes = 25;
  static uint64_t progress_start_time;

  // Constructor should do nothing since we rely on explicit Init()
  // call, which may or may not be called before the constructor runs.
  SizeMap() {
  }

  static inline int SizeClass(size_t size) {
    return class_array_[ClassIndex(size)];
  }

  static inline uint32_t SizeClassToPageCount(size_t sizeClass) {
    return class_to_page_[sizeClass];
  }

  // Check if size is small enough to be representable by a size
  // class, and if it is, put matching size class into *cl. Returns
  // true iff matching size class was found.
  static inline bool ATTRIBUTE_ALWAYS_INLINE GetSizeClass(size_t size, uint32_t *cl) {
    uint32_t idx;
    if (!ClassIndexMaybe(size, &idx)) {
      return false;
    }
    *cl = class_array_[idx];
    return true;
  }

  // Get the byte-size for a specified class
  // static inline int32_t ATTRIBUTE_ALWAYS_INLINE ByteSizeForClass(uint32_t cl) {
  static inline uint32_t ATTRIBUTE_ALWAYS_INLINE ByteSizeForClass(uint32_t cl) {
    return class_to_size_[static_cast<uint32_t>(cl)];
  }

  static inline uint32_t ObjectCountForClass(uint32_t cl) {
    return SizeClassToPageCount(cl) * kPageSize / ByteSizeForClass(cl);
  }

  static inline uint32_t OccupancyCutoffForClass(uint32_t cl) {
    return class_occupancy_cutoff_[cl];
  }

  static void SetOccupancyCutoff(uint32_t cl, size_t partialSize);

  static inline uint32_t NumToMoveForClass(uint32_t cl) {
    return class_num_to_move_[cl];
  }

  static inline uint32_t MaxCacheForClass(uint32_t cl) {
    return class_max_cache_[cl];
  }

  // Mapping from size class to max size storable in that class
  static inline uint32_t class_to_size(uint32_t cl) {
    return class_to_size_[cl];
  }

  static void Init();
};

namespace time {
using clock = std::chrono::high_resolution_clock;
using time_point = std::chrono::time_point<clock>;

inline time_point ATTRIBUTE_ALWAYS_INLINE now() {
#ifdef __linux__
  using namespace std::chrono;
  struct timespec tp;
  clock_gettime(CLOCK_MONOTONIC_COARSE, &tp);
  return time_point(seconds(tp.tv_sec) + nanoseconds(tp.tv_nsec));
#else
  return std::chrono::high_resolution_clock::now();
#endif
}

inline uint64_t ATTRIBUTE_ALWAYS_INLINE now_milliseconds() {
#ifdef __linux__
  struct timespec tp;
  clock_gettime(CLOCK_MONOTONIC_COARSE, &tp);
  return static_cast<uint64_t>(tp.tv_sec) * 1000ul + tp.tv_nsec / 1000000.0;
#else
  using namespace std::chrono;
  return duration_cast<milliseconds>(now().time_since_epoch()).count();
  ;
#endif
}

inline uint32_t ATTRIBUTE_ALWAYS_INLINE now_ticks() {
  return static_cast<uint32_t>((now_milliseconds() - SizeMap::progress_start_time) / 10.0);
}
}  // namespace time

}  // namespace mesh

// ------------------------------------------------------
// Size of a pointer.
// We assume that `sizeof(void*)==sizeof(intptr_t)`
// and it holds for all platforms we know of.
//
// However, the C standard only requires that:
//  p == (void*)((intptr_t)p))
// but we also need:
//  i == (intptr_t)((void*)i)
// or otherwise one might define an intptr_t type that is larger than a pointer...
// ------------------------------------------------------

#if INTPTR_MAX == 9223372036854775807LL
#define MESH_INTPTR_SHIFT (3)
#elif INTPTR_MAX == 2147483647LL
#define MESH_INTPTR_SHIFT (2)
#else
#error platform must be 32 or 64 bits
#endif

#define MESH_INTPTR_SIZE (1 << MESH_INTPTR_SHIFT)
#define MESH_INTPTR_BITS (MESH_INTPTR_SIZE * 8)

// ---------------------------------------------------------------------------------
// Provide our own `_mesh_memcpy` for potential performance optimizations.
//
// For now, only on Windows with msvc/clang-cl we optimize to `rep movsb` if
// we happen to run on x86/x64 cpu's that have "fast short rep movsb" (FSRM) support
// (AMD Zen3+ (~2020) or Intel Ice Lake+ (~2017). See also issue #201 and pr #253.
// ---------------------------------------------------------------------------------

#if defined(_WIN32) && (defined(_M_IX86) || defined(_M_X64))
extern bool _mesh_cpu_has_fsrm;
static inline void _mesh_memcpy(void *dst, const void *src, size_t n) {
  if (_mesh_cpu_has_fsrm) {
    __movsb((unsigned char *)dst, (const unsigned char *)src, n);
  } else {
    memcpy(dst, src, n);  // todo: use noinline?
  }
}
#else
static inline void _mesh_memcpy(void *dst, const void *src, size_t n) {
  memcpy(dst, src, n);
}
#endif

// -------------------------------------------------------------------------------
// The `_mesh_memcpy_aligned` can be used if the pointers are machine-word aligned
// This is used for example in `mi_realloc`.
// -------------------------------------------------------------------------------

#if (__GNUC__ >= 4) || defined(__clang__)
// On GCC/CLang we provide a hint that the pointers are word aligned.
static inline void _mesh_memcpy_aligned(void *dst, const void *src, size_t n) {
  d_assert(((uintptr_t)dst % MESH_INTPTR_SIZE == 0) && ((uintptr_t)src % MESH_INTPTR_SIZE == 0));
  void *adst = __builtin_assume_aligned(dst, MESH_INTPTR_SIZE);
  const void *asrc = __builtin_assume_aligned(src, MESH_INTPTR_SIZE);
  memcpy(adst, asrc, n);
}
#else
// Default fallback on `_mesh_memcpy`
static inline void _mesh_memcpy_aligned(void *dst, const void *src, size_t n) {
  d_assert(((uintptr_t)dst % MESH_INTPTR_SIZE == 0) && ((uintptr_t)src % MESH_INTPTR_SIZE == 0));
  _mesh_memcpy(dst, src, n);
}
#endif

#endif  // MESH_COMMON_H
