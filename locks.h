/*
 * Copyright 2014 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FOLLY_SMALLLOCKS_H_
#define FOLLY_SMALLLOCKS_H_

/*
 * This header defines a few very small mutex types.  These are useful
 * in highly memory-constrained environments where contention is
 * unlikely.
 *
 * Note: these locks are for use when you aren't likely to contend on
 * the critical section, or when the critical section is incredibly
 * small.  Given that, both of the locks defined in this header are
 * inherently unfair: that is, the longer a thread is waiting, the
 * longer it waits between attempts to acquire, so newer waiters are
 * more likely to get the mutex.  For the intended use-case this is
 * fine.
 *
 * @author Keith Adams <kma@fb.com>
 * @author Jordan DeLong <delong.j@fb.com>
 */

#include <array>
#include <cinttypes>
#include <type_traits>
#include <ctime>
#include <cstdlib>
#include <pthread.h>
#include <mutex>
//#include <asm/sync_bitops.h>

#if !__x86_64__
# error "SmallLocks.h is currently x64-only."
#endif


//////////////////////////////////////////////////////////////////////


  /*
   * A helper object for the condended case. Starts off with eager
   * spinning, and falls back to sleeping for small quantums.
   */
  class Sleeper {
    static const uint32_t kMaxActiveSpin = 4000;

    uint32_t spinCount;

  public:
    Sleeper() : spinCount(0) {}

    void wait();
  };


//////////////////////////////////////////////////////////////////////

/*
 * A really, *really* small spinlock for fine-grained locking of lots
 * of teeny-tiny data.
 *
 * Zero initializing these is guaranteed to be as good as calling
 * init(), since the free state is guaranteed to be all-bits zero.
 *
 * This class should be kept a POD, so we can used it in other packed
 * structs (gcc does not allow __attribute__((packed)) on structs that
 * contain non-POD data).  This means avoid adding a constructor, or
 * making some members private, etc.
 */
struct MicroSpinLock {
  enum { FREE = 0, LOCKED = 1 };
  uint8_t lock_;
  MicroSpinLock();
  /*
   * Atomically move lock_ from "compare" to "newval". Return boolean
   * success. Do not play on or around.
   */
  bool cas(uint8_t compare, uint8_t newVal);

  bool try_lock();

  void lock();


  void unlock();
};// __attribute__ ((packed));

//////////////////////////////////////////////////////////////////////

/*
 * Spin lock on a single bit in an integral type.  You can use this
 * with 16, 32, or 64-bit integral types.
 *
 * This is useful if you want a small lock and already have an int
 * with a bit in it that you aren't using.  But note that it can't be
 * as small as MicroSpinLock (1 byte), if you don't already have a
 * convenient int with an unused bit lying around to put it on.
 *
 * To construct these, either use init() or zero initialize.  We don't
 * have a real constructor because we want this to be a POD type so we
 * can put it into packed structs.
 */
struct PicoSpinLock {
  // Internally we deal with the unsigned version of the type.
  //typedef typename std::make_unsigned<IntType>::type UIntType;
   typedef uint16_t UIntType;
  /*static_assert(std::is_integral<IntType>::value,
                "PicoSpinLock needs an integral type");
  static_assert(sizeof(UIntType) == 2 || sizeof(UIntType) == 4 ||
                  sizeof(UIntType) == 8,
                "PicoSpinLock can't work on integers smaller than 2 bytes");
*/
 public:
  //static const UIntType kLockBitMask_ = UIntType(1) << Bit;
  UIntType* lock_;
  unsigned char len;
  /*
   * You must call this function before using this class, if you
   * default constructed it.  If you zero-initialized it you can
   * assume the PicoSpinLock is in a valid unlocked state with
   * getData() == 0.
   *
   * (This doesn't use a constructor because we want to be a POD.)
   */
 bool init(int bits = 16);
  void clear();
  /*
   * Try to get the lock without blocking: returns whether or not we
   * got it.
   */
  //template<int N>
  bool try_lock(UIntType N) const;

  /*
   * Block until we can acquire the lock.  Uses Sleeper to wait.
   */
 // template <int N>
  void lock(UIntType N) const;

  /*
   * Release the lock, without changing the value of the rest of the
   * integer.
   */
 // template<int N>
  void unlock(UIntType N) const;
};


//////////////////////////////////////////////////////////////////////


#endif
