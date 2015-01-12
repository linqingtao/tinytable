#include "locks.h"
#include <string.h>

void Sleeper::wait() {
    if (spinCount < kMaxActiveSpin) {
        ++spinCount;
        asm volatile("pause");
    } else {
        /*
         * Always sleep 0.5ms, assuming this will make the kernel put
         * us down for whatever its minimum timer resolution is (in
         * linux this varies by kernel version from 1ms to 10ms).
         */
        struct timespec ts = { 0, 500000 };
        nanosleep(&ts, NULL);
    }
}

MicroSpinLock::MicroSpinLock()
: lock_(FREE) {}

bool MicroSpinLock::cas(uint8_t compare, uint8_t newVal) {
    bool out;
    bool memVal; // only set if the cmpxchg fails
    asm volatile("lock; cmpxchgb %[newVal], (%[lockPtr]);"
            "setz %[output];"
            : [output] "=r" (out), "=a" (memVal)
            : "a" (compare), // cmpxchgb constrains this to be in %al
            [newVal] "q" (newVal),  // Needs to be byte-accessible
            [lockPtr] "r" (&lock_)
            : "memory", "flags");
    return out;
}

bool MicroSpinLock::try_lock() {
    return cas(FREE, LOCKED);
}

void MicroSpinLock::lock() {
    Sleeper sleeper;
    do {
        while (lock_ != FREE) {
            asm volatile("" : : : "memory");
            sleeper.wait();
        }
    } while (!try_lock());
    //DCHECK(lock_ == LOCKED);
}

void MicroSpinLock::unlock() {
    //CHECK(lock_ == LOCKED);
    asm volatile("" : : : "memory");
    lock_ = FREE; // release barrier on x86
}


bool PicoSpinLock::init(int bits) {
    //CHECK(!(initialValue & kLockBitMask_));
    int num = (bits + sizeof(UIntType)*8 - 1)/sizeof(UIntType);
    lock_ = new(std::nothrow) UIntType[num];
    memset(lock_, 0, sizeof(UIntType) * num);
    if (NULL == lock_) {
        return false;
    }
    len = num;
    return true;
}

void PicoSpinLock::clear() {
    if (lock_ != NULL) {
        memset(lock_, 0, sizeof(UIntType) * (int)len);
    }
}

/*
 * Try to get the lock without blocking: returns whether or not we
 * got it.
 */
//template<int N>
bool PicoSpinLock::try_lock(UIntType N) const {
    bool ret = false;
    int offset = N / sizeof(UIntType);
#define FB_DOBTS(size)                                  \
    asm volatile("lock; bts"#size" %1, (%2); setnc %0"  \
            : "=r" (ret)                             \
            : "r" (N%sizeof(UIntType)),                             \
            "r" (&lock_[offset])                           \
            : "memory", "flags")

    switch (sizeof(UIntType)) {
        case 2: FB_DOBTS(w); break;
        case 4: FB_DOBTS(l); break;
        case 8: FB_DOBTS(q); break;
    }

#undef FB_DOBTS

    return ret;
}

/*
 * Block until we can acquire the lock.  Uses Sleeper to wait.
 */
// template <int N>
void PicoSpinLock::lock(UIntType N) const {
    Sleeper sleeper;
    while (!try_lock(N)) {
        sleeper.wait();
    }
}

/*
 * Release the lock, without changing the value of the rest of the
 * integer.
 */
// template<int N>
void PicoSpinLock::unlock(UIntType N) const {
    bool ret = false;
    int offset = N / sizeof(UIntType);
#define FB_DOBTR(size)                          \
    __asm__ __volatile__ ("lock; btr"#size"  %0, (%1)"    \
            :                              \
            : "r" (N%sizeof(UIntType)),                     \
            "r" (&lock_[offset])      \
            : "memory", "flags")


    // Reads and writes can not be reordered wrt locked instructions,
    // so we don't need a memory fence here.
    switch (sizeof(UIntType)) {
        case 2: FB_DOBTR(w); break;
        case 4: FB_DOBTR(l); break;
        case 8: FB_DOBTR(q); break;
    }

#undef FB_DOBTR
}


