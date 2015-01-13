#ifndef _ATOMIC_SIGN_H__
#define _ATOMIC_SIGN_H__
#include <stdint.h>

//extern void generateHashtable();
extern uint64_t get_sign64 (const char* str, int len);
extern uint32_t get_sign32 (const char* str, int len);

#endif

