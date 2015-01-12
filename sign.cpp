#include "sign.h"
#include <string.h>

unsigned long cryptTable[0x500];

void generateHashtable() {  
    unsigned long seed = 0x00100001, index1 = 0, index2 = 0, i;  
    for( index1 = 0; index1 < 0x100; ++index1) {  
        for( index2 = index1, i = 0; i < 5; i++, index2 += 0x100) {  
            unsigned long temp1, temp2;  
            seed = (seed * 125 + 3) % 0x2AAAAB;  
            temp1 = (seed & 0xFFFF)<<0x10;  
            seed = (seed * 125 + 3) % 0x2AAAAB;  
            temp2 = (seed & 0xFFFF);  
            cryptTable[index2] = ( temp1 | temp2 );  
        }  
    }  
}  

unsigned long sign32(const char *str, int len, unsigned long type)  
{  
    unsigned char *key  = (unsigned char *)str;  
    unsigned long seed1 = 0x7FED7FED;  
    unsigned long seed2 = 0xEEEEEEEE;  
    int ch;  

    for (int i = 0; i < len; ++i) {
        ch = *key++;
        seed1 = cryptTable[(type<<8) + ch] ^ (seed1 + seed2);  
        seed2 = ch + seed1 + seed2 + (seed2<<5) + 3;  
    }  
    return seed1;  
}  

uint64_t get_sign64(const char* str, int len) {
    uint64_t sign = 0;
    unsigned int* sign1 = (unsigned int*)&sign;
    unsigned int* sign2 = (unsigned int*)(4 + (char*)sign1);
    if( len <= 4 ) {
        memcpy(sign1, str, len);
        return sign;
    } else {
        if(len<=8) {
            memcpy(sign1, str, 4);
            memcpy(sign2, str+4, len-4);
            return sign;
        } else {
            (*sign1)= sign32(str, len, 0);
            (*sign2)= sign32(str, len, 1);
            return sign;
        }
    }
}

uint32_t get_sign32(const char* str, int len) {
    return sign32(str, len, 0);
}












