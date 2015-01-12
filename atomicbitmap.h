#ifndef _ATOMIC_BITMAP_H_
#define _ATOMIC_BITMAP_H_

#include <limits>
#include <vector>
#include <string.h>
#include "sign.h"

#define FIXED_BITMAP_SIZE 32

class AtomicBitmap {
public:
    typedef unsigned short BlockType;
    AtomicBitmap(short bits);
    AtomicBitmap();
    ~AtomicBitmap();
    AtomicBitmap& operator=(const AtomicBitmap& map) {
        BlockType* blocks = (BlockType*)(_info.m_blocks);
        if (blocks != NULL) {
            delete blocks;
            blocks = NULL;
        }
        _info.m_block_num = map.getBlockNum();
        blocks = new BlockType[_info.m_block_num];
        memcpy(blocks, map.getBlocks(), sizeof(BlockType) * _info.m_block_num);
        _info.m_blocks = (long long)(blocks);
        return *this;
    }
    bool resize(short bits);
    bool operator==(const AtomicBitmap& map) {
        BlockType* blocks = (BlockType*)(_info.m_blocks);
        for (short i = 0; i < _info.m_block_num; i++) {
            if (0 != memcmp(blocks + i, map.getBlocks() + i, sizeof(BlockType))) {
                return false;
            }
        }
        return true;
    }
    bool operator!=(const AtomicBitmap& map) {
        BlockType* blocks = (BlockType*)(_info.m_blocks);
        for (short i = 0; i < _info.m_block_num; i++) {
            if (0 != memcmp(blocks+i, map.getBlocks() + i, sizeof(BlockType))) {
                return true;
            }
        }
        return false;
    }
    bool set(int idx);
    bool reset(int idx = -1);
    bool operator[](size_t idx) const;
    bool  hasDirty() {
        BlockType* blocks = (BlockType*)(_info.m_blocks);
        for (short i = 0; i < _info.m_block_num; i++) {
            if (blocks[i] != 0) {
                return true;
            }
        }
        return false;
    }
    int getDirty(std::vector<int>& dirties) {
        int cnt = 0;
        BlockType* blocks = (BlockType*)(_info.m_blocks);
        for (short i = 0; i < _info.m_block_num; i++) {
             BlockType t = blocks[i];
             int j = 0;
             while (t != 0) {
                if (t & 0x01) {
                    dirties.push_back(i * kBitsPerBlock + j);
                    cnt++;
                }
                t >>= 1;
                j ++;
             }
        }
        return cnt;
    }
    const BlockType* getBuffer(short i) const {
        return (BlockType*)(_info.m_blocks) + i;
    }
    inline short getBlockNum() const {return _info.m_block_num;}
private:

    static const size_t kBitsPerBlock =
        std::numeric_limits<BlockType>::digits;

    static size_t blockIndex(size_t bit) {
        return bit / kBitsPerBlock;
    }

    static size_t bitOffset(size_t bit) {
        return bit % kBitsPerBlock;
    }
    static const BlockType kOne = 1;
    const BlockType* getBlocks() const {return (BlockType*)(_info.m_blocks);}
private:
    union {
        long long  _block;
        struct {
            long long     m_blocks : 48;
            short         m_block_num : 16;
        } _info;
    };
};

 class AtomicFixedBitmap {
public:
#ifdef __x86_64__
    typedef unsigned long long BlockType;
#elif __i386__
    typedef unsigned int BlockType;
#endif
    AtomicFixedBitmap();
    ~AtomicFixedBitmap();
    AtomicFixedBitmap& operator=(const AtomicFixedBitmap& map) {
        memcpy(m_blocks, map.getBlocks(), sizeof(BlockType) * FIXED_BITMAP_SIZE);
        return *this;
    }
    bool operator==(const AtomicFixedBitmap& map) {
        if (0 != memcmp(m_blocks, map.getBlocks(), sizeof(BlockType) * FIXED_BITMAP_SIZE)) {
            return false;
        }
        return true;
    }
    bool operator!=(const AtomicFixedBitmap& map) {
        if (0 != memcmp(m_blocks, map.getBlocks(), sizeof(BlockType) * FIXED_BITMAP_SIZE)) {
            return true;
        }
        return false;
    }
    bool set(int idx);
    bool reset(int idx = -1);
    bool operator[](size_t idx) const;
    bool  hasDirty() {
        for (short i = 0; i < FIXED_BITMAP_SIZE; i++) {
            if (m_blocks[i] != 0) {
                return true;
            }
        }
        return false;
    }
    int getDirty(std::vector<int>& dirties) {
        int cnt = 0;
        for (short i = 0; i < FIXED_BITMAP_SIZE; i++) {
             BlockType t = m_blocks[i];
             int j = 0;
             while (t != 0) {
                if (t & 0x01) {
                    dirties.push_back(i * kBitsPerBlock + j);
                    cnt++;
                }
                t >>= 1;
                j ++;
             }
        }
        return cnt;
    }
    const BlockType* getBuffer(short i) const {
        return m_blocks + i;
    }
    uint64_t sign() {
        return get_sign64((char*)m_blocks, sizeof(BlockType) * FIXED_BITMAP_SIZE);
    }
private:

    static const size_t kBitsPerBlock =
        std::numeric_limits<BlockType>::digits;

    static size_t blockIndex(size_t bit) {
        return bit / kBitsPerBlock;
    }

    static size_t bitOffset(size_t bit) {
        return bit % kBitsPerBlock;
    }
    static const BlockType kOne = 1;
    const BlockType* getBlocks() const {return m_blocks;}
private:
    BlockType  m_blocks[FIXED_BITMAP_SIZE];
};


#endif

