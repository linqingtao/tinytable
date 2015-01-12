#include "atomicbitmap.h"

AtomicBitmap::AtomicBitmap(short bits) {
    _info.m_block_num = (bits % kBitsPerBlock) ? bits/kBitsPerBlock + 1 : bits/kBitsPerBlock;
    BlockType* blocks = new BlockType[_info.m_block_num];
    memset(blocks, 0, sizeof(BlockType) * _info.m_block_num);
    _info.m_blocks = (long long)blocks;
}

AtomicBitmap::AtomicBitmap()
: _block(0) {
}

bool AtomicBitmap::resize(short bits) {
    _info.m_block_num = (bits % kBitsPerBlock) ? bits/kBitsPerBlock + 1 : bits/kBitsPerBlock;
    BlockType* blocks = new(std::nothrow) BlockType[_info.m_block_num];
    if (blocks == NULL) {
        return false;
    }
    memset(blocks, 0, sizeof(BlockType) * _info.m_block_num);
    _info.m_blocks = (long long)blocks;
    return true;
}

AtomicBitmap::~AtomicBitmap() {
    BlockType* blocks = (BlockType*)(_info.m_blocks);
    if (NULL != blocks) {
        delete blocks;
        blocks = NULL;
    }
}

bool AtomicBitmap::set(int idx) {
    BlockType* blocks = (BlockType*)(_info.m_blocks);
    BlockType mask = kOne << bitOffset(idx);
    return __sync_fetch_and_or(&blocks[blockIndex(idx)], mask);
}

bool AtomicBitmap::reset(int idx) {
    BlockType* blocks = (BlockType*)(_info.m_blocks);
    if (idx == -1) {
        for (short i = 0; i < _info.m_block_num; i++) {
            memset(&blocks[i], 0, kBitsPerBlock);
        }
        return true;
    } else {
        BlockType mask = kOne << bitOffset(idx);
        return __sync_fetch_and_and(&blocks[blockIndex(idx)], ~mask);
    }
}

bool AtomicBitmap::operator[](size_t idx) const {
    BlockType* blocks = (BlockType*)(_info.m_blocks);
    BlockType mask = kOne << bitOffset(idx);
    return __sync_and_and_fetch(&mask, blocks[blockIndex(idx)]);
}


AtomicFixedBitmap::AtomicFixedBitmap() {
    memset(m_blocks, 0, sizeof(BlockType) * FIXED_BITMAP_SIZE);
}

AtomicFixedBitmap::~AtomicFixedBitmap() {
}

bool AtomicFixedBitmap::set(int idx) {
    BlockType mask = kOne << bitOffset(idx);
    return __sync_fetch_and_or(&m_blocks[blockIndex(idx)], mask);
}

bool AtomicFixedBitmap::reset(int idx) {
    if (idx == -1) {
        for (short i = 0; i < FIXED_BITMAP_SIZE; i++) {
            memset(&m_blocks[i], 0, kBitsPerBlock);
        }
        return true;
    } else {
        BlockType mask = kOne << bitOffset(idx);
        return __sync_fetch_and_and(&m_blocks[blockIndex(idx)], ~mask);
    }
}

bool AtomicFixedBitmap::operator[](size_t idx) const {
    BlockType mask = kOne << bitOffset(idx);
    return __sync_and_and_fetch(&mask, m_blocks[blockIndex(idx)]);
}


