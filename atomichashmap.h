#ifndef _ATOMIC_HASHMAP_H_
#define _ATOMIC_HASHMAP_H_

#include <string.h>
#include <utility>
#include <iterator>
#include <new>
#include <stdint.h>
#include <assert.h>
#include "atomiccache.h"
#include "atomicbitmap.h"
#include "sign.h"
#include "locks.h"

using namespace std;

#define MAX_LOCKS_NUM  64
#define BEFORE_STARTING_POS ((void*)-1L)

static const uint64_t internal_prime_list[] = {
	13ul, 53ul, 97ul, 193ul, 389ul, 769ul,
	1543ul, 3079ul, 6151ul, 12289ul, 24593ul,
	49157ul, 98317ul, 196613ul, 393241ul, 786433ul,
	1572869ul, 2500009, 3145739ul, 6291469ul, 12582917ul, 25165843ul,
	50331653ul, 100663319ul, 201326611ul, 402653189ul, 805306457ul,
	1610612741ul, 3221225473ul, 4294967291ul
};

template<typename T>
inline void FreeAtomicHashmapTElement(T t) {
}

template<typename T>
void PushAtomicHashmapTElementToCache(AtomicCache<T>* cache, T t) {
}

template<typename T>
void PushAtomicHashmapTElementToCache(AtomicCache<T*>* cache, T* t) {
    if (t != NULL && cache != NULL) {
        cache->push(t);
    }
}

template<typename T>
inline void FreeAtomicHashmapTElement(T* t) {
    if (t != NULL) {
        delete t;
        t = NULL;
    }
}

//! compare key
template < class TKey >
inline bool MapCompareKey(const TKey & key1, const TKey & key2)
{
	return (key1 == key2);
}

inline bool MapCompareKey(const char*& key1, const char*& key2)
{
	if (strcmp(key1, key2) == 0)
		return true;
	else
		return false;
}

template < class TKey > inline uint64_t HashValue(TKey  key)
{
	return (uint64_t)key;
}

template<> inline uint64_t HashValue<const char*>(const char* key) {
    return get_sign64(key, strlen(key));
}

template<> inline uint64_t HashValue<std::string>(std::string key) {
    return get_sign64(key.c_str(), key.length());
}

template<> inline uint64_t HashValue<AtomicFixedBitmap>(AtomicFixedBitmap key) {
    return key.sign();
}


template <class TKey, class TElement>
class AtomicHashmap
{
	public:
		class iterator : public std::iterator < std::forward_iterator_tag, pair < const TKey, TElement > > {
		public:
			typedef pair < const TKey, TElement > value_type;
			typedef TKey key_type;
			iterator()
            : ptr(NULL)
            , pMap(NULL) {}

			iterator(void *p)
            : ptr((value_type *)p)
            , pMap(NULL) {}

			iterator(void *pEntry, void *map)
            : ptr((value_type *)pEntry)
            , pMap((AtomicHashmap < TKey, TElement > *)map) {}

			bool operator ==(const iterator & iter) {
				return iter.ptr == ptr;
			}
			bool operator !=(const iterator & iter) {
				return iter.ptr != ptr;
			}
			// increase and then fetch
			iterator & operator ++() {
				TKey *pKey;
				TElement *pValue;
				void *pos;
				pos = (char *) ptr - sizeof(SEntry *);
				if (pMap->FindNext(pos, pKey, pValue))
					ptr = (value_type *) ((uint8_t *) pos + sizeof(SEntry *));
				else
					ptr = NULL;
				return *this;
			}

			// fetch and then increase
			const iterator operator ++(int) {
				iterator tmp = *this;
				TKey *pKey;
				TElement *pValue;
				void *pos;
				pos = (uint8_t *) ptr - sizeof(SEntry *);
				if (pMap->FindNext(pos, pKey, pValue))
					ptr = (value_type *) ((uint8_t *) pos + sizeof(SEntry *));
				else
					ptr = NULL;
				return tmp;
			}

			value_type & operator *() const {
				return (value_type &) * ptr;
			}

			value_type *operator ->() const {
				return (value_type *) ptr;
			}
		protected:
			value_type * ptr;
			AtomicHashmap < TKey, TElement > *pMap;
    	};
		typedef typename iterator::key_type key_type;
		typedef typename iterator::value_type value_type;
    	typedef typename iterator::pointer pointer;
		typedef typename iterator::reference reference;

		iterator begin() {
			TKey *pKey;
			TElement *pValue;
			void *pos;
			if (FindFirst(pos, pKey, pValue))
				return iterator((uint8_t *) pos + sizeof(SEntry *), this);
			else
				return NULL;
		}

		void clear() {
            if (NULL == m_pHashTable) {
                return;
            }
            LockAll();
            EntryTable* table = m_pHashTable;
            EntryTable* newTable = new EntryTable();
            (void)__sync_lock_test_and_set(&m_pHashTable, newTable);
            UnlockAll();
            m_free_table->push(table);
		}


		bool empty() const {
			return (m_pHashTable->m_nCount == 0);
		}

		iterator end() {
			return NULL;
		}

		bool insert(const TKey & key, const TElement & value, bool auto_resize = true)
		{
            int lock_index = Lock(key);
             iterator iter = find(key);
            if (iter != end()) {
                UnlockIndex(lock_index);
                return false;
            }           
            SafeAdd(key, value, lock_index, auto_resize);
            UnlockIndex(lock_index);
            __sync_fetch_and_add(&m_pHashTable->m_nCount, 1);
            return true;
        }
		bool insertNolock(const TKey & key, const TElement & value, int& lock_index, bool auto_resize = true)
		{
             iterator iter = find(key);
            if (iter != end()) {
                return false;
            }           
            SafeAdd(key, value, lock_index, auto_resize);
            __sync_fetch_and_add(&m_pHashTable->m_nCount, 1);
            return true;
        }

		pair < iterator, bool > insert(const value_type & x, bool auto_resize = true)
		{
			const TKey & key = x.first;
			const TElement & value = x.second;
			pair < iterator, bool > ret;
			iterator iter;

            uint64_t nHash;
            int lock_index = Lock(key);
            iter = find(key);
            if (iter != end()) {
                ret.first = iter;
                ret.second = false;
                UnlockIndex(lock_index);
                return ret;
            }
            SEntry *pEntry;
            if (auto_resize) {
                pEntry = NewEntry(key, lock_index);
            } else {
                pEntry = SafeNewEntry(key);
            }
            nHash = HashValue(key) % m_pHashTable->m_nHashTableSize;
            // put into hash table
            pEntry->pNext = m_pHashTable->m_pHashTable[nHash];
            m_pHashTable->m_pHashTable[nHash] = pEntry;

            pEntry->value = value;
            iter = iterator((uint8_t *) pEntry + sizeof(SEntry *), this);
            ret.second = true;
            ret.first = iter;
            UnlockIndex(lock_index);
            __sync_fetch_and_add(&m_pHashTable->m_nCount, 1);
            return ret;
		}

		iterator find(const TKey & key)
		{
			SEntry *pEntry = GetEntryAt(key);
			if (pEntry == NULL) {
				return NULL;
            } else {
				return iterator((uint8_t *) pEntry + sizeof(SEntry *), this);
            }
		}

		//returns the size of AtomicHashmap
		uint64_t size() const
		{
			return m_pHashTable->m_nCount;
		}

		// same as resize
		void reserve(uint64_t nHint)
		{
			ReSize(nHint);
		}

		void resize(uint64_t nHint)
		{
			ReSize(nHint);
		}

		explicit AtomicHashmap(bool free_V = false, uint64_t nHint = 10, bool lazy = true)
		: m_free_V(free_V)
        , m_lazy(lazy) {
			m_pHashTable = NULL;
            if (lazy) {
                m_free_entry = new AtomicLazyCache<SEntry*>();
            } else {
                m_free_entry = new AtomicThreadCache<SEntry*>();
            }
            m_free_entry->init(12, true, true);
            if (lazy) {
                m_free_elem = new AtomicLazyCache<TElement>();
            } else {
                m_free_elem = new AtomicThreadCache<TElement>();
            }
            m_free_elem->init(11, free_V, false);
            // this is used for resize
            if (lazy) {
                m_free_table = new AtomicLazyCache<EntryTable*>();
            } else {
                m_free_table = new AtomicThreadCache<EntryTable*>();
            }
            m_free_table->init(8, true, false);
			(void)ReSize(nHint);
            m_rehashing = false;
		}

		explicit AtomicHashmap(const AtomicHashmap & other)
		{
            bool lazy = other.isLazy();
            if (lazy) {
                m_free_entry = new AtomicLazyCache<SEntry*>();
            } else {
                m_free_entry = new AtomicThreadCache<SEntry*>();
            }
            m_free_entry->init(12, true, true);
            if (lazy) {
                m_free_elem = new AtomicLazyCache<TElement>();
            } else {
                m_free_elem = new AtomicThreadCache<TElement>();
            }
            m_free_elem->init(12, other.isFreeV(), false);
            // this is used for resize
            if (lazy) {
                m_free_table = new AtomicLazyCache<EntryTable*>();
            } else {
                m_free_table = new AtomicThreadCache<EntryTable*>();
            }
            m_free_table->init(8, true, false);
			*this = other;
            m_rehashing = false;
		}
		~AtomicHashmap()
		{
            if (m_free_entry != NULL) {
                delete m_free_entry;
                m_free_entry = NULL;
            }
            if (m_free_elem != NULL) {
                delete m_free_elem;
                m_free_elem = NULL;
            }
            if (m_free_table != NULL) {
                delete m_free_table;
                m_free_table = NULL;
            }

            if (m_pHashTable != NULL) {
                delete m_pHashTable;
                m_pHashTable = NULL;
            }
		}


		bool FindFirst(void *&pos, TKey * &rKey, TElement * &rValue) const
		{
			if (m_pHashTable->m_nCount == 0) {
				pos = NULL;
				rKey = NULL;
				rValue = NULL;
				return false;
			} else {
				pos = BEFORE_STARTING_POS;
				return FindNext(pos, rKey, rValue);
			}
		}

		bool FindNext(void *&pos, TKey * &rKey, TElement * &rValue) const
		{
			uint64_t nBucket;
			SEntry *pEntryRet = (SEntry *) pos;
            // use atomic ops to make sure get the real info
            EntryTable* table = NULL;
            (void)__sync_lock_test_and_set(&table, m_pHashTable);
            uint64_t size = table->m_nHashTableSize;
            SEntry** pTable = table->m_pHashTable;

			if (pEntryRet == (SEntry *) BEFORE_STARTING_POS)
			{
				// find the first entry
				for (nBucket = 0; nBucket < size; nBucket++)
					if ((pEntryRet = pTable[nBucket]) != NULL)
						break;
			} else {
				if (NULL != pEntryRet->pNext)
					pEntryRet = pEntryRet->pNext;
				else
				{
					uint64_t nHash = HashValue(pEntryRet->key) % size;
					pEntryRet = NULL;	// set default
					for (nBucket = nHash + 1; nBucket < size;
							nBucket++)
						if ((pEntryRet = pTable[nBucket]) != NULL)
							break;
				}
			}
			if (pEntryRet == NULL) {
				pos = NULL;
				rKey = NULL;
				rValue = NULL;
				return false;
			} else {
				pos = (void *) pEntryRet;
				rKey = &(pEntryRet->key);
				rValue = &(pEntryRet->value);
				return true;
			}
		}

		bool Lookup(const TKey & key) const
		{
			SEntry *pEntry = GetEntryAt(key);
			if (pEntry == NULL)
				return false;
			else
				return true;
		}

		bool Lookup(const TKey & key, TElement * &pValue) const
		{
			SEntry *pEntry = GetEntryAt(key);
			if (pEntry == NULL)
			{
				pValue = NULL;
				return false;
			}
			pValue = &(pEntry->value);
			return true;
		}

		bool Lookup(const TKey & key, TElement & rValue) const
		{
			SEntry *pEntry = GetEntryAt(key);
			if (pEntry == NULL)
				return false;		// not in map

			rValue = (pEntry->value);
			return true;
		}

		//deep copy
		const AtomicHashmap < TKey, TElement > &operator =(const AtomicHashmap & src)
        {
            if (this != &src)
            {
                m_free_V = src.isFreeV();
                m_rehashing = false;
                void *pos;
                TKey *pKey;
                TElement *pValue;
                bool more = src.FindFirst(pos, pKey, pValue);
                while (more)
                {
                    int lock_index = Lock(*pKey);
                    (*this)[*pKey] = *pValue;
                    UnlockIndex(lock_index);
                    more = src.FindNext(pos, pKey, pValue);
                }
            }
            return *this;
        }

		bool Remove(const TKey & key)
		{
			if (m_pHashTable == NULL)
				return false;
            // atomic ops
            EntryTable* table = NULL;
            int lock_index = Lock(key);
            table = m_pHashTable;
            uint64_t size = table->m_nHashTableSize;
            SEntry** pTable = table->m_pHashTable;           
            
            
            SEntry **ppEntryPrev;
			ppEntryPrev = &pTable[HashValue(key) % size];
            
			SEntry *pEntry;
			for (pEntry = *ppEntryPrev; pEntry != NULL; pEntry = pEntry->pNext)
			{
				if (pEntry->key == key)
				{
					// remove it
					*ppEntryPrev = pEntry->pNext;
                    __sync_fetch_and_sub(&table->m_nCount, 1);
                    UnlockIndex(lock_index);
                    m_free_entry->push(pEntry);
                    if (m_free_V) {
                        PushAtomicHashmapTElementToCache(m_free_elem, pEntry->value);
                    }
                    return true;
                }
                ppEntryPrev = &pEntry->pNext;
            }
            UnlockIndex(lock_index);
			return false;
		}

        bool RemoveNolock(const TKey & key) {
            uint64_t size = m_pHashTable->m_nHashTableSize;
            SEntry** pTable = m_pHashTable->m_pHashTable;           
            
            
            SEntry **ppEntryPrev;
			ppEntryPrev = &pTable[HashValue(key) % size];
            
			SEntry *pEntry;
			for (pEntry = *ppEntryPrev; pEntry != NULL; pEntry = pEntry->pNext)
			{
				if (pEntry->key == key)
				{
					// remove it
					*ppEntryPrev = pEntry->pNext;
                    m_free_entry->push(pEntry);
                    if (m_free_V) {
                        PushAtomicHashmapTElementToCache(m_free_elem, pEntry->value);
                    }
                    __sync_fetch_and_sub(&m_pHashTable->m_nCount, 1);
                    return true;
                }
                ppEntryPrev = &pEntry->pNext;
            }
			return false;
        }
        inline bool isFreeV() {return m_free_V;}

		void ReSize(uint64_t nHint)
		{
			uint32_t i;
			uint64_t nHashSize;
            // find the size from the listtable
			uint32_t nListSize = sizeof(internal_prime_list) / sizeof(internal_prime_list[0]);
			for (i = 0; i < nListSize; i++)
			{
				if (internal_prime_list[i] >= nHint)
					break;
			}
			nHashSize = internal_prime_list[i];
            // create hashtable
            if (NULL == m_pHashTable)
			{
                m_pHashTable = new(std::nothrow) EntryTable(nHashSize);
			}
			else
			{
				SEntry *pEntry;
                EntryTable* pNewTable = new(std::nothrow) EntryTable(nHashSize);
				uint64_t i, nNewhash, nOld;
				nOld = m_pHashTable->m_nHashTableSize;
				for (i = 0; i < nOld; i++)
				{
					pEntry = m_pHashTable->m_pHashTable[i];
					while (pEntry)
					{
						nNewhash = HashValue(pEntry->key) % nHashSize;
						m_pHashTable->m_pHashTable[i] = pEntry->pNext;
						pEntry->pNext = pNewTable->m_pHashTable[nNewhash];
						pNewTable->m_pHashTable[nNewhash] = pEntry;
						pEntry = m_pHashTable->m_pHashTable[i];
                        pNewTable->m_nCount++;
					}
				}
                m_free_table->push(m_pHashTable);
                (void)__sync_lock_test_and_set(&m_pHashTable, pNewTable);
			}
		}
        bool isLazy() const {return m_lazy;}
        // lock func
        int Lock(TKey key) {
           uint64_t size = m_pHashTable->m_nHashTableSize;
           uint64_t ori_size = 0;
           uint64_t lock_idx = 0;
           uint64_t mutex_idx = 0;
           uint32_t gap = 1;
           bool start = false;
           do {
               if (start) {
                   m_locks[mutex_idx].unlock();
               }
               ori_size = size;
               lock_idx = HashValue(key)%size;
               gap = size/MAX_LOCKS_NUM;
               if (gap <= 0) {gap = 1;}
               mutex_idx = lock_idx/gap;
               mutex_idx = mutex_idx >= MAX_LOCKS_NUM ? MAX_LOCKS_NUM - 1 : mutex_idx;
               m_locks[mutex_idx].lock();
               size = m_pHashTable->m_nHashTableSize;
               start = true;
           } while (ori_size != size);
           return mutex_idx;
        }

        void UnlockIndex(int index) {
            if (index < 0) {return;}
            m_locks[index].unlock();
        }

        void ContinueUnlockIndex(int index) {
            for (int i = 0; i < MAX_LOCKS_NUM; i++) {
                if (i == index) {continue;}
                m_locks[i].unlock();
            }
        }

         void ContinueLockIndex(int index) {
            for (int i = 0; i < MAX_LOCKS_NUM; i++) {
                if (i == index) {continue;}
                m_locks[i].lock();
            }
        }       

        void LockAll() {
            for (int i = 0; i < MAX_LOCKS_NUM; i++) {
                m_locks[i].lock();
            }
        }

        void UnlockAll() {
            for (int i = 0; i < MAX_LOCKS_NUM; i++) {
                m_locks[i].unlock();
            }
        }
	protected:
		// node struct
		struct SEntry
		{
			SEntry *pNext;
			TKey key;
			TElement value;
		};

	protected:
        // we create two SEntry** to store items
        // as one is used for rehash
        // use s struct to store the table info
        // in order to exchange table in atomic ops
        struct EntryTable {
            SEntry**   m_pHashTable;
            uint64_t m_nHashTableSize;
            uint64_t m_nCount;
            EntryTable(uint64_t size = 0)
            : m_pHashTable(NULL)
            , m_nHashTableSize(size)
            , m_nCount(0) {
                uint32_t i = 0;
                uint32_t nListSize =
                    sizeof(internal_prime_list) / sizeof(internal_prime_list[0]);
                for (i = 0; i < nListSize; i++)
                {
                    if (internal_prime_list[i] >= size)
                        break;
                }
                uint64_t nHashSize = internal_prime_list[i];
                m_pHashTable = new(std::nothrow) SEntry *[nHashSize];
                if (NULL != m_pHashTable) {
                    memset(m_pHashTable, 0, sizeof(SEntry *) * nHashSize);
                    m_nHashTableSize = nHashSize;
                }
            }
            ~EntryTable() {
                if (NULL != m_pHashTable) {
                    uint64_t i = 0;
                    for (; i < m_nHashTableSize; i++) {
                        SEntry *pEntry = m_pHashTable[i];
                        SEntry* tmp = NULL;
                        while (pEntry != NULL) {
                            tmp = pEntry;
                            pEntry = pEntry->pNext;
                            delete tmp;
                        }
                    }
                    delete[] m_pHashTable;
                    m_pHashTable = NULL;
                }
                m_nCount = 0;
            }
        };
        EntryTable*  m_pHashTable;
        bool       m_rehashing;
		
        bool       m_free_V;
        MicroSpinLock              m_locks[MAX_LOCKS_NUM];
        // the first ver is not lockfree it uses many locks
        AtomicCache<SEntry*>*  m_free_entry;
        // this is used for resize
        AtomicCache<EntryTable*>*  m_free_table;
        AtomicCache<TElement>*  m_free_elem;
		// Implementation
        bool       m_lazy;
		TElement *Add(const TKey & key, int& lock_index)
		{
			uint64_t nHash;
			SEntry *pEntry;
			assert(m_pHashTable != NULL);
			assert(GetEntryAt(key) == NULL);

			// always add a new entry
			pEntry = NewEntry(key, lock_index);
			nHash = HashValue(key) % m_pHashTable->m_nHashTableSize;

			// put into hash table
			pEntry->pNext = m_pHashTable->m_pHashTable[nHash];
			m_pHashTable->m_pHashTable[nHash] = pEntry;
			return &(pEntry->value);
		}

		TElement *SafeAdd(const TKey & key, const TElement & value, int& lock_index, bool auto_resize = true)
		{
			uint64_t nHash;
			SEntry *pEntry;
			assert(m_pHashTable != NULL);

			// always add a new entry
			if (auto_resize) 
			{
				pEntry = NewEntry(key, lock_index);
			}
			else
			{
				pEntry = SafeNewEntry(key);
			}
			nHash = HashValue(key) % m_pHashTable->m_nHashTableSize;

			// set value for new entry
			pEntry->value = value;

			// put into hash table
			pEntry->pNext = m_pHashTable->m_pHashTable[nHash];
			m_pHashTable->m_pHashTable[nHash] = pEntry;
			return &(pEntry->value);
		}

		SEntry *NewEntry(const TKey & key, int& key_mutex)
		{
			SEntry * pEntry;
            uint64_t count = m_pHashTable->m_nCount;
			// automatically expand the hash table for efficiency
			if (count > 4 * m_pHashTable->m_nHashTableSize) {
                if (!__sync_bool_compare_and_swap(&m_rehashing, false, true)) {
                    // fix me here if other threads check the talbe is rehashing then they will not alloc mem for new item
                    // we make it that when the table is rehashing no other threads will do insert func until rehashing is over
                    // so this  may slow the speed after we can do something else to make it more efficiently
                    UnlockIndex(key_mutex);
                    Sleeper sleeper;
                    while (m_rehashing != false) {
                        sleeper.wait();
                    }
                    if (key_mutex >= 0) {
                        key_mutex = Lock(key);
                    }
                } else {
                    ContinueLockIndex(key_mutex);
                    ReSize(count);
                    m_rehashing = false;
                    UnlockAll();
                    if (key_mutex >= 0) {
                        key_mutex = Lock(key);
                    }
                }
            }
			if (!m_free_entry->pop(pEntry))
			{
				pEntry = new SEntry();
			}
            pEntry->pNext = NULL;
			pEntry->key = key;
			return pEntry;
		}

		SEntry *SafeNewEntry(const TKey & key)
		{
			SEntry * pEntry;
			if (!m_free_entry->pop(pEntry))
			{
				pEntry = new SEntry();
			}
			pEntry->key = key;
			return pEntry;
		}

		SEntry *GetEntryAt(const TKey & key) const
		{
            EntryTable* pTable;
            __sync_lock_test_and_set(&pTable, m_pHashTable);
            SEntry** table = pTable->m_pHashTable;
            uint64_t nHash = HashValue(key) % pTable->m_nHashTableSize;;
			// see if it exists
			SEntry *pEntry;
			for (pEntry = table[nHash]; pEntry != NULL;
					pEntry = pEntry->pNext)
			{
				if (MapCompareKey(pEntry->key, key))
					return pEntry;
			}
			return NULL;
		}
};


#endif

