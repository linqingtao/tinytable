#ifndef _TABLE_ITEM_H_
#define _TABLE_ITEM_H_
#include <stdio.h>
#include <stdint.h>
#include <string>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <vector>
#include <set>
#include "atomiccache.h"
#include "atomicbitmap.h"
#include "sign.h"
#include "common.h"
#include "locks.h"
#include "atomichashmap.h"
#include "item_array.h"
#include "tinytable_log_utils.h"
#include "config.h"

class TableItem_t;

#define BASE_TIME 1262275200   //2010.1.1 0:0:0

class TableItem_t {
public:
    TableItem_t();
    TableItem_t(const TableConfig* config, TableItem_t item);
    ~TableItem_t();
protected:
    char*  value;
    AtomicBitmap m_dirty;
    mutable MicroSpinLock              m_lock;
    mutable MicroSpinLock              m_item_lock;
    mutable MicroSpinLock              m_trigger_lock;
    bool                       m_deleted;
    mutable unsigned int       m_last_active_time;
public:
    time_t getLastActiveTime() const {return BASE_TIME + (time_t)m_last_active_time;}
    bool dirty() {return m_dirty.hasDirty();}
    void setDirty(int index) {m_dirty.set(index);}
    bool isDirty(int index) {return m_dirty[index];}
    void resetDirty(int index = -1) {
        m_dirty.reset(index);
    }
    void clear(const TableConfig* table_config);
    inline bool isDeleted() {return m_deleted;}
    inline void   setDeleted() {m_deleted = true;}
    int init(const TableConfig* table_config);
    int create(const TableConfig* table_config,  ...);
    template<typename T>
    T get(const TableConfig* config, int index) const;
    template<typename T>
    T set(const TableConfig* config, int index, T value);
    template<typename T>
    T add(const TableConfig* config, int index, T val);
    template<typename T>
    T dec(const TableConfig* config, int index, T val);
	
    ItemValue add(const TableConfig* config, int col, ItemValue* val);
    ItemValue dec(const TableConfig* config, int col, ItemValue* val);
    ItemValue set(const TableConfig* config, int col, ItemValue* val);
    
    ItemValue getItem(const TableConfig* config, int col) const;
	bool setItem(const TableConfig* config, int index, ItemValue * item )
	{
        m_last_active_time = time(NULL) - BASE_TIME;
		if(item->isString())
		{
            m_item_lock.lock();
            std::string* str = (std::string*)(value + config->index[index]);
			*str = item->str.c_str();
            m_item_lock.unlock();
		}
		else
		{
			switch (config->types[index])
				{
				case OBJECT_INT32:
					{
						__sync_lock_test_and_set((int32_t*)(value + config->index[index]), (int32_t)item->get<int32_t>());
						break;
					}
				case OBJECT_INT64:
					{
						__sync_lock_test_and_set((int64_t*)(value + config->index[index]), (int64_t)item->get<int64_t>());
						break;
					}
				case OBJECT_UINT64:
					{
						__sync_lock_test_and_set((uint64_t*)(value + config->index[index]), (uint64_t)item->get<uint64_t>());
						break;
					}
				case OBJECT_UINT32:
					{
						__sync_lock_test_and_set((uint32_t*)(value + config->index[index]), (uint32_t)item->get<uint32_t>());
						break;
					}
                case OBJECT_BOOL:
				case OBJECT_INT8:
					{
						__sync_lock_test_and_set((int8_t*)(value + (int)config->index[index]), (int8_t)item->get<int8_t>());
						break;
					}
				case OBJECT_UINT8:
					{
						__sync_lock_test_and_set((uint8_t*)(value + (int)config->index[index]), (uint8_t)item->get<uint8_t>());
						break;
					}
				case OBJECT_INT16:
					{
						__sync_lock_test_and_set((int16_t*)(value + config->index[index]), (int16_t)item->get<int16_t>());
						break;
					}
				case OBJECT_UINT16:
					{
						__sync_lock_test_and_set((uint16_t*)(value + config->index[index]), (uint16_t)item->get<uint16_t>());
						break;
					}


				case OBJECT_FLOAT:
					{
						uint32_t tmp;
						float val = (float)item->get<float>();
						memcpy(&tmp, &val, sizeof(tmp));
						__sync_lock_test_and_set((uint32_t*)(value + config->index[index]), tmp);
						break;
					}

				case OBJECT_DOUBLE:
					{
						uint64_t tmp;
						double val = (double)item->get<double>();
						memcpy(&tmp, &val, sizeof(tmp));
						__sync_lock_test_and_set((uint64_t*)(value + config->index[index]), tmp);
						break;
					}
				default: break;
			}
			
		}
		m_dirty.set(index);
		return true;
	}
	bool setItems(const TableConfig* config, RowItemValue * input )
	{
		RowItemValue::iterator iter;
		for(iter = input->begin();iter!=input->end();++iter)
		{
			if(setItem(config, iter->first, &(iter->second)) == false)
				return false;
		}
		return true;
	}
	bool setItems(const TableConfig* config, RowItemValue2 * input )
	{
		RowItemValue2::iterator iter;
		for(iter = input->begin();iter!=input->end();++iter)
		{
			if(setItem(config, iter->first, &(iter->second)) == false)
				return false;
		}
		return true;
	}
	static TableItem_t max(const TableConfig* config);
	static TableItem_t min(const TableConfig* config);
public:
    void lock() {
        m_lock.lock();    
    }
    void unlock() {
        m_lock.unlock();
    }
    void trigger_lock() {
        m_trigger_lock.lock();    
    }
    void trigger_unlock() {
        m_trigger_lock.unlock();
    }
public:
    bool parseFromArray(const TableConfig* config, char* buffer, int len);
    int serializeToArray(const TableConfig* config, char* buffer, int len, bool dirty = false, bool only_primary = false);
    int getSerializeSize(const TableConfig* config, bool dirty = false, bool only_primary = false);
protected:
    void destroy();
};
// for template
template<> 
std::string TableItem_t::get<std::string>(const TableConfig* config, int index) const;
template<> 
const char* TableItem_t::get<const char*>(const TableConfig* config, int index) const;
template<> 
std::string TableItem_t::set<std::string>(const TableConfig* config, int index, std::string item);
template<> 
const char* TableItem_t::set<const char*>(const TableConfig* config, int index, const char* item);
template<>
std::string TableItem_t::add<std::string>(const TableConfig* config, int index, std::string item);
template<>
const char* TableItem_t::add<const char*>(const TableConfig* config, int index, const char* item);
template<>
std::string TableItem_t::dec<std::string>(const TableConfig* config, int index, std::string item);
template<>
const char* TableItem_t::dec<const char*>(const TableConfig* config, int index, const char* item);


template<typename T>
T TableItem_t::get(const TableConfig* config, int index) const {
    m_last_active_time = time(NULL) - BASE_TIME;
    switch (config->types[index])
    {
        case OBJECT_INT32:
            {
                int32_t tmp;
                __sync_lock_test_and_set(&tmp, *(int32_t*)(value + config->index[index]));
                return (T)tmp;
            }
        case OBJECT_INT64:
            {
                int64_t tmp;
                __sync_lock_test_and_set(&tmp, *(int64_t*)(value + config->index[index]));
                return (T)tmp;
            }
        case OBJECT_UINT64:
            {
                uint64_t tmp;
                __sync_lock_test_and_set(&tmp, *(uint64_t*)(value + config->index[index]));
                return (T)tmp;
            }
        case OBJECT_UINT32:
            {
                uint32_t tmp;
                __sync_lock_test_and_set(&tmp, *(uint32_t*)(value + config->index[index]));
                return (T)tmp;
            }
        case OBJECT_BOOL:
        case OBJECT_INT8:
            {
                int8_t tmp;
                __sync_lock_test_and_set(&tmp, *(int8_t*)(value + config->index[index]));
                return (T)tmp;
            }
        case OBJECT_UINT8:
            {
                uint8_t tmp;
                __sync_lock_test_and_set(&tmp, *(uint8_t*)(value + config->index[index]));
                return (T)tmp;
            }
        case OBJECT_INT16:
            {
                int16_t tmp;
                __sync_lock_test_and_set(&tmp, *(int16_t*)(value + config->index[index]));
                return (T)tmp;
            }
        case OBJECT_UINT16:
            {
                uint16_t tmp;
                __sync_lock_test_and_set(&tmp, *(uint16_t*)(value + config->index[index]));
                return (T)tmp;
            }


        case OBJECT_FLOAT:
            {
                uint32_t tmp;
                __sync_lock_test_and_set(&tmp, *(uint32_t*)(value + config->index[index]));
                float temp;
                memcpy(&temp, &tmp, sizeof(tmp));
                return (T)temp;
            }

        case OBJECT_DOUBLE:
            {
                uint64_t tmp;
                __sync_lock_test_and_set(&tmp, *(uint64_t*)(value + config->index[index]));
                double temp;
                memcpy(&temp, &tmp, sizeof(tmp));
                return (T)temp;
            }
        default: return T();
    }
}


template<typename T>
T TableItem_t::set(const TableConfig* config, int index, T item) {
    m_last_active_time = time(NULL) - BASE_TIME;
    T t;
    switch (config->types[index])
    {
        case OBJECT_INT32:
            {
                t = __sync_lock_test_and_set((int32_t*)(value + config->index[index]), (int32_t)item);
                break;
            }
        case OBJECT_INT64:
            {
                t = __sync_lock_test_and_set((int64_t*)(value + config->index[index]), (int64_t)item);
                break;
            }
        case OBJECT_UINT64:
            {
                t = __sync_lock_test_and_set((uint64_t*)(value + config->index[index]), (uint64_t)item);
                break;
            }
        case OBJECT_UINT32:
            {
                t = __sync_lock_test_and_set((uint32_t*)(value + config->index[index]), (uint32_t)item);
                break;
            }
        case OBJECT_BOOL:
        case OBJECT_INT8:
            {
                t = __sync_lock_test_and_set((int8_t*)(value + (int)config->index[index]), (int8_t)item);
                break;
            }
        case OBJECT_UINT8:
            {
                t = __sync_lock_test_and_set((uint8_t*)(value + (int)config->index[index]), (uint8_t)item);
                break;
            }
        case OBJECT_INT16:
            {
                t = __sync_lock_test_and_set((int16_t*)(value + config->index[index]), (int16_t)item);
                break;
            }
        case OBJECT_UINT16:
            {
                t = __sync_lock_test_and_set((uint16_t*)(value + config->index[index]), (uint16_t)item);
                break;
            }


        case OBJECT_FLOAT:
            {
                uint32_t tmp;
                float val = (float)item;
                memcpy(&tmp, &val, sizeof(tmp));
                uint32_t temp = __sync_lock_test_and_set((uint32_t*)(value + config->index[index]), tmp);
                memcpy(&val, &temp, sizeof(temp));
                t = val;
                break;
            }

        case OBJECT_DOUBLE:
            {
                uint64_t tmp;
                double val = (double)item;
                memcpy(&tmp, &val, sizeof(tmp));
                uint64_t temp = __sync_lock_test_and_set((uint64_t*)(value + config->index[index]), tmp);
                memcpy(&val, &temp, sizeof(temp));
                t = val;
                break;
            }
        default: break;
    }
    m_dirty.set(index);
    return t;
}


template<typename T>
T TableItem_t::add(const TableConfig* config, int index, T item) {
    m_last_active_time = time(NULL) - BASE_TIME;
    if (item == 0) {
        return get<T>(config, index);
    }
    T t = 0;
    switch (config->types[index])
    {
        case OBJECT_INT32:
            {
                t = __sync_add_and_fetch((int32_t*)(value + config->index[index]), (int32_t)item);
                break;
            }
        case OBJECT_INT64:
            {
                t = __sync_add_and_fetch((int64_t*)(value + config->index[index]), (int64_t)item);
                break;
            }
        case OBJECT_UINT64:
            {
                t = __sync_add_and_fetch((uint64_t*)(value + config->index[index]), (uint64_t)item);
                break;
            }
        case OBJECT_UINT32:
            {
                t = __sync_add_and_fetch((uint32_t*)(value + config->index[index]), (uint32_t)item);
                break;
            }
        case OBJECT_BOOL:
        case OBJECT_INT8:
            {
                t = __sync_add_and_fetch((int8_t*)(value + (int)config->index[index]), (int8_t)item);
                break;
            }
        case OBJECT_UINT8:
            {
                t = __sync_add_and_fetch((uint8_t*)(value + (int)config->index[index]), (uint8_t)item);
                break;
            }
        case OBJECT_INT16:
            {
                t = __sync_add_and_fetch((int16_t*)(value + config->index[index]), (int16_t)item);
                break;
            }
        case OBJECT_UINT16:
            {
                t = __sync_add_and_fetch((uint16_t*)(value + config->index[index]), (uint16_t)item);
                break;
            }


        case OBJECT_FLOAT:
            {
                uint32_t tmp;
                float val = (float)item;
                m_item_lock.lock();
                float ori = *(float*)(value + config->index[index]);
                val += ori;
                memcpy(&tmp, &val, sizeof(tmp));
                __sync_lock_test_and_set((uint32_t*)(value + config->index[index]), tmp);
                m_item_lock.unlock();
                t = val;
                break;
            }

        case OBJECT_DOUBLE:
            {
                uint64_t tmp;
                double val = (double)item;
                m_item_lock.lock();
                double ori = *(double*)(value + config->index[index]);
                val += ori;
                memcpy(&tmp, &val, sizeof(tmp));
                __sync_lock_test_and_set((uint64_t*)(value + config->index[index]), tmp);
                m_item_lock.unlock();
                t = val;
                break;
            }
        default: break;
    }
    m_dirty.set(index);
    return t;
}



template<typename T>
T TableItem_t::dec(const TableConfig* config, int index, T item) {
    if (index < 0 || index > config->column_num) { return 0;}
    m_last_active_time = time(NULL) - BASE_TIME;
    if (0 == item) {
        return 0;
    }
    T t = 0;
    switch (config->types[index])
    {
        case OBJECT_INT32:
            {
                t = __sync_sub_and_fetch((int32_t*)(value + config->index[index]), (int32_t)item);
                break;
            }
        case OBJECT_INT64:
            {
                t = __sync_sub_and_fetch((int64_t*)(value + config->index[index]), (int64_t)item);
                break;
            }
        case OBJECT_UINT64:
            {
                t = __sync_sub_and_fetch((uint64_t*)(value + config->index[index]), (uint64_t)item);
                break;
            }
         case OBJECT_UINT32:
            {
                t = __sync_sub_and_fetch((uint32_t*)(value + config->index[index]), (uint32_t)item);
                break;
            }
        case OBJECT_BOOL:
        case OBJECT_INT8:
            {
                t = __sync_sub_and_fetch((int8_t*)(value + (int)config->index[index]), (int8_t)item);
                break;
            }
        case OBJECT_UINT8:
            {
                t = __sync_sub_and_fetch((uint8_t*)(value + (int)config->index[index]), (uint8_t)item);
                break;
            }
        case OBJECT_INT16:
            {
                t = __sync_sub_and_fetch((int16_t*)(value + config->index[index]), (int16_t)item);
                break;
            }
        case OBJECT_UINT16:
            {
                t = __sync_sub_and_fetch((uint16_t*)(value + config->index[index]), (uint16_t)item);
                break;
            }


        case OBJECT_FLOAT:
            {
                uint32_t tmp;
                float val = (float)item;
                m_item_lock.lock();
                float ori = *(float*)(value + config->index[index]);
                val = ori - val;
                memcpy(&tmp, &val, sizeof(tmp));
                __sync_lock_test_and_set((uint32_t*)(value + config->index[index]), tmp);
                m_item_lock.unlock();
                t = val;
                break;
            }

        case OBJECT_DOUBLE:
            {
                uint64_t tmp;
                double val = (double)item;
                m_item_lock.lock();
                double ori = *(double*)(value + config->index[index]);
                val = ori - val;
                memcpy(&tmp, &val, sizeof(tmp));
                __sync_lock_test_and_set((uint64_t*)(value + config->index[index]), tmp);
                m_item_lock.unlock();
                t = val;
                break;
            }
        default: break;
    }
    m_dirty.set(index);
    return t;
}


#endif

