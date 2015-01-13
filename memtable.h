#ifndef _MEM_TABLE_H_
#define _MEM_TABLE_H_
#include <string>
#include <vector>

#include "atomichashmap.h"
#include "item_array.h"
#include "table.h"
#include "config.h"
#include "atomictimer.h"

namespace tinytable {

class MemTable {
public:
    MemTable();
    virtual ~MemTable();
public:
    int  addTable(SingleTable* table);
    int  addTable(TableConfig* config);
    int  addTable(const char* name, int column, char* types, std::set<short>& primary, std::string* names = NULL, int id = -1);
    int  addTable(const char* xml_path, const char* xml_file);
    int  init(const char* id_path, const char* id_file, const char* table_path, bool enable_trigger = true);
public:
    // del
    int  delTable(TableName table);
    // get interface
    SingleTable* getTable(TableName& name);
    SingleTable* getTable(std::string name);
public:
    virtual SingleTable::real_iterator queue_begin(TableName table);
    virtual SingleTable::real_iterator queue_end(TableName table);
    virtual SingleTable::real_iterator queue_begin(TableName table, RowItemValue input);
    virtual SingleTable::real_iterator queue_end(TableName table, RowItemValue input);
    virtual SingleTable::const_iterator begin(TableName table);
    SingleTable::const_iterator begin(SingleTable* table);
    virtual SingleTable::const_iterator end(TableName table);
    SingleTable::const_iterator end(SingleTable* table);
    // this is only used for cluster
    virtual SingleTable::const_iterator begin(TableName table, RowItemValue input);
    SingleTable::const_iterator begin(SingleTable* table, RowItemValue* input);
    virtual SingleTable::const_iterator end(TableName table, RowItemValue input);
    SingleTable::const_iterator end(SingleTable* table, RowItemValue* input);
    // this is only used for sort
    virtual SingleTable::real_iterator real_begin(TableName table, TableIndex index); 
    SingleTable::real_iterator real_begin(SingleTable* table, TableIndex* index); 
    virtual SingleTable::real_iterator real_end(TableName table, TableIndex index); 
    SingleTable::real_iterator real_end(SingleTable* table, TableIndex* index); 
    virtual SingleTable::real_iterator real_find(TableName table, TableIndex index, int rank);

    virtual SingleTable::dump_iterator dump_begin(TableName table, TableIndex index, int idx); 
    SingleTable::dump_iterator dump_begin(SingleTable* table, TableIndex* index, int idx); 
    virtual SingleTable::dump_iterator dump_end(TableName table, TableIndex index, int idx); 
    SingleTable::dump_iterator dump_end(SingleTable* table, TableIndex* index, int idx); 
    virtual int           getDumpIdx(TableName table, TableIndex index);
    virtual SingleTable::const_iterator find(TableName table, RowItemValue value, char type = OPS_LOCAL);
    virtual SingleTable::const_iterator find(TableName* table, RowItemValue* value, char type = OPS_LOCAL);
    SingleTable::const_iterator find(SingleTable* table, RowItemValue* value, char type = OPS_LOCAL);
    virtual SingleTable::const_iterator find(TableName table, RowItemValue value1, RowItemValue value2, char type = OPS_LOCAL);
    virtual SingleTable::const_iterator find(TableName* table, RowItemValue* value1, RowItemValue* value2, char type = OPS_LOCAL);
    SingleTable::const_iterator find(SingleTable* table, RowItemValue* value1, RowItemValue* value2, char type = OPS_LOCAL);
    virtual int   size(TableName name);
    int   size(SingleTable* table);
    virtual int   size(TableName name, RowItemValue value);
    int   size(SingleTable* table, RowItemValue* value);
    virtual void   clear(TableName name, char type = OPS_DEFAULT);
    void   clear(SingleTable* table, char type = OPS_DEFAULT);
    virtual void   clear(TableName name, RowItemValue value, char type = OPS_DEFAULT);
    void   clear(SingleTable* table, RowItemValue* value, char type = OPS_DEFAULT);
public:
    virtual bool del(TableName table, RowItemValue input, char type = OPS_LOCAL);  
    bool del(SingleTable* table, RowItemValue* input, char type = OPS_LOCAL);  
    virtual bool del(TableName table, RowItemValue input1, RowItemValue input2, char type = OPS_LOCAL);   
    bool del(SingleTable* table, RowItemValue* input1, RowItemValue* input2, char type = OPS_LOCAL);   
    virtual ItemValue get(TableName table, RowItemValue input, int col, char type = OPS_LOCAL);
    ItemValue get(SingleTable* table, RowItemValue* input, int col, char type = OPS_LOCAL);
    virtual ItemValue get(TableName table, RowItemValue input1, RowItemValue input2, int col, char type = OPS_LOCAL);
    ItemValue get(SingleTable* table, RowItemValue* input1, RowItemValue* input2, int col, char type = OPS_LOCAL);
    virtual int set(TableName table, RowItemValue input, RowItemValue val, char type = OPS_LOCAL);
    int set(SingleTable* table, RowItemValue* input, RowItemValue* val, char type = OPS_LOCAL);
    int set(SingleTable* table, TinyTableItem* item, RowItemValue* val, char type = OPS_LOCAL);
    virtual int set(TableName table, RowItemValue input1, RowItemValue input2, RowItemValue val, char type = OPS_LOCAL);
    int set(SingleTable* table, RowItemValue* input1, RowItemValue* input2, RowItemValue* val, char type = OPS_LOCAL);
    virtual int set(TableName table, RowItemValue input1, RowItemValue input2, int col, ItemValue val, ItemValue* old = NULL, char type = OPS_LOCAL);
    int set(SingleTable* table, RowItemValue* input1, RowItemValue* input2, int col, ItemValue* val, ItemValue* old = NULL, char type = OPS_LOCAL);
    int set(SingleTable* table, TinyTableItem* item, int col, ItemValue* val, ItemValue* old = NULL, char type = OPS_LOCAL);
    virtual int set(TableName table, RowItemValue input, int col, ItemValue val, ItemValue* old = NULL, char type = OPS_LOCAL);
    int set(SingleTable* table, RowItemValue* input, int col, ItemValue* val, ItemValue* old = NULL, char type = OPS_LOCAL);
    virtual int add(TableName table, RowItemValue input1, RowItemValue input2, int col, ItemValue val, ItemValue* cur = NULL, char type = OPS_LOCAL);
    int add(SingleTable* table, RowItemValue* input1, RowItemValue* input2, int col, ItemValue* val, ItemValue* cur = NULL, char type = OPS_LOCAL);
    int add(SingleTable* table, TinyTableItem* item, int col, ItemValue* val, ItemValue* cur = NULL, char type = OPS_LOCAL);
    virtual int add(TableName table, RowItemValue input, int col, ItemValue val, ItemValue* cur = NULL, char type = OPS_LOCAL);
    int add(SingleTable* table, RowItemValue* input, int col, ItemValue* val, ItemValue* cur = NULL, char type = OPS_LOCAL);
    virtual int dec(TableName table, RowItemValue input1, RowItemValue input2, int col, ItemValue val, ItemValue* cur = NULL, char type = OPS_LOCAL);
    int dec(SingleTable* table, RowItemValue* input1, RowItemValue* input2, int col, ItemValue* val, ItemValue* cur = NULL, char type = OPS_LOCAL);
    int dec(SingleTable* table, TinyTableItem* item, int col, ItemValue* val, ItemValue* cur = NULL, char type = OPS_LOCAL);
    virtual int dec(TableName table, RowItemValue input, int col, ItemValue val, ItemValue* cur = NULL, char type = OPS_LOCAL);
    int dec(SingleTable* table, RowItemValue* input, int col, ItemValue* val, ItemValue* cur = NULL, char type = OPS_LOCAL);
public:
    virtual bool insert(TableName table, ...);
    virtual long long insertSerial(TableName table, ...);
    virtual bool dump_sort(TableName table, TableIndex index);
    bool dump_sort(SingleTable* table, TableIndex* index);
    virtual int  rank(TableName table, RowItemValue unique, TableIndex sort);
    int  rank(SingleTable* table, RowItemValue* unique, TableIndex* sort);
public:
    int expire(TableName table);   
    int expire();
    bool setChangedCallbackFunc(TableName table, itemchanged_callback* func, void* arg);
    bool setDeletedCallbackFunc(TableName table, itemdeleted_callback* func, void* arg);
    bool setInsertCallbackFunc(TableName table, iteminsert_callback* func, void* arg);
    TableIDConfig* getIDConfig() { return _id_config;}
public:
    static bool addTimer(uint64_t us_time, timer_callback func, uint64_t interval = 0, int round = 1, uint64_t key = -1, uint64_t* newKey = NULL);
    static bool addFixedTimer(int wday, int hour, int min, int sec, timer_callback func, uint64_t interval = 0, int round = 1, uint64_t key = -1, uint64_t* newKey = NULL);
    static bool addHourlyTimer(int min, int sec, timer_callback func, uint64_t key = -1, uint64_t* newKey = NULL);
    static bool addDailyTimer(int hour, int min, int sec, timer_callback func, uint64_t key = -1, uint64_t* newKey = NULL);
    static bool addWeeklyTimer(int wday, int hour, int min, int sec, timer_callback func, uint64_t key = -1, uint64_t* newKey = NULL);
    static bool delTimer(uint64_t key);
public:
    static bool  tinytable_init(bool addlog, int ticks = 20000, runfunc* func = NULL, void* func_arg = NULL);
    static bool  tinytable_destroy();
    static AtomicTimer*  global_timer;
protected:
    virtual void initRealtimerTrigger(SingleTable* table, TinyTableItem* item) {}
    int  getTableID(std::string table);
protected:
    SingleTable**    _tables;
    TableIDConfig*                   _id_config;
};


}

#endif

