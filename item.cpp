#include "item.h"
#include <string>

typedef std::string ItemString;

TableItem_t TableItem_t::max(const TableConfig* config) {
    TableItem_t t;
    if (0 != t.init(config)) {
        throw;
    }
    char MAXSTR[10] = "";
    MAXSTR[0] = 0xFF;
    MAXSTR[1] = 0;
    // set max
    for (int i = 0; i < config->column_num; ++i) {
        switch (config->types[i])
        {
            case OBJECT_INT32: t.set<int32_t>(config, i, std::numeric_limits<int32_t>::max());break;
            case OBJECT_STRING: t.set<std::string>(config, i, MAXSTR);break;
            case OBJECT_INT64: t.set<int64_t>(config, i, std::numeric_limits<int64_t>::max());break;
            case OBJECT_UINT64: t.set<uint64_t>(config, i, std::numeric_limits<uint64_t>::max());break;
            case OBJECT_UINT32: t.set<uint32_t>(config, i, std::numeric_limits<uint32_t>::max());break;
            case OBJECT_UINT8: t.set<uint8_t>(config, i, std::numeric_limits<uint8_t>::max());break;
            case OBJECT_INT8: t.set<int8_t>(config, i, std::numeric_limits<int8_t>::max());break;
            case OBJECT_UINT16: t.set<uint16_t>(config, i, std::numeric_limits<uint16_t>::max());break;
            case OBJECT_INT16: t.set<int16_t>(config, i, std::numeric_limits<int16_t>::max());break;
            case OBJECT_FLOAT: t.set<float>(config, i, std::numeric_limits<float>::max());break;
            case OBJECT_DOUBLE: t.set<double>(config, i, std::numeric_limits<double>::max());break;
        }
    }
    return t;
}

TableItem_t TableItem_t::min(const TableConfig* config) {
    TableItem_t t;
    if (0 != t.init(config)) {
        throw;
    }
    // set max
    for (int i = 0; i < config->column_num; ++i) {
        switch (config->types[i])
        {
            case OBJECT_INT32: t.set<int32_t>(config, i, std::numeric_limits<int32_t>::min());break;
            case OBJECT_STRING: t.set<std::string>(config, i, "");break;
            case OBJECT_INT64: t.set<int64_t>(config, i, std::numeric_limits<int64_t>::min());break;
            case OBJECT_UINT64: t.set<uint64_t>(config, i, std::numeric_limits<uint64_t>::min());break;
            case OBJECT_UINT32: t.set<uint32_t>(config, i, std::numeric_limits<uint32_t>::min());break;
            case OBJECT_UINT8: t.set<uint8_t>(config, i, std::numeric_limits<uint8_t>::min());break;
            case OBJECT_INT8: t.set<int8_t>(config, i, std::numeric_limits<int8_t>::min());break;
            case OBJECT_UINT16: t.set<uint16_t>(config, i, std::numeric_limits<uint16_t>::min());break;
            case OBJECT_INT16: t.set<int16_t>(config, i, std::numeric_limits<int16_t>::min());break;
            case OBJECT_FLOAT: t.set<float>(config, i, std::numeric_limits<float>::min());break;
            case OBJECT_DOUBLE: t.set<double>(config, i, std::numeric_limits<double>::min());break;
        }
    }
    return t;
}


ItemValue TableItem_t::add(const TableConfig* config, int col, ItemValue* val) {
        switch (config->types[col])
        {
            case OBJECT_INT32: return add<int32_t>(config, col, val->get<int32_t>());
            case OBJECT_STRING: return add<std::string>(config, col, val->get<std::string>());
            case OBJECT_INT64: return add<int64_t>(config, col, val->get<int64_t>());
            case OBJECT_UINT64: return add<uint64_t>(config, col, val->get<uint64_t>());
            case OBJECT_UINT32: return add<uint32_t>(config, col, val->get<uint32_t>());
            case OBJECT_BOOL: return add<bool>(config, col, val->get<bool>());
            case OBJECT_UINT8: return add<uint8_t>(config, col, val->get<uint8_t>());
            case OBJECT_INT8: return add<int8_t>(config, col, val->get<int8_t>());
            case OBJECT_UINT16: return add<uint16_t>(config, col, val->get<uint16_t>());
            case OBJECT_INT16: return add<int16_t>(config, col, val->get<int16_t>());
            case OBJECT_FLOAT: return add<float>(config, col, val->get<float>());
            case OBJECT_DOUBLE: return add<double>(config, col, val->get<double>());
        }   
    return ItemValue();
}

ItemValue TableItem_t::set(const TableConfig* config, int col, ItemValue* val) {
        switch (config->types[col])
        {
            case OBJECT_INT32: return set<int32_t>(config, col, val->get<int32_t>());
            case OBJECT_STRING: return set<std::string>(config, col, val->get<std::string>());
            case OBJECT_INT64: return set<int64_t>(config, col, val->get<int64_t>());
            case OBJECT_UINT64: return set<uint64_t>(config, col, val->get<uint64_t>());
            case OBJECT_UINT32: return set<uint32_t>(config, col, val->get<uint32_t>());
            case OBJECT_BOOL: return set<bool>(config, col, val->get<bool>());
            case OBJECT_UINT8: return set<uint8_t>(config, col, val->get<uint8_t>());
            case OBJECT_INT8: return set<int8_t>(config, col, val->get<int8_t>());
            case OBJECT_UINT16: return set<uint16_t>(config, col, val->get<uint16_t>());
            case OBJECT_INT16: return set<int16_t>(config, col, val->get<int16_t>());
            case OBJECT_FLOAT: return set<float>(config, col, val->get<float>());
            case OBJECT_DOUBLE: return set<double>(config, col, val->get<double>());
        }

    return ItemValue();
}

ItemValue TableItem_t::dec(const TableConfig* config, int col, ItemValue* val) {
        switch (config->types[col])
        {
            case OBJECT_INT32: return dec<int32_t>(config, col, val->get<int32_t>());
            case OBJECT_STRING: return dec<std::string>(config, col, val->get<std::string>());
            case OBJECT_INT64: return dec<int64_t>(config, col, val->get<int64_t>());
            case OBJECT_UINT64: return dec<uint64_t>(config, col, val->get<uint64_t>());
            case OBJECT_UINT32: return dec<uint32_t>(config, col, val->get<uint32_t>());
            case OBJECT_BOOL: return dec<bool>(config, col, val->get<bool>());
            case OBJECT_UINT8: return dec<uint8_t>(config, col, val->get<uint8_t>());
            case OBJECT_INT8: return dec<int8_t>(config, col, val->get<int8_t>());
            case OBJECT_UINT16: return dec<uint16_t>(config, col, val->get<uint16_t>());
            case OBJECT_INT16: return dec<int16_t>(config, col, val->get<int16_t>());
            case OBJECT_FLOAT: return dec<float>(config, col, val->get<float>());
            case OBJECT_DOUBLE: return dec<double>(config, col, val->get<double>());
        }   
    return ItemValue();
}


template<> 
std::string TableItem_t::get<std::string>(const TableConfig* config, int index) const {
    m_last_active_time = time(NULL) - BASE_TIME;
    if (config->types[index] != OBJECT_STRING) {
        return STR_NULL;
    }
    m_item_lock.lock();
    std::string* ptr = (std::string*)(value + config->index[index]);
    std::string ret = ptr->c_str();
    m_item_lock.unlock();
    return ret;
}
template<>
const char* TableItem_t::get<const char*>(const TableConfig* config, int index) const {
    m_last_active_time = time(NULL) - BASE_TIME;
    if (config->types[index] != OBJECT_STRING) {
        return STR_CONST_NULL;
    }
    m_item_lock.lock();
    std::string* ptr = (std::string*)(value + config->index[index]);
    m_item_lock.unlock();
    return ptr->c_str();
}

ItemValue TableItem_t::getItem(const TableConfig* config, int col) const {
    if (col >= config->column_num) {
        return ItemValue();
    }
    char type = config->types[col];
    switch (type)
    {
        case OBJECT_INT32: return get<int32_t>(config, col);
        case OBJECT_STRING: return get<std::string>(config, col);
        case OBJECT_INT64: return get<int64_t>(config, col);
        case OBJECT_UINT64: return get<uint64_t>(config, col);
        case OBJECT_UINT32: return get<uint32_t>(config, col);
        case OBJECT_BOOL: return get<bool>(config, col);
        case OBJECT_UINT8: return get<uint8_t>(config, col);
        case OBJECT_INT8: return get<int8_t>(config, col);
        case OBJECT_UINT16: return get<uint16_t>(config, col);
        case OBJECT_INT16: return get<int16_t>(config, col);
        case OBJECT_FLOAT: return get<float>(config, col);
        case OBJECT_DOUBLE: return get<double>(config, col);
        default: TT_WARN_LOG("col %d type error: %d", col, config->types[col]); return ItemValue();
    }
}


template<>
std::string TableItem_t::set<std::string>(const TableConfig* config, int index, std::string item) {
    if (index < 0 || index > config->column_num || config->types[index] != OBJECT_STRING) {
        return get<std::string>(config, index);
    }
    if (config->limits != NULL && item.length() > config->limits[index]) {
        return get<std::string>(config, index);
    }
    m_last_active_time = time(NULL) - BASE_TIME;
    m_item_lock.lock();
    std::string* str = (std::string*)(value + config->index[index]);
    std::string old = str->c_str();
    *str = item.c_str();
    // enqueue old string
    m_dirty.set(index);
    m_item_lock.unlock();
    return old;
}

template<>
const char* TableItem_t::set<const char*>(const TableConfig* config, int index, const char* item) {
    if (index < 0 || index > config->column_num || config->types[index] != OBJECT_STRING) {
        return get<const char*>(config, index);
    }
    if (config->limits != NULL && strlen(item) > config->limits[index]) {
        return get<const char*>(config, index);
    }
    m_last_active_time = time(NULL) - BASE_TIME;
    m_item_lock.lock();
    std::string* str = (std::string*)(value + config->index[index]);
    *str = item;
    // enqueue old string
    m_dirty.set(index);
    m_item_lock.unlock();
    TT_FATAL_LOG("this func is not allowed to be used!!!!!");
    return STR_CONST_NULL;
}


template<>
std::string TableItem_t::add<std::string>(const TableConfig* config, int index, std::string item) {
    m_last_active_time = time(NULL) - BASE_TIME;
    (void)index;
    (void)item;
    return "";
}

template<>
const char* TableItem_t::add<const char*>(const TableConfig* config, int index, const char* item) {
    m_last_active_time = time(NULL) - BASE_TIME;
    (void)index;
    (void)item;
    return STR_CONST_NULL;
}


template<>
std::string TableItem_t::dec<std::string>(const TableConfig* config, int index, std::string item) {
    m_last_active_time = time(NULL) - BASE_TIME;
    (void)index;
    (void)item;
    return "";
}

template<>
const char* TableItem_t::dec<const char*>(const TableConfig* config, int index, const char* item) {
    m_last_active_time = time(NULL) - BASE_TIME;
    (void)index;
    (void)item;
    return STR_CONST_NULL;
}


TableItem_t::TableItem_t(const TableConfig* config, TableItem_t item)
: value(NULL)
, m_lock()
, m_item_lock()
, m_trigger_lock()
, m_deleted(false)
, m_last_active_time(time(NULL) - BASE_TIME) {
    int items = config->column_num;
    init(config);
    for (int i = 0; i < items; i++) {
        if (config->types[i] == OBJECT_FLOAT) {
            set<float>(config, i, item.get<float>(config, i));
        } else if (config->types[i] == OBJECT_DOUBLE) {
            set<double>(config, i, item.get<double>(config, i));
        } else if (config->types[i] != OBJECT_STRING) {
            set<long long>(config, i, item.get<long long>(config, i));
        } else {
            new(value + config->index[i]) std::string(item.get<std::string>(config, i));
        }
    }
}

TableItem_t::TableItem_t()
: value(NULL)
, m_lock()
, m_item_lock()
, m_trigger_lock()
, m_deleted(false)
, m_last_active_time(time(NULL) - BASE_TIME) {
}



TableItem_t::~TableItem_t() {
    destroy();
}

void TableItem_t::destroy() {
    if (value == NULL) {
        return;
    }
    // del
    int col_num = *(unsigned short*)value;
    for (int i = 0; i < col_num; ++i) {
        std::string* ptr = (std::string*)(value + 2 + i * 8);
        ptr->~ItemString();
    }
    // destroy string buffer
    if (NULL != value) {
        delete value;
        value = NULL;
    }
}

void TableItem_t::clear(const TableConfig* config) {
    if (value == NULL) {
        return;
    }
    int columns = config->column_num;
    for (int k = 0; k < columns; k++) {
        m_dirty.reset(k);
    }   
    m_lock.unlock();
    m_item_lock.unlock();
    m_trigger_lock.unlock();
    m_deleted = false;
}

int TableItem_t::init(const TableConfig* config) {
    if (config == NULL || config->column_num <= 0) {
        return -1;
    }
    destroy();
    int columns = config->column_num;
    int len = 2;
    for (int i = 0; i < columns; ++i) {
        switch (config->types[i])
        {
            case OBJECT_INT32:
            case OBJECT_UINT32: len += 4;break;
            case OBJECT_STRING:
            case OBJECT_INT64:
            case OBJECT_DOUBLE:
            case OBJECT_UINT64: len += 8; break;
            case OBJECT_BOOL:
            case OBJECT_INT8:
            case OBJECT_UINT8:len += 1;break;
            case OBJECT_INT16:
            case OBJECT_UINT16: len += 2;break;
            case OBJECT_FLOAT: len += 4; break;
            default:len += 8;break;
        }
    }
    value = new(std::nothrow) char[len];
    if (NULL == value) {
        if (value != NULL) {
            delete value;
            value = NULL;
        }
        return -1;
    }
    memset(value, 0, sizeof(char) * len);
    // add string count
    *(unsigned short*)value = config->string_cnt;
    // init string
    for (int i = 0; i < config->column_num; i++) {
        if (config->types[i] == OBJECT_STRING) {
            new(value + config->index[i]) std::string("");
        }
    }
    if (!m_dirty.resize(columns)) {
        goto CREATEERROR;
    }
    m_last_active_time = time(NULL) - BASE_TIME;
    return 0;
CREATEERROR:
    destroy();
    return -1;
}
int TableItem_t::create(const TableConfig* config, ...) {
    if (config == NULL || config->column_num <= 0 || value == NULL) {
        TT_WARN_LOG("config or value null");
        return -1;
    }
    int columns = config->column_num;
    va_list argptr;
    va_start(argptr, config);
    for (int j = 0; j < columns; j++) {
        switch (config->types[j])
        {
            case OBJECT_INT32:
                {
                    int tmp = va_arg(argptr, int32_t);
                    memcpy(value + config->index[j], &tmp, sizeof(tmp));
                    break;
                }
            case OBJECT_STRING:
                {
                    const char* str = va_arg(argptr, char*);
                    if (config->limits != NULL && strlen(str) > config->limits[j]) {
                        return -1;
                    }
                    std::string* ptr = (std::string*)(value + config->index[j]);
                    m_item_lock.lock();
                    *ptr = str;
                    m_item_lock.unlock();
                    break;
                }
            case OBJECT_INT64:
                {
                    int64_t tmp = va_arg(argptr, long long);
                    printf("int64 %ld\n", tmp);
                    memcpy(value + config->index[j], &tmp, sizeof(tmp));
                    break;
                }
            case OBJECT_UINT32:
                {
                    unsigned int tmp = va_arg(argptr, uint32_t);
                    memcpy(value + config->index[j], &tmp, sizeof(tmp));
                    break;
                }

            case OBJECT_UINT64:
                {
                    uint64_t tmp = va_arg(argptr, unsigned long long);
                    printf("uint64 %ld\n", tmp);
                    memcpy(value + config->index[j], &tmp, sizeof(tmp));
                    break;
                }

            case OBJECT_BOOL:
                {
                    char tmp = va_arg(argptr, int);
                    memcpy(value + config->index[j], &tmp, sizeof(tmp));
                    break;
                }
            case OBJECT_INT8:
                {
                    char tmp = va_arg(argptr, int);
                    memcpy(value + config->index[j], &tmp, sizeof(tmp));
                    break;
                }
            case OBJECT_UINT8:
                {
                    unsigned char tmp = va_arg(argptr, unsigned int);
                    memcpy(value + config->index[j], &tmp, sizeof(tmp));
                    break;
                }
            case OBJECT_INT16:
                {
                    short tmp  = va_arg(argptr, int);
                    memcpy(value + config->index[j], &tmp, sizeof(tmp));
                    break;
                }
            case OBJECT_UINT16:
                {
                    unsigned short tmp = va_arg(argptr, unsigned int);
                    memcpy(value + config->index[j], &tmp, sizeof(tmp));
                    break;
                }
            case OBJECT_FLOAT:
                {
                    float tmp = va_arg(argptr, double);
                    printf("float %.2f\n", tmp);
                    memcpy(value + config->index[j], &tmp, sizeof(tmp));
                    break;
                }
            case OBJECT_DOUBLE:
                {
                    double tmp = va_arg(argptr, double);
                    printf("double %.2lf\n", tmp);
                    memcpy(value + config->index[j], &tmp, sizeof(tmp));
                    break;
                }


            default: goto ERROR;
        }
    }
    va_end(argptr);
    for (int k = 0; k < columns; k++) {
        m_dirty.set(k);
    }
    m_last_active_time = time(NULL) - BASE_TIME;
    return 0;
ERROR:
    return -1;
}

bool TableItem_t::parseFromArray(const TableConfig* config, char* buffer, int len) {
#define CHECK_POS_LEN(offset) \
        if (pos + offset > len) { \
            return false; \
        }

        if (buffer == NULL ||len <= 0){
            return false;
        }
        if (value == NULL || config == NULL) {
            return false;
        }
        int pos = 0;
        unsigned short index = 0;
        unsigned char  type = 0;
        while (pos < len) {
            CHECK_POS_LEN(sizeof(index));
            index = *(unsigned short*)(buffer + pos);
            pos += sizeof(index);
            CHECK_POS_LEN(sizeof(type));
            type = buffer[pos];
            pos += sizeof(type);
            if (type == OBJECT_NULL) {continue;}
            // check if equal
            if (config->types[index] != type) {
                return false;
            }
            uint64_t val = 0;
            switch (type)
            {
                case OBJECT_INT32: CHECK_POS_LEN(4);val = *(int32_t*)(buffer + pos); pos += 4; set<int32_t>(config, index, val); break;
                case OBJECT_STRING: 
                    {
                        int  str_len = 0;
                        CHECK_POS_LEN(4);
                        str_len = *(int*)(buffer + pos);pos += sizeof(len);
                        CHECK_POS_LEN(str_len);
                        char* buf = buffer + pos;set<std::string>(config, index, buf); pos += str_len; break;
                    }
                case OBJECT_INT64: CHECK_POS_LEN(8);val = *(int64_t*)(buffer + pos); pos += 8; set<int64_t>(config, index, val); break;
                case OBJECT_UINT64: CHECK_POS_LEN(8);val = *(uint64_t*)(buffer + pos); pos += 8; set<uint64_t>(config, index, val); break;
                case OBJECT_UINT32: CHECK_POS_LEN(4); val = *(uint32_t*)(buffer + pos); pos += 4; set<uint32_t>(config, index, val); break;
                case OBJECT_BOOL: CHECK_POS_LEN(1); val = buffer[pos]; pos += 1; set<bool>(config, index, val); break;
                case OBJECT_UINT8: CHECK_POS_LEN(1); val = buffer[pos]; pos += 1; set<uint8_t>(config, index, val); break;
                case OBJECT_INT8: CHECK_POS_LEN(1); val = buffer[pos]; pos += 1; set<int8_t>(config, index, val); break;
                case OBJECT_UINT16: CHECK_POS_LEN(2); val = *(uint16_t*)(buffer + pos); pos += 2; set<uint16_t>(config, index, val); break;
                case OBJECT_INT16: CHECK_POS_LEN(2); val = *(int16_t*)(buffer + pos); pos += 2; set<int16_t>(config, index, val); break;
                case OBJECT_FLOAT: 
                {
                    CHECK_POS_LEN(4); 
                    float valf = *(float*)(buffer + pos); pos += 4; set<float>(config, index, valf); break;
                }
                case OBJECT_DOUBLE: 
                    {
                        CHECK_POS_LEN(8);double vald = *(double*)(buffer + pos); pos += 8; set<double>(config, index, vald); break;
                    }

                default: return false;
            }
        }
#undef CHECK_POS_LEN
        return true;
    }


int TableItem_t::getSerializeSize(const TableConfig* config, bool dirty, bool only_primary) {
    int len = 0;
    for (int i = 0; i < config->column_num; i++){
        if (only_primary && !config->isPrimary(i)) {continue;}
        if (dirty && !isDirty(i) && !config->isPrimary(i)) {continue;}
        switch (config->types[i])
        {
            case OBJECT_INT32: len += 7; break;
            case OBJECT_STRING: len += (7 + get<std::string>(config, i).length()); break;
            case OBJECT_INT64: len += 11; break;
            case OBJECT_UINT64: len += 11; break;
            case OBJECT_UINT32: len += 7; break;
            case OBJECT_BOOL: len += 4; break;
            case OBJECT_UINT8: len += 4; break;
            case OBJECT_INT8: len += 4; break;
            case OBJECT_UINT16: len += 5; break;
            case OBJECT_INT16: len += 5; break;
            case OBJECT_FLOAT: len += 7; break;
            case OBJECT_DOUBLE: len += 11; break;
        }
    }
    return len;
}

int TableItem_t::serializeToArray(const TableConfig* config, char* buffer, int len, bool dirty, bool only_primary) {
#define CHECK_POS_SERIALIZE(offset) \
        if (pos + offset > len) { \
            printf("pos %d offset %d len %d\n", pos, offset, len); \
            return -1; \
        }
        if (value == NULL || config == NULL) {
            return -1;
        }
        unsigned short i = 0;
        int pos = 0;
        for (i = 0; i < config->column_num; i++){
            if (only_primary && !config->isPrimary(i)) {continue;}
            if (dirty && !isDirty(i) && !config->isPrimary(i)) {continue;}
            CHECK_POS_SERIALIZE(2);
            if (config->isPrimary(i)) {
                *(unsigned short*)(buffer+pos) = i;
            } else {
                *(unsigned short*)(buffer+pos) = (i | 0x80);
            }
            pos += sizeof(i);
            CHECK_POS_SERIALIZE(1);
            buffer[pos] = config->types[i];
            pos += 1;
            switch (config->types[i])
            {

                case OBJECT_INT32: 
                    {
                        CHECK_POS_SERIALIZE(4); *(int32_t*)(buffer + pos) = get<int32_t>(config, i); pos += 4;break;
                    }
                case OBJECT_STRING: 
                    {
                        std::string str = get<std::string>(config, i);
                        int str_len = str.length();
                        CHECK_POS_SERIALIZE(4);
                        *(int*)(buffer + pos) = str_len;
                        pos += 4;
                        CHECK_POS_SERIALIZE(str_len);
                        memcpy(buffer+pos, str.c_str(), str_len); 
                        pos += str_len;
                        break;
                    }
                case OBJECT_INT64: 
                    {
                        CHECK_POS_SERIALIZE(8);*(int64_t*)(buffer + pos) = get<int64_t>(config, i); pos += 8;break;
                    }
                case OBJECT_UINT64: 
                    {
                        CHECK_POS_SERIALIZE(8);*(uint64_t*)(buffer + pos) = get<uint64_t>(config, i); pos += 8;break;
                    }
                 case OBJECT_UINT32: 
                    {
                        CHECK_POS_SERIALIZE(4); *(uint32_t*)(buffer + pos) = get<uint32_t>(config, i); pos += 4;break;
                    }                   

                case OBJECT_BOOL: 
                    {
                        CHECK_POS_SERIALIZE(1);
                        buffer[pos] = get<bool>(config, i);
                        pos += 1;
                        break;
                    }
                case OBJECT_UINT8: 
                    {
                        CHECK_POS_SERIALIZE(1); 
                        buffer[pos] = get<uint8_t>(config, i); 
                        pos += 1;
                        break;
                    }
                case OBJECT_INT8: 
                    {
                        CHECK_POS_SERIALIZE(1); buffer[pos] = get<int8_t>(config, i); pos += 1;break;
                    }
                case OBJECT_UINT16: 
                    {
                        CHECK_POS_SERIALIZE(2); *(uint16_t*)(buffer + pos) = get<uint16_t>(config, i); pos += 2;break;
                    }
                case OBJECT_INT16: 
                    {
                        CHECK_POS_SERIALIZE(2); *(int16_t*)(buffer + pos) = get<int16_t>(config, i); pos += 2;break;
                    }
                case OBJECT_FLOAT: 
                    {
                        CHECK_POS_SERIALIZE(4); *(float*)(buffer + pos) = get<float>(config, i); pos += 4;break;
                    }
                case OBJECT_DOUBLE: 
                    {
                        CHECK_POS_SERIALIZE(8);*(double*)(buffer + pos) = get<double>(config, i); pos += 8;break;
                    }
                default: return -1;
            }
        }
#undef CHECK_POS_SERIALIZE
        return pos;
    }




