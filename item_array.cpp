#include <sstream>
#include "item_array.h"
#include "sign.h"

template<>
std::string ItemValue::get<std::string>() const {
    return str.c_str();
}

template<>
const char* ItemValue::get<const char*>() const {
    return str.c_str();
}

template<>
void ItemValue::set<std::string>(std::string input) {
    str = input.c_str();
}

template<>
void ItemValue::set<const char*>(const char* input) {
    str = input;
}



bool ItemValue::operator>(const ItemValue& value) const {
    if (isString() && value.isString()) {
        return (str > value.str);
    }
    if (state == OBJECT_FLOAT) {
        return (val_f > value.val_f);
    } else if (state == OBJECT_DOUBLE) {
        return (val_d > value.val_d);
    } else {
        switch (state)
        {
            case OBJECT_INT32: return get<int32_t>() > value.get<int32_t>();
            case OBJECT_INT64: return get<int64_t>() > value.get<int64_t>();
            case OBJECT_UINT64: return get<uint64_t>() > value.get<uint64_t>();
            case OBJECT_UINT32: return get<uint32_t>() > value.get<uint32_t>();
            case OBJECT_BOOL: return get<int8_t>() > value.get<int8_t>();
            case OBJECT_INT8: return get<int8_t>() > value.get<int8_t>();
            case OBJECT_UINT8: return get<uint8_t>() > value.get<uint8_t>();
            case OBJECT_INT16: return get<int16_t>() > value.get<int16_t>();
            case OBJECT_UINT16: return get<uint16_t>() > value.get<uint16_t>();
        }
    }
}

bool ItemValue::operator<(const ItemValue& value) const {
    if (isString() && value.isString()) {
        return (str < value.str);
    }
    if (state == OBJECT_FLOAT) {
        return (val_f < value.val_f);
    } else if (state == OBJECT_DOUBLE) {
        return (val_d < value.val_d);
    } else {
        switch (state)
        {
            case OBJECT_INT32: return get<int32_t>() < value.get<int32_t>();
            case OBJECT_INT64: return get<int64_t>() < value.get<int64_t>();
            case OBJECT_UINT64: return get<uint64_t>() < value.get<uint64_t>();
            case OBJECT_UINT32: return get<uint32_t>() < value.get<uint32_t>();
            case OBJECT_BOOL: return get<int8_t>() < value.get<int8_t>();
            case OBJECT_INT8: return get<int8_t>() < value.get<int8_t>();
            case OBJECT_UINT8: return get<uint8_t>() < value.get<uint8_t>();
            case OBJECT_INT16: return get<int16_t>() < value.get<int16_t>();
            case OBJECT_UINT16: return get<uint16_t>() < value.get<uint16_t>();
        }
    }
}

bool ItemValue::operator<=(const ItemValue& value) const {
    if (isString() && value.isString()) {
        return (str <= value.str);
    }
    if (state == OBJECT_FLOAT) {
        return (val_f <= value.val_f);
    } else if (state == OBJECT_DOUBLE) {
        return (val_d <= value.val_d);
    } else {
        switch (state)
        {
            case OBJECT_INT32: return get<int32_t>() <= value.get<int32_t>();
            case OBJECT_INT64: return get<int64_t>() <= value.get<int64_t>();
            case OBJECT_UINT64: return get<uint64_t>() <= value.get<uint64_t>();
            case OBJECT_UINT32: return get<uint32_t>() <= value.get<uint32_t>();
            case OBJECT_BOOL: return get<int8_t>() <= value.get<int8_t>();
            case OBJECT_INT8: return get<int8_t>() <= value.get<int8_t>();
            case OBJECT_UINT8: return get<uint8_t>() <= value.get<uint8_t>();
            case OBJECT_INT16: return get<int16_t>() <= value.get<int16_t>();
            case OBJECT_UINT16: return get<uint16_t>() <= value.get<uint16_t>();
        }
    }
}
bool ItemValue::operator>=(const ItemValue& value) const {
    if (isString() && value.isString()) {
        return (str >= value.str);
    }
    if (state == OBJECT_FLOAT) {
        return (val_f >= value.val_f);
    } else if (state == OBJECT_DOUBLE) {
        return (val_d >= value.val_d);
    } else {
        switch (state)
        {
            case OBJECT_INT32: return get<int32_t>() >= value.get<int32_t>();
            case OBJECT_INT64: return get<int64_t>() >= value.get<int64_t>();
            case OBJECT_UINT64: return get<uint64_t>() >= value.get<uint64_t>();
            case OBJECT_UINT32: return get<uint32_t>() >= value.get<uint32_t>();
            case OBJECT_BOOL: return get<int8_t>() >= value.get<int8_t>();
            case OBJECT_INT8: return get<int8_t>() >= value.get<int8_t>();
            case OBJECT_UINT8: return get<uint8_t>() >= value.get<uint8_t>();
            case OBJECT_INT16: return get<int16_t>() >= value.get<int16_t>();
            case OBJECT_UINT16: return get<uint16_t>() >= value.get<uint16_t>();
        }
    }
}


ItemValue ItemValue::operator+(const ItemValue& value) const {
    ItemValue ret;
    if (isString() && value.isString()) {
        ret = str + value.str;
    } else if (state == OBJECT_FLOAT) {
        ret = val_f + value.val_f;
    } else if (state == OBJECT_DOUBLE) {
        ret = val_d + value.val_d;
    } else {
        switch (state)
        {
            case OBJECT_INT32: ret = get<int32_t>() + value.get<int32_t>(); break;
            case OBJECT_INT64: ret = get<int64_t>() + value.get<int64_t>(); break;
            case OBJECT_UINT64: ret = get<uint64_t>() + value.get<uint64_t>(); break;
            case OBJECT_UINT32: ret = get<uint32_t>() + value.get<uint32_t>(); break;
            case OBJECT_BOOL: ret = get<int8_t>() + value.get<int8_t>(); break;
            case OBJECT_INT8: ret = get<int8_t>() + value.get<int8_t>(); break;
            case OBJECT_UINT8: ret = get<uint8_t>() + value.get<uint8_t>(); break;
            case OBJECT_INT16: ret = get<int16_t>() + value.get<int16_t>(); break;
            case OBJECT_UINT16: ret = get<uint16_t>() + value.get<uint16_t>(); break;
        }
    }
    return ret;
}
ItemValue ItemValue::operator-(const ItemValue& value) const {
    ItemValue ret;
    if (isString() && value.isString()) {
        ret = str;
    } else if (state == OBJECT_FLOAT) {
        ret = val_f - value.val_f;
    } else if (state == OBJECT_DOUBLE) {
        ret = val_d - value.val_d;
    } else {
        switch (state)
        {
            case OBJECT_INT32: ret = get<int32_t>() - value.get<int32_t>(); break;
            case OBJECT_INT64: ret = get<int64_t>() - value.get<int64_t>(); break;
            case OBJECT_UINT64: ret = get<uint64_t>() - value.get<uint64_t>(); break;
            case OBJECT_UINT32: ret = get<uint32_t>() - value.get<uint32_t>(); break;
            case OBJECT_BOOL: ret = get<int8_t>() - value.get<int8_t>(); break;
            case OBJECT_INT8: ret = get<int8_t>() - value.get<int8_t>(); break;
            case OBJECT_UINT8: ret = get<uint8_t>() - value.get<uint8_t>(); break;
            case OBJECT_INT16: ret = get<int16_t>() - value.get<int16_t>(); break;
            case OBJECT_UINT16: ret = get<uint16_t>() - value.get<uint16_t>(); break;
        }
    }
    return ret;
}

ItemValue ItemValue::operator*(int val) const {
    ItemValue ret;
    if (isString()) {
        ret = str;
    } else if (state == OBJECT_FLOAT) {
        ret = val_f * val;
    } else if (state == OBJECT_DOUBLE) {
        ret = val_d * val;
    } else {
        switch (state)
        {
            case OBJECT_INT32: ret = (int32_t)(get<int32_t>() * val); break;
            case OBJECT_INT64: ret = (int64_t)(get<int64_t>() * val); break;
            case OBJECT_UINT64: ret = (uint64_t)(get<uint64_t>() * val); break;
            case OBJECT_UINT32: ret = (uint32_t)(get<uint32_t>() * val); break;
            case OBJECT_BOOL: ret = get<int8_t>(); break;
            case OBJECT_INT8: ret = (int8_t)(get<int8_t>() * val); break;
            case OBJECT_UINT8: ret = (uint8_t)(get<uint8_t>() * val); break;
            case OBJECT_INT16: ret = (int16_t)(get<int16_t>() * val); break;
            case OBJECT_UINT16: ret = (uint16_t)(get<uint16_t>() * val); break;
        }
    }
    return ret;
}

uint64_t MultiValue::val_sign() {
    if (size() == 1 && at(0).state < OBJECT_NUMBER) {
        ItemValue* val = (ItemValue*)buffer;
        return val->get<uint64_t>();
    }
    std::stringstream buf;
    for (int i = 0; i < item_size; ++i) {
        ItemValue* val = (ItemValue*)(buffer + i * sizeof(ItemValue));
        switch (val->state)
        {
            case OBJECT_STRING: buf << val->getstr(); break;
            case OBJECT_INT32:
            case OBJECT_INT64:
            case OBJECT_UINT32:
            case OBJECT_UINT64:
            case OBJECT_BOOL:
            case OBJECT_UINT8:
            case OBJECT_INT8:
            case OBJECT_INT16:
            case OBJECT_UINT16: buf << val->get<uint64_t>(); break;
            case OBJECT_FLOAT: buf << val->get<float>(); break;
            case OBJECT_DOUBLE: buf << val->get<double>(); break;
        }
        buf << ":";
    }
    return get_sign64(buf.str().c_str(), buf.str().length() - 1);
}

uint64_t RowItemValue::val_sign() {
    if (size() == 1 && rowItem.firstVal().state < OBJECT_NUMBER) {
        return rowItem.firstVal().get<uint64_t>();
    }
    std::stringstream buffer;
    RowItemValue::iterator iter = begin();
    RowItemValue::iterator iter_end = end();
    for (; iter != iter_end; ++iter) {
        switch (iter->second.state)
        {
            case OBJECT_STRING: buffer << iter->second.getstr(); break;
            case OBJECT_INT32:
            case OBJECT_INT64:
            case OBJECT_UINT32:
            case OBJECT_UINT64:
            case OBJECT_BOOL:
            case OBJECT_UINT8:
            case OBJECT_INT8:
            case OBJECT_INT16:
            case OBJECT_UINT16: buffer << iter->second.get<uint64_t>(); break;
            case OBJECT_FLOAT: buffer << iter->second.get<float>(); break;
            case OBJECT_DOUBLE: buffer << iter->second.get<double>(); break;
        }
        buffer << ":";
    }
    return get_sign64(buffer.str().c_str(), buffer.str().length() - 1);
}

uint64_t RowItemValue::col_sign() {
    if (size() == 1) {
        return (uint64_t)rowItem.firstKey();
    }
    std::stringstream buffer;
    iterator iter = begin();
    iterator iter_end = end();
    for (; iter != iter_end; ++iter) {
        buffer << iter->first;
        buffer << ":";
    }
    return get_sign64(buffer.str().c_str(), buffer.str().length() - 1);
}

uint64_t RowItemValue2::val_sign() {
    if (size() == 1 && begin()->second.state < OBJECT_NUMBER) {
        return (uint64_t)begin()->second.get<uint64_t>();
    }
    std::stringstream buffer;
    RowItemValue2::iterator iter = begin();
    RowItemValue2::iterator iter_end = end();
    for (; iter != iter_end; ++iter) {
        switch (iter->second.state)
        {
            case OBJECT_STRING: buffer << iter->second.getstr(); break;
            case OBJECT_INT32:
            case OBJECT_INT64:
            case OBJECT_UINT32:
            case OBJECT_UINT64:
            case OBJECT_BOOL:
            case OBJECT_UINT8:
            case OBJECT_INT8:
            case OBJECT_INT16:
            case OBJECT_UINT16: buffer << iter->second.get<uint64_t>(); break;
            case OBJECT_FLOAT: buffer << iter->second.get<float>(); break;
            case OBJECT_DOUBLE: buffer << iter->second.get<double>(); break;
        }
        buffer << ":";
    }
    return get_sign64(buffer.str().c_str(), buffer.str().length() - 1);
}

uint64_t RowItemValue2::col_sign() {
    if (size() == 1) {
        return (uint64_t)begin()->first;
    }
    std::stringstream buffer;
    iterator iter = begin();
    iterator iter_end = end();
    for (; iter != iter_end; ++iter) {
        buffer << iter->first;
        buffer << ":";
    }
    return get_sign64(buffer.str().c_str(), buffer.str().length() - 1);
}




