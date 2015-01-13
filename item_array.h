#ifndef _ITEM_ARRAY_H_
#define _ITEM_ARRAY_H_
#include <stdint.h>
#include <string>
#include <map>
#include <vector>
#include <set>
#include "common.h"
#include "sign.h"
#include "list_map.h"
#include "list_set.h"

#define MAX_MULTI_VAL_NUM  8

struct TableIndex {
    TableIndex(std::set<short>& sets)
    : index(sets) {}
    TableIndex(ListSet<short>& sets)
    : index(sets) {}
    TableIndex() {}
    TableIndex(short a) {
        index.insert(a);
    }
    TableIndex(short a, short b) {
        index.insert(a);
        index.insert(b);
    }
    TableIndex(short a, short b, short c) {
        index.insert(a);
        index.insert(b);
        index.insert(c);
    }
    TableIndex(short a, short b, short c, short d) {
        index.insert(a);
        index.insert(b);
        index.insert(c);
        index.insert(d);
    }
    TableIndex(short a, short b, short c, short d, short e) {
        index.insert(a);
        index.insert(b);
        index.insert(c);
        index.insert(d);
        index.insert(e);
    }
    TableIndex(short a, short b, short c, short d, short e, short f) {
        index.insert(a);
        index.insert(b);
        index.insert(c);
        index.insert(d);
        index.insert(e);
        index.insert(f);
    }
    TableIndex(short a, short b, short c, short d, short e, short f, short g) {
        index.insert(a);
        index.insert(b);
        index.insert(c);
        index.insert(d);
        index.insert(e);
        index.insert(f);
        index.insert(g);
    }
    ListSet<short> index;
};

struct TableName {
    int id;
    std::string name;
    std::string server_name;
    TableName(int id_)
    : id(id_)
    , name("")
    , server_name("") {}
    TableName(std::string name_)
    : id(-1)
    , name(name_.c_str())
    , server_name("") {}
    TableName(const char* name_)
    : id(-1)
    , name(name_)
    , server_name("") {}
    TableName(std::string servername, int table_id)
    : id(table_id)
    , name("")
    , server_name(servername.c_str()) {}
    TableName(std::string servername, const char* name_)
    : id(-1)
    , name(name_)
    , server_name(servername.c_str()) {}
    inline int64_t server_id() {
        return get_sign64(server_name.c_str(), server_name.length());
    }
};


#define RPC(A, B) \
    TableName(A, B)


#define COL0 \
    TableIndex()

#define COL1(A) \
    TableIndex(A)

#define COL2(A, B) \
    TableIndex(A, B)

#define COL3(A, B, C) \
    TableIndex(A, B, C)

#define COL4(A, B, C, D) \
    TableIndex(A, B, C, D)

#define COL5(A, B, C, D, E) \
    TableIndex(A, B, C, D, E)

#define COL6(A, B, C, D, E, F) \
    TableIndex(A, B, C, D, E, F)

#define COL7(A, B, C, D, E, F, G) \
    TableIndex(A, B, C, D, E, F, G)

struct ItemValue
{
    unsigned char state;
    union {
        uint64_t  val;
        float     val_f;
        double    val_d;
    };
    std::string  str;
    ItemValue(const ItemValue& value)
    : state(value.state)
    , val(value.val)
    , str(value.str.c_str()) {}
    ItemValue()
    : state(OBJECT_NULL)
    , val(0)
    , str("") {}
    ItemValue(char t)
    : state(OBJECT_INT8)
    , val(t)
    , str("") {}
     ItemValue(unsigned char t)
    : state(OBJECT_UINT8)
    , val(t)
    , str("") {}
    ItemValue(short t)
    : state(OBJECT_INT16)
    , val(t)
    , str("") {}
    ItemValue(unsigned short t)
    : state(OBJECT_UINT16)
    , val(t)
    , str("") {}
    ItemValue(int t)
    : state(OBJECT_INT32)
    , val(t)
    , str("") {}
    ItemValue(unsigned int t)
    : state(OBJECT_UINT32)
    , val(t)
    , str("") {}
    ItemValue(float t)
    : state(OBJECT_FLOAT)
    , val_f(t)
    , str("") {}
    ItemValue(double t)
    : state(OBJECT_DOUBLE)
    , val_d(t)
    , str("") {}
    ItemValue(unsigned long long t)
    : state(OBJECT_UINT64)
    , val(t)
    , str("") {}
    ItemValue(long long t)
    : state(OBJECT_INT64)
    , val(t)
    , str("") {}
    ItemValue(uint64_t t)
    : state(OBJECT_UINT64)
    , val(t)
    , str("") {}
    ItemValue(int64_t t)
    : state(OBJECT_INT64)
    , val(t)
    , str("") {}
    ItemValue(bool t)
    : state(OBJECT_BOOL)
    , val(t)
    , str("") {}
    ItemValue(const char* t)
    : state(OBJECT_STRING)
    , val(0)
    , str(t) {
    }
    ItemValue(std::string t)
    : state(OBJECT_STRING)
    , val(0)
    , str(t.c_str()) {
    }
    template<typename T>
    T get() const;
	template<typename T>
    void set(T input);

	std::string getstr() const {
		if (!isString()) {
			return "";
		}
		return str.c_str();
	}
	const char* getchar() const {
		if (!isString()) {
			return STR_CONST_NULL;
		}
		return str.c_str();
	}
	
	void setstr(std::string * input) {
		if (!isString()) {
			return;
		}
		str = input->c_str();
	}
	
    inline bool isNull() const {return (state == OBJECT_NULL);}
    inline bool isString() const {return (state == OBJECT_STRING);}
    bool operator==(const ItemValue& vals) const {
        if (state == OBJECT_STRING) { return (str == vals.str);}
        else if (state < OBJECT_NUMBER) {return (val == vals.val);}
        else if (state == OBJECT_DOUBLE) {return (val_d == vals.val_d);}
        else {return (val_f == vals.val_f);}
        return false;
    }
    bool operator!=(const ItemValue& vals) const {
        if (state == OBJECT_STRING) { return (str != vals.str);}
        else if (state < OBJECT_NUMBER) {return (val != vals.val);}
        else if (state == OBJECT_DOUBLE) {return (val_d != vals.val_d);}
        else {return (val_f != vals.val_f);}
        return true;
    }
    bool operator>(const ItemValue& value) const;
    bool operator<(const ItemValue& value) const;
    bool operator<=(const ItemValue& value) const;
    //ItemValue& operator=(ItemValue& value);
    bool operator>=(const ItemValue& value) const;
    ItemValue operator+(const ItemValue& value) const;
    ItemValue operator-(const ItemValue& value) const;
    ItemValue operator*(int val) const;
};

struct MultiValue {
    ~MultiValue() {
        for (int i = 0; i < item_size; ++i) {
            ((ItemValue*)(buffer + i * sizeof(ItemValue)))->~ItemValue();
        }
    }
    MultiValue(ItemValue v1) {
        new(buffer) ItemValue(v1);
        item_size = 1;
    }
    MultiValue(ItemValue v1, ItemValue v2) {
        new(buffer) ItemValue(v1);
        new(buffer + sizeof(ItemValue)) ItemValue(v2);
        item_size = 2;
    }
    MultiValue(ItemValue v1, ItemValue v2, ItemValue v3) {
        new(buffer) ItemValue(v1);
        new(buffer + sizeof(ItemValue)) ItemValue(v2);
        new(buffer + 2 * sizeof(ItemValue)) ItemValue(v3);
        item_size = 3;
    }
    MultiValue(ItemValue v1, ItemValue v2, ItemValue v3, ItemValue v4) {
        new(buffer) ItemValue(v1);
        new(buffer + sizeof(ItemValue)) ItemValue(v2);
        new(buffer + 2 * sizeof(ItemValue)) ItemValue(v3);
        new(buffer + 3 * sizeof(ItemValue)) ItemValue(v4);
        item_size = 4;
    }
    MultiValue(ItemValue v1, ItemValue v2, ItemValue v3, ItemValue v4, ItemValue v5) {
        new(buffer) ItemValue(v1);
        new(buffer + sizeof(ItemValue)) ItemValue(v2);
        new(buffer + 2 * sizeof(ItemValue)) ItemValue(v3);
        new(buffer + 3 * sizeof(ItemValue)) ItemValue(v4);
        new(buffer + 4 * sizeof(ItemValue)) ItemValue(v5);
        item_size = 5;
    }
    MultiValue(ItemValue v1, ItemValue v2, ItemValue v3, ItemValue v4, ItemValue v5, ItemValue v6) {
        new(buffer) ItemValue(v1);
        new(buffer + sizeof(ItemValue)) ItemValue(v2);
        new(buffer + 2 * sizeof(ItemValue)) ItemValue(v3);
        new(buffer + 3 * sizeof(ItemValue)) ItemValue(v4);
        new(buffer + 4 * sizeof(ItemValue)) ItemValue(v5);
        new(buffer + 5 * sizeof(ItemValue)) ItemValue(v6);
        item_size = 6;
    }
    MultiValue(ItemValue v1, ItemValue v2, ItemValue v3, ItemValue v4, ItemValue v5, ItemValue v6, ItemValue v7) {
        new(buffer) ItemValue(v1);
        new(buffer + sizeof(ItemValue)) ItemValue(v2);
        new(buffer + 2 * sizeof(ItemValue)) ItemValue(v3);
        new(buffer + 3 * sizeof(ItemValue)) ItemValue(v4);
        new(buffer + 4 * sizeof(ItemValue)) ItemValue(v5);
        new(buffer + 5 * sizeof(ItemValue)) ItemValue(v6);
        new(buffer + 6 * sizeof(ItemValue)) ItemValue(v7);
        item_size = 7;
    }
    MultiValue(ItemValue v1, ItemValue v2, ItemValue v3, ItemValue v4, ItemValue v5, ItemValue v6, ItemValue v7, ItemValue v8) {
        new(buffer) ItemValue(v1);
        new(buffer + sizeof(ItemValue)) ItemValue(v2);
        new(buffer + 2 * sizeof(ItemValue)) ItemValue(v3);
        new(buffer + 3 * sizeof(ItemValue)) ItemValue(v4);
        new(buffer + 4 * sizeof(ItemValue)) ItemValue(v5);
        new(buffer + 5 * sizeof(ItemValue)) ItemValue(v6);
        new(buffer + 6 * sizeof(ItemValue)) ItemValue(v7);
        new(buffer + 7 * sizeof(ItemValue)) ItemValue(v8);
        item_size = 8;
    }
    ItemValue at(int N) const {
        return *(ItemValue*)(buffer + N * sizeof(ItemValue));
    }
    uint64_t val_sign();
    int size() const { return item_size;}
    int item_size;
    mutable char buffer[MAX_MULTI_VAL_NUM * sizeof(ItemValue)]; 
    MultiValue(const MultiValue& val) {
        int size = val.size();
        for (int i = 0; i < size; ++i) {
            new(buffer + i * sizeof(ItemValue)) ItemValue(val.at(i));
        }
    }
private:
    MultiValue& operator=(const MultiValue& val) {
        return   *this;
    }
};


#define MULTI1(A) MultiValue(A)
#define MULTI2(A, B) MultiValue(A, B)
#define MULTI3(A, B, C) MultiValue(A, B, C)
#define MULTI4(A, B, C, D) MultiValue(A, B, C, D)
#define MULTI5(A, B, C, D, E) MultiValue(A, B, C, D, E)
#define MULTI6(A, B, C, D, E, F) MultiValue(A, B, C, D, E, F)
#define MULTI7(A, B, C, D, E, F, G) MultiValue(A, B, C, D, E, F, G)
#define MULTI8(A, B, C, D, E, F, G, H) MultiValue(A, B, C, D, E, F, G, H)


struct RowItemValue {
    typedef ListMap<unsigned short, ItemValue>::iterator iterator;
    RowItemValue() {}
    RowItemValue(unsigned short N, ItemValue val) {
        //rowItem[N] = val;
        rowItem.insert(N, val);
    }
    RowItemValue(unsigned short N1, ItemValue V1, unsigned short N2, ItemValue V2) {
        rowItem.insert(N1, V1);
        rowItem.insert(N2, V2);
    }
    RowItemValue(unsigned short N1, ItemValue V1, unsigned short N2, ItemValue V2, unsigned short N3, ItemValue V3) {
        rowItem.insert(N1, V1);
        rowItem.insert(N2, V2);
        rowItem.insert(N3, V3);
    }
    RowItemValue(unsigned short N1, ItemValue V1, unsigned short N2, ItemValue V2, unsigned short N3, ItemValue V3, unsigned short N4, ItemValue V4) {
        rowItem.insert(N1, V1);
        rowItem.insert(N2, V2);
        rowItem.insert(N3, V3);
        rowItem.insert(N4, V4);
    }
    RowItemValue(unsigned short N1, ItemValue V1, unsigned short N2, ItemValue V2, unsigned short N3, ItemValue V3, unsigned short N4, ItemValue V4, unsigned short N5, ItemValue V5) {
        rowItem.insert(N1, V1);
        rowItem.insert(N2, V2);
        rowItem.insert(N3, V3);
        rowItem.insert(N4, V4);
        rowItem.insert(N5, V5);
    }
    RowItemValue(unsigned short N1, ItemValue V1, unsigned short N2, ItemValue V2, unsigned short N3, ItemValue V3, unsigned short N4, ItemValue V4, unsigned short N5, ItemValue V5, unsigned short N6, ItemValue V6) {
        rowItem.insert(N1, V1);
        rowItem.insert(N2, V2);
        rowItem.insert(N3, V3);
        rowItem.insert(N4, V4);
        rowItem.insert(N5, V5);
        rowItem.insert(N6, V6);
    }
    RowItemValue(unsigned short N1, ItemValue V1, unsigned short N2, ItemValue V2, unsigned short N3, ItemValue V3, unsigned short N4, ItemValue V4, unsigned short N5, ItemValue V5, unsigned short N6, ItemValue V6, unsigned short N7, ItemValue V7) {
        rowItem.insert(N1, V1);
        rowItem.insert(N2, V2);
        rowItem.insert(N3, V3);
        rowItem.insert(N4, V4);
        rowItem.insert(N5, V5);
        rowItem.insert(N6, V6);
        rowItem.insert(N7, V7);
    }
    void push(unsigned short col, ItemValue item) {
        rowItem.insert(col, item);
    }
    iterator begin() const {
        return rowItem.begin();
    }
    iterator end() const {
        return rowItem.end();
    }
	inline int  size() const
	{
		return rowItem.size();
	}

    ItemValue at(int N) const {
        iterator iter = rowItem.begin();
        iterator iter_end = rowItem.end();
        for (; iter != iter_end; ++iter) {
            if (iter->first == N) {
                return iter->second;
            }
        }
        return ItemValue();
    }
    uint64_t val_sign();
    uint64_t col_sign();
    RowItemValue(const RowItemValue& val) {
        iterator iter = val.begin();
        iterator end = val.end();
        for (; iter != end; ++iter) {
            rowItem.insert(iter->first, iter->second);
        }
    }
private:
    unsigned short first_index() {
        return rowItem.firstKey();
    }
    ItemValue& first_value() {
        return rowItem.firstVal();
    }
    RowItemValue& operator=(const RowItemValue& value) {
        return *this;
    }
private:
    mutable ListMap<unsigned short, ItemValue> rowItem;
};


struct RowItemValue2 {
    typedef std::map<unsigned short, ItemValue>::iterator iterator;
    RowItemValue2() {}
    RowItemValue2(unsigned short N, ItemValue val) {
        //rowItem[N] = val;
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N, val));
    }
    RowItemValue2(unsigned short N1, ItemValue V1, unsigned short N2, ItemValue V2) {
        //rowItem[N1] = V1;
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N1, V1));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N2, V2));
        //rowItem[N2] = V2;
    }
    RowItemValue2(unsigned short N1, ItemValue V1, unsigned short N2, ItemValue V2, unsigned short N3, ItemValue V3) {
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N1, V1));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N2, V2));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N3, V3));
    }
    RowItemValue2(unsigned short N1, ItemValue V1, unsigned short N2, ItemValue V2, unsigned short N3, ItemValue V3, unsigned short N4, ItemValue V4) {
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N1, V1));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N2, V2));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N3, V3));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N4, V4));
    }
    RowItemValue2(unsigned short N1, ItemValue V1, unsigned short N2, ItemValue V2, unsigned short N3, ItemValue V3, unsigned short N4, ItemValue V4, unsigned short N5, ItemValue V5) {
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N1, V1));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N2, V2));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N3, V3));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N4, V4));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N5, V5));
    }
    RowItemValue2(unsigned short N1, ItemValue V1, unsigned short N2, ItemValue V2, unsigned short N3, ItemValue V3, unsigned short N4, ItemValue V4, unsigned short N5, ItemValue V5, unsigned short N6, ItemValue V6) {
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N1, V1));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N2, V2));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N3, V3));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N4, V4));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N5, V5));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N6, V6));
    }
    RowItemValue2(unsigned short N1, ItemValue V1, unsigned short N2, ItemValue V2, unsigned short N3, ItemValue V3, unsigned short N4, ItemValue V4, unsigned short N5, ItemValue V5, unsigned short N6, ItemValue V6, unsigned short N7, ItemValue V7) {
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N1, V1));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N2, V2));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N3, V3));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N4, V4));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N5, V5));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N6, V6));
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(N7, V7));
    }
    void push(short col, ItemValue item) {
        rowItem.insert(std::map<unsigned short, ItemValue>::value_type(col, item));
    }
    iterator begin() {
        return rowItem.begin();
    }
    iterator end() {
        return rowItem.end();
    }
	inline int  size()
	{
		return rowItem.size();
	}
    ItemValue at(int N) {
        iterator iter = rowItem.find(N);
        if (iter == rowItem.end()) {
            return ItemValue();
        }
        return iter->second;
    }
    uint64_t val_sign();
    uint64_t col_sign();
private:
    RowItemValue2& operator=(const RowItemValue2& val) {
        return *this;
    }
    RowItemValue2(const RowItemValue2& val) {}
private:
    std::map<unsigned short, ItemValue> rowItem;
};


#define IN0() \
    RowItemValue()

#define IN1(A, B) \
    RowItemValue(A, B)

#define IN2(A, B, C, D) \
    RowItemValue(A, B, C, D)

#define IN3(A, B, C, D, E, F) \
    RowItemValue(A, B, C, D, E, F)

#define IN4(A, B, C, D, E, F, G, H) \
    RowItemValue(A, B, C, D, E, F, G, H)

#define IN5(A, B, C, D, E, F, G, H, I, J) \
    RowItemValue(A, B, C, D, E, F, G, H, I, J)

#define IN6(A, B, C, D, E, F, G, H, I, J, K, L) \
    RowItemValue(A, B, C, D, E, F, G, H, I, J, K, L)

#define IN7(A, B, C, D, E, F, G, H, I, J, K, L, M, N) \
    RowItemValue(A, B, C, D, E, F, G, H, I, J, K, L, M, N)


#define VAL1(A, B) \
    RowItemValue(A, B)

#define VAL2(A, B, C, D) \
    RowItemValue(A, B, C, D)

#define VAL3(A, B, C, D, E, F) \
    RowItemValue(A, B, C, D, E, F)

#define VAL4(A, B, C, D, E, F, G, H) \
    RowItemValue(A, B, C, D, E, F, G, H)

#define VAL5(A, B, C, D, E, F, G, H, I, J) \
    RowItemValue(A, B, C, D, E, F, G, H, I, J)

#define VAL6(A, B, C, D, E, F, G, H, I, J, K, L) \
    RowItemValue(A, B, C, D, E, F, G, H, I, J, K, L)

#define VAL7(A, B, C, D, E, F, G, H, I, J, K, L, M, N) \
    RowItemValue(A, B, C, D, E, F, G, H, I, J, K, L, M, N)



template<>
std::string ItemValue::get<std::string>() const;

template<>
const char* ItemValue::get<const char*>() const;

template<>
void ItemValue::set<std::string>(std::string input);

template<>
void ItemValue::set<const char*>(const char* input);



template<typename T>
T ItemValue::get() const {
    if (state == OBJECT_FLOAT) {
        return (T)val_f;
    } else if (state == OBJECT_DOUBLE) {
        return (T)val_d;
    } else if (state != OBJECT_NULL) {
        return (T)val;
    } else {
        return 0;
    }
}

template<typename T>
void ItemValue::set(T input)
{
	if (state == OBJECT_FLOAT) {
		val_f = input;
	} else if (state == OBJECT_DOUBLE) {
		val_d = input;
	} else {
		val = input;
	}
}

#endif

