#ifndef _SERIAL_TABLE_H_
#define _SERIAL_TABLE_H_

#include "table.h"

#define SERIAL_TABLE_ID  0
#define SERIAL_TABLE_NAME "increase"

enum {
    COL_SERIAL_TABLE_NAME,
    COL_SERIAL_COL_NAME,
    COL_SERIAL_SERIAL,
};

class Serial {
private:
    Serial() {};
    ~Serial() {};
public:
    static SingleTable& SERIAL() { return *_serial;}
    static bool global_init();
    static bool global_destroy();
private:
    static SingleTable* _serial;
};
#endif

