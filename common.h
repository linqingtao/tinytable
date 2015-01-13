#ifndef _TINYTABLE_COMMON_H_
#define _TINYTABLE_COMMON_H_
#include <stdint.h>
#include <string>
#include <pthread.h>
#include <ctime>
#include <boost/function.hpp>
#include <boost/bind.hpp>


typedef boost::function<void()> event_func;
typedef void (run_eventfunc)(event_func, void*);

static const int MAX_BUFFER_LEN = 1024 * 100;

enum {
OBJECT_BOOL,
OBJECT_INT8,
OBJECT_UINT8,
OBJECT_INT16,
OBJECT_UINT16,
OBJECT_INT32,
OBJECT_UINT32,
OBJECT_INT64,
OBJECT_UINT64,
OBJECT_NUMBER,
OBJECT_FLOAT,
OBJECT_DOUBLE,
OBJECT_STRING,
OBJECT_NULL,
};

enum {
    OPS_DB, // flush the change to db right now
    OPS_CACHE, // flush the change to cache right now
    OPS_LOCAL, // get from local mem
    OPS_DEFAULT, // ops according to the default config
    OPS_RPC,
};

static const char* STR_CONST_NULL = "";
static char STR_NULL[2] = "";


extern int      triggerTimes(int weekday, int hour, int min, int sec, time_t last_time, time_t now);

#endif

