#ifndef _ATOMIC_TIMER_H_
#define _ATOMIC_TIMER_H_
#include <stdint.h>
#include <sys/time.h>
#include <boost/function.hpp>


#include "hashmap.h"
#include "atomiccache.h"
#include "atomicqueue.h"
#include "tinytable_log_utils.h"

// for test
//#define TT_WARN_LOG printf
//#define TT_DEBUG_LOG printf
//#define TT_FATAL_LOG printf


// this is the timer wheel class
// use hashmap to make the bucket
// it has three types of expire
// 1 real timer
// 2 lazy check
#define  TIMER_SAME_DETECT_US  100000ll
extern const int  TIMER_FOREVER_LOOP;
//this is level step
//now is defined here

// use the default config
// tick is 50ms
// the max timer time you can get listed below
// level      time
//  1         12.8s
//  2         29.127hour
//  3         6.81years
//  4         6973years
//  5         1785000 years

extern int TIMER_DEFAULT_BUCKET[5];
extern int TIMER_DEFAULT_RATE[5];

#define MAX_BUCKET_NUM 1024
#define MIN_BUCKET_NUM 32
#define MAX_LEVEL_NUM  5
#define MIN_LEVEL_RATE 1
#define MAX_LEVEL_RATE 64

// the callback func and run callback func
typedef boost::function<void ()> timer_callback;
typedef void (runfunc)(timer_callback, void* arg);
class AtomicTimer {
public:
    // timer types
    enum {
        TIMER_REAL, // start a realtimer
        TIMER_LAZY, // used with epoll or select
    };
    enum {
        POS_OK,
        POS_OVERFLOW,
        POS_EXPIRED,
    };
    // tick is the timer step
    // all the time will be devided into many steps
    // bucket is  2^bucket  in order to make the size can be devided
    AtomicTimer();
    virtual ~AtomicTimer();
    struct TimerNode {
        uint64_t  expired_us; // expired time
        uint64_t  interval_us; // interval time (us)
        uint64_t  key;// timer unique key
        timer_callback callback; // callback func
        int       round; // last rounds
        TimerNode(uint64_t us_times, timer_callback callback_, uint64_t interval_us_ = 0, int round_ = 1, uint64_t key_ = -1)
        : expired_us(us_times)
        , interval_us(interval_us_)
        , key(key_)
        , callback(callback_)
        , round(round_) {
            if (key == (uint64_t)-1) {
                // generate a uniqe key
                key = __sync_fetch_and_sub(&AtomicTimer::global_key, 1);
            }
        }
        // reset according to cur
        int reset(long long cur) {
            if (round <= 0 && round != TIMER_FOREVER_LOOP) {
                return round;
            }
            if (((long long)expired_us) > cur) {
                return round;
            }
            long long diff = cur - expired_us;
            // forever loop  reset node the cur time
            if (round == TIMER_FOREVER_LOOP) {
                expired_us = cur + interval_us - (diff % interval_us);
                return TIMER_FOREVER_LOOP;
            }
            round--;
            if (round <= 0) {
                round = 0;
                return 0;
            }
            int expire_round = diff/interval_us;
            round -= expire_round;
            if (round <= 0) {
                round = 0;
                return 0;
            }
            expired_us = cur + interval_us - (diff % interval_us);
            return round;
        }
    };
public:
    static  bool global_init(int type = TIMER_REAL, int tick_us = 10000);
    static  bool global_destroy();
    static  long long now(bool absolute);
public:
    virtual bool init(bool absolute = true, runfunc* func = NULL, void* func_arg = NULL, int level = 3, int* bucket = NULL, int* rate = NULL, bool cache_lazy = true);
    virtual void setRunfunc(runfunc* func, void* func_arg = NULL);
    // key is used to make the timer unique
    // when we del the timer we can use the key to find it
    // if key == -1  then the class will generate a key for this timer
    virtual bool addTimer(uint64_t us_time, timer_callback func, uint64_t interval = 0, int round = 1, uint64_t key = -1, uint64_t* newKey = NULL);
    // this is func begin with fixed time and user set the interval
    virtual bool addFixedTimer(int wday, int hour, int min, int sec, timer_callback func, uint64_t interval = 0, int round = 1, uint64_t key = -1, uint64_t* newKey = NULL);
    // these are funcs used for regular fixed timer
    virtual bool addHourlyTimer(int min, int sec, timer_callback func, uint64_t key = -1, uint64_t* newKey = NULL);
    virtual bool addDailyTimer(int hour, int min, int sec, timer_callback func, uint64_t key = -1, uint64_t* newKey = NULL);
    virtual bool addWeeklyTimer(int wday, int hour, int min, int sec, timer_callback func, uint64_t key = -1, uint64_t* newKey = NULL);
    // del timer
    // only the timer add with key can be deleted
    virtual bool delTimer(uint64_t key);
    // this func returns the next expired time and deal the expired timers
    //  so we can use the func to get a timeout time(ms) cooperate with epoll or select to make the lazy timer
    //  this func can be used in multithread when multithread is true
    virtual long long expire(bool multithread = false);
    inline bool isInit() const {return _is_init;}
public:
    static bool global_timer_init;
    // next expired time
    static long long  global_expired_absolute_time;
    static long long  global_expired_unabsolute_time;
    // realtimer or lazy timer
    static int  global_type;
    // used for multithread to sync
    // if expire used in many threads this param is used to make sure only one thread can expire at one step
    static bool global_expired;
    static int  global_tick_us;
    // global key id for timer no need to delete
    // we want the key is different from the added key
    // so we will sub the key from 0xFFFFFFFFFFFFFFFF  in case that normal key is in int range
    static uint64_t global_key;
    static AtomicQueue<timer_callback>* global_timer_func;
protected:
    // first get the free node cached
    // if it has no cached node then we new a node
    // as well when the timer is deleted or expired the node will push to the cache to be reused
    virtual TimerNode* newNode(uint64_t us_expired, timer_callback callback_, uint64_t interval_us_ = 0, int round_ = 0, uint64_t key_ = -1);
    virtual long long _expire();
    void _destroy();
    virtual int _getPos(long long t, int& level, int& bucket);
    virtual long long _backward(long long current, long long expired_time);
    virtual bool _addTimer(TimerNode* node);
    virtual bool _cascadeTimers(int level, int bucket);
    virtual long long _getExpiredTime();
private:
    // these funcs are for realtimer
    // realtimer can be only created three in single process so we make the realtimer globally
    // this func will be called in global_init
    static void start_timer();
    static void stop_timer();
private:
    // the 
    bool _is_init;
    bool _absolute;
    int  _level;
    int*  _bucket;
    int*  _rate;
    // current idx of the different timer wheel
    int*  _cur_index;
    long long _bucket_step[MAX_LEVEL_NUM];
    long long _level_max[MAX_LEVEL_NUM];
    // timer wheel
    // we make three pointer here
    // the first is level pointer
    // the second is bucket level
    // the third is very special while we make the wheel item pointer
    // AtomicHashmap has the func to resize while more and more items added in it  then the map will be larger and larger
    // make it pointer so we can free the map mem when there are only a few items in it
    // but the first version will do nothing
    AtomicHashmap<uint64_t, TimerNode*>** _wheel[MAX_LEVEL_NUM];
    // timer bucket idx in order to delete key quickly
    AtomicHashmap<uint64_t, std::pair<int, int> >* _timer_index;
    // node cache to reuse mem
    AtomicCache<TimerNode*>* _cache;
    // the callback func run methods
    // if _func==NULL the run callback func in the expire thread
    runfunc*   _func;
    void*      _func_arg;
};

#endif

