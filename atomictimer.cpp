#include "atomictimer.h"
#include<signal.h>
#include <boost/bind.hpp>

bool AtomicTimer::global_timer_init = false;
long long AtomicTimer::global_expired_absolute_time = 0;
long long AtomicTimer::global_expired_unabsolute_time = 0;
int AtomicTimer::global_type = 0;
int AtomicTimer::global_tick_us = 10000;
bool AtomicTimer::global_expired = false;
uint64_t AtomicTimer::global_key = 0xFFFFFFFFFFFFFFFF;
AtomicQueue<timer_callback>* AtomicTimer::global_timer_func = NULL;

const int  TIMER_FOREVER_LOOP = -10000;
int TIMER_DEFAULT_BUCKET[5] = {512, 256, 64, 64, 32};
int TIMER_DEFAULT_RATE[5] = {1, 256,128, 32, 32};

// some time const
// we set here to forbid it being calculated in process
const long long MIN_SECONDS = 60;
const long long HOUR_SECONDS = 60 * 60;
const long long HOUR_MICROSECONDS = 60 * 60 * 1000000ll;
const long long DAY_SECONDS = 24 * 60 * 60;
const long long DAY_MICROSECONDS = 24 * 60 * 60ll * 1000000ll;
const long long WEEK_SECONDS = 7 * 24 * 60 * 60;
const long long WEEK_MICROSECONDS = 7 * 24ll * 60ll * 60ll * 1000000ll;

inline void defaultfunc(timer_callback func, void* arg) {
    func();
}

AtomicTimer::AtomicTimer()
: _is_init(false)
, _absolute(false)
, _level(0)
, _bucket(NULL)
, _rate(NULL)
, _cur_index(NULL)
, _timer_index(NULL)
, _cache(NULL)
, _func(NULL)
, _func_arg(NULL) {
    for (int i = 0; i < MAX_LEVEL_NUM; i++) { 
        _wheel[i] = NULL;
        _bucket_step[i] = 1;
        _level_max[i] = 0;
    }
}

AtomicTimer::~AtomicTimer() {
    _destroy();
}

bool AtomicTimer::global_init(int type, int tick_us) {
    if (AtomicTimer::global_timer_init) {
        return true;
    }
    // we need to make a thread or real timer
    if (tick_us <= 0) {
        TT_FATAL_LOG("timer tick %d error\n", tick_us);
        return false;
    }
    switch (type)
    {
        case TIMER_REAL: 
        {
            // realtimer need a func queue to store the registered expire func
            // enqueue happens in AtomicTimer::init
            AtomicTimer::global_timer_func = new(std::nothrow) AtomicQueue<timer_callback>(false);
            if (NULL == AtomicTimer::global_timer_func) {
                TT_FATAL_LOG("create global timer func error");
                return false;
            }
            AtomicTimer::global_tick_us = tick_us;
            start_timer(); 
            break;
        }
        case TIMER_LAZY: break;
        default: TT_FATAL_LOG("init timer error type %d", type); return false;
    }
    AtomicTimer::global_tick_us = tick_us;
    AtomicTimer::global_type = type;
    AtomicTimer::global_timer_init = true;
    AtomicTimer::global_expired_absolute_time = now(true) + tick_us;
    AtomicTimer::global_expired_unabsolute_time = now(false) + tick_us;
    return true;
}

bool AtomicTimer::global_destroy() {
    if (AtomicTimer::global_type == TIMER_REAL && AtomicTimer::global_timer_init) {
        AtomicTimer::stop_timer();
        AtomicTimer::global_timer_init = false;
    }
    if (NULL != AtomicTimer::global_timer_func) {
        delete AtomicTimer::global_timer_func;
        AtomicTimer::global_timer_func = NULL;
    }
    return true;
}

/* func init all the params of the class
 * 
 */
void AtomicTimer::setRunfunc(runfunc* func, void* func_arg) {
    if (func != NULL) {
        _func = func;
        _func_arg = func_arg;
    }
}

bool AtomicTimer::init(bool absolute, runfunc* func, void* func_arg, int level, int* bucket, int* rate, bool cache_lazy) {
    int i = 0;
    // store bucket && rate
    if (!AtomicTimer::global_timer_init) {
        TT_WARN_LOG("timer global is not inited");
        return false;
    }
    if (_is_init) {return true;}
    _level = level;
    if (_level <= 0) {
        _level = 1;
    }
    if (level > MAX_LEVEL_NUM) {
        _level = MAX_LEVEL_NUM;
    }
    _func = func;
    if (_func == NULL) {
        _func = &defaultfunc;
    }
    _func_arg = func_arg;
    _absolute = absolute;
    // bucket
    _bucket = new(std::nothrow) int[_level];
    if (_bucket == NULL) {
        TT_WARN_LOG("new timer bucket %d error", _level);
        goto ERROR;
    }
    if (bucket == NULL) {
        bucket = TIMER_DEFAULT_BUCKET;
    }
    memcpy(_bucket, bucket, sizeof(int) * _level);
    //check 
    for (i = 0; i < _level; i++) {
        if (_bucket[i] > MAX_BUCKET_NUM) {
            _bucket[i] = MAX_BUCKET_NUM;
        }
        if (_bucket[i] < MIN_BUCKET_NUM) {
            _bucket[i] = MIN_BUCKET_NUM;
        }
        // here we make sure the rate is 2^n
        int tmp = _bucket[i];
        tmp >>= 1;
        _bucket[i] = 1;
        while (tmp) {
            _bucket[i] <<= 1;
            tmp >>= 1;
        }
    }
    // rate
    _rate = new(std::nothrow) int[_level];
    if (_rate == NULL) {
        TT_WARN_LOG("new timer rate %d error", _level);
        goto ERROR;
    }
    if (rate == NULL) {
        rate = TIMER_DEFAULT_RATE;
    }
    memcpy(_rate, rate, sizeof(int) * _level);
    //check 
    _rate[0] = 1;
    for (i = 1; i < _level; i++) {
        if (_rate[i] > MAX_LEVEL_RATE) {
            _rate[i] = MAX_LEVEL_RATE;
        }
        if (_rate[i] < MIN_LEVEL_RATE) {
            _rate[i] = MIN_LEVEL_RATE;
        }
        // here we make sure the rate is 2^n
        int tmp = _rate[i];
        tmp >>= 1;
        _rate[i] = 1;
        while (tmp) {
            _rate[i] <<= 1;
            tmp >>= 1;
        }
    }
    // bucket step
    _bucket_step[0] = AtomicTimer::global_tick_us;
    for (i = 1; i < _level; i++) {
        _bucket_step[i] = _bucket_step[i - 1] * _rate[i];
    }
    // level step
    /*_level_total
    for (i = 1; i < _level; i++) {
        _level_step[i] = _bucket_step[i - 1] * _bucket[i] * _rate[i - 1];
    }*/
    // level base
    _level_max[0] = AtomicTimer::global_tick_us * _bucket[0];
    for (i = 1; i < _level; i++) {
        _level_max[i] = _level_max[i - 1] + _bucket_step[i] * _bucket[i];
    }
    // cur index
    _cur_index = new(std::nothrow) int[_level];
    if (_cur_index == NULL) {
        TT_WARN_LOG("new timer cur index %d error", _level);
        goto ERROR;
    }
    for (int i = 0; i < _level; i++) {
        _cur_index[i] = _bucket[i] - 1;
    }
    // wheel
    for (i = 0; i < _level; i++) {
        _wheel[i] = new(std::nothrow) AtomicHashmap<uint64_t, TimerNode*>*[_bucket[i]];
        if (_wheel[i] == NULL) {
            TT_WARN_LOG("create wheel[%d] bucket %d error", i, _bucket[i]);
            goto ERROR;
        }
        // create single map
        int j = 0;
        for (j = 0; j < _bucket[i]; j++) {
            _wheel[i][j] = new(std::nothrow) AtomicHashmap<uint64_t, TimerNode*>(false, 10, true);
            if (NULL == _wheel[i][j]) {
                TT_WARN_LOG("create wheel[%d][%d] error", i, j);
                // destroy prev
                for (int k = 0; k < j; k++) {
                    delete _wheel[i][k];
                    _wheel[i][k] = NULL;
                }
                delete _wheel[i];
                _wheel[i] = NULL;
                goto ERROR;
            }
        }
    }
    // timer index
    _timer_index = new(std::nothrow) AtomicHashmap<uint64_t, std::pair<int, int> >(false, 10, true);
    if (_timer_index == NULL) {
        TT_WARN_LOG("create timer index error: %d", _level);
        goto ERROR;
    }
    // cache (delay 10s/free node/reuse mem)
    if (cache_lazy) {
        _cache = new(std::nothrow) AtomicLazyCache<TimerNode*>();
    } else {
        _cache = new(std::nothrow) AtomicThreadCache<TimerNode*>();
    }
    _cache->init(5, true, true);
    // add realtimer queue
    if (AtomicTimer::global_type == TIMER_REAL) {
        AtomicTimer::global_timer_func->enqueue(boost::bind(&AtomicTimer::expire, this, false));
    }
    _is_init = true;
    TT_DEBUG_LOG("init timer absolute %d level %d success!\n", absolute, _level);
    return true;
ERROR:
    _destroy();
    return false;
}

// free all the class params
void AtomicTimer::_destroy() {
    for (int i = 0; i < MAX_LEVEL_NUM; i++) {
        if (_wheel[i] != NULL) {
            delete[] _wheel[i];
            _wheel[i] = NULL;
        }
    }
    if (_bucket != NULL) {
        delete _bucket;
        _bucket = NULL;
    }
    if (_rate != NULL) {
        delete _rate;
        _rate = NULL;
    }
    if (_cur_index != NULL) {
        delete _cur_index;
        _cur_index = NULL;
    }   
    if (_cache != NULL) {
        delete _cache;
        _cache = NULL;
    }
    if (_timer_index != NULL) {
        delete _timer_index;
        _timer_index = NULL;
    }
}

// node creater
AtomicTimer::TimerNode* AtomicTimer::newNode(uint64_t us_expired, timer_callback callback_, uint64_t interval_us_, int round_, uint64_t key_) {
    TimerNode* node = NULL;
    // first try to pop from cache
    if (_cache->pop(node)) {
        new(node) TimerNode(us_expired, callback_, interval_us_, round_, key_);
    } else {
        // if cache is empty then new a node
        node = new(std::nothrow) TimerNode(us_expired, callback_, interval_us_, round_, key_);
    }
    return node;
}

long long AtomicTimer::now(bool absolute) {
    struct timespec ts;
    // if absolute mode we use real time
    // in this mode timer expire according to the sys time
    if (absolute) {
        clock_gettime(CLOCK_REALTIME, &ts);
    } else {
        // the timer is only expired by interval times no matter how you change the sys time
        clock_gettime(CLOCK_MONOTONIC, &ts);
    }
    return ts.tv_sec *1000000ll + ts.tv_nsec/1000ll;
}

long long  AtomicTimer::_getExpiredTime() {
    long long expired_time = AtomicTimer::global_expired_absolute_time;
    if (!_absolute) {
        expired_time = AtomicTimer::global_expired_unabsolute_time;
    }
    return expired_time;
}

// this is the basic func
bool AtomicTimer::_addTimer(TimerNode* node) {
    long long now = AtomicTimer::now(_absolute);
    long long  prev = _getExpiredTime();
    long long expired_us = node->expired_us;
    int level = 0;
    int bucket = 0;
    if (now >= expired_us) {
        //printf("addtimer expire now %lld expired %lld\n", now, expired_us);
        (*_func)(node->callback, _func_arg);
        node->reset(now);
        if (node->round == 0) {
            return true;
        }
        return _addTimer(node);
    }
    // here we do not to check the time
    // as node can be added according to expired time no matter it changes during the insert
    // when it happens it means the node insert with the old time but expires with the new time so it will be ok
    int ret = _getPos(expired_us, level, bucket);
    if (POS_OVERFLOW == ret) {
        TT_WARN_LOG("add timer error:overflow us %lld interval %lu round %d key %lu\n", expired_us, node->interval_us, node->round, node->key);
        //_cache->push(node);
        return false;
    } else if (POS_EXPIRED == ret) {
        //printf("addtimer pos expire now %lld expired %lld\n", now, expired_us);
        (*_func)(node->callback, _func_arg);
        long long current = now;
        long long expired_time = prev;
        if (current < expired_time - AtomicTimer::global_tick_us) {
            TT_FATAL_LOG("time is backword: cur %lld expired %lld\n", current, expired_time);
            long long diff = expired_time - AtomicTimer::global_tick_us - current;
            long long new_expired = now + (diff%AtomicTimer::global_tick_us);
            if (_absolute) {
                __sync_lock_test_and_set(&AtomicTimer::global_expired_absolute_time, new_expired);
            } else {
                __sync_lock_test_and_set(&AtomicTimer::global_expired_unabsolute_time, new_expired);
            }
            return _backward(current, expired_time);
        }
        node->reset(now);
        if (node->round == 0){
            return false;
        }
        return _addTimer(node);
    }
    //printf("add node time %lld lvl %d buc %d\n", expired_us, level, bucket);
    // check if exist
    AtomicHashmap<uint64_t, std::pair<int, int> >::iterator iter = _timer_index->find(node->key);
    if (iter != _timer_index->end()) {
        TT_WARN_LOG("key %llu exist", node->key);
        return false;
    }
    // add to wheel
    if (!_wheel[level][bucket]->insert(node->key, node)) {
        TT_WARN_LOG("add timer node exist:us %lld interval %lu round %d key %lu\n",  expired_us, node->interval_us, node->round, node->key);
        return false;
    }
    // add to timer index
    if (!_timer_index->insert(node->key, std::make_pair(level, bucket))) {
        TT_WARN_LOG("add timer node conflict:us %lld interval %lu round %d key %lu\n",  expired_us, node->interval_us, node->round, node->key);
        _wheel[level][bucket]->Remove(node->key);
        return false;
    }
    //printf("add node time %lld lvl %d buc %d success!\n", expired_us, level, bucket);
    return true;
}


// this is the basic func
bool AtomicTimer::addTimer(uint64_t us_time, timer_callback func, uint64_t interval_us, int round, uint64_t key, uint64_t* newKey) {
    long long now = AtomicTimer::now(_absolute);
    long long prev = _getExpiredTime();
    long long expired_us = now + us_time;
    int level = 0;
    int bucket = 0;
    if (newKey != NULL) {
        *newKey = 0;
    }
    if (round > 1 && interval_us == 0){
        return false;
    }
    // do a easy check
    if (us_time == 0 &&round == 1) {
        //printf("expired in add\n");
        (*_func)(func, _func_arg);
        return true;
    }
    // add to timer index
    TimerNode* node = newNode(expired_us, func, interval_us, round, key);
    if (node == NULL) {
        TT_WARN_LOG("add timer create node eror:us %lu interval %lu round %d key %lu\n",  us_time, interval_us, round, key);
        return false;
    }
    if (newKey != NULL) {
        *newKey = node->key;
    }
    if (us_time == 0) {
        //printf("expired in add time = 0\n");
        (*_func)(func, _func_arg);
        node->reset(now);
        if (node->round == 0) {
            _cache->push(node);
            return true;
        }
        if (!_addTimer(node)) {
            _cache->push(node);
        }
        return true;
    }
    if (!_addTimer(node)) {
        _cache->push(node);
        return false;
    }
    return true;
}

bool AtomicTimer::addFixedTimer(int wday, int hour, int min, int sec, timer_callback func, uint64_t interval_us, int round, uint64_t key, uint64_t* newKey) {
    long long now = AtomicTimer::now(_absolute);
    time_t now_s = time(NULL);
    struct tm timenow;
    if (newKey != NULL) {
        *newKey = 0;
    }
    if (wday <= 0 && wday != -1) {
        return false;
    }
    if (round > 1 &&interval_us == 0){
        return false;
    }
    localtime_r(&now_s, &timenow);   
    int cur_wday = timenow.tm_wday;
    cur_wday = (cur_wday + 6) % 7;
    int cur_hour = timenow.tm_hour;
    int cur_min = timenow.tm_min;
    int cur_sec = timenow.tm_sec;
    int cur_seconds = 0;
    int timer_seconds = 0;
    if (wday > 0) {
        wday -= 1;
        cur_seconds = cur_wday * DAY_SECONDS + cur_hour * HOUR_SECONDS + cur_min * MIN_SECONDS + cur_sec;
        timer_seconds = wday * DAY_SECONDS + hour * HOUR_SECONDS + min * MIN_SECONDS + sec;
    } else if (hour >= 0) {
        cur_seconds = cur_hour * HOUR_SECONDS + cur_min * MIN_SECONDS + cur_sec;
        timer_seconds = hour * HOUR_SECONDS + min * MIN_SECONDS + sec;
    } else if (min >= 0) {
        cur_seconds = cur_min * MIN_SECONDS + cur_sec;
        timer_seconds = min * MIN_SECONDS + sec;
    } else if (sec >= 0) {
        cur_seconds = cur_sec;
        timer_seconds = sec;
    } else {
        TT_WARN_LOG("error time wday %d hour %d min %d sec %d", wday, hour, min, sec);
        return false;
    }
    long long diff_us = (timer_seconds - cur_seconds) * 1000000ll;
    if (diff_us > 0) {
        long long diff = diff_us - (now % 1000000);
        return addTimer(diff, func, interval_us, round, key, newKey);
    } else {
        if (interval_us <= 0) {
            TT_WARN_LOG("timer is over now: hour %d min %d sec %d interval %lu round %d key %lu",
                    hour, min, sec, interval_us, round, key);
            return false;
        }
        int diff_round = diff_us/(int64_t)interval_us;
        // forever timer
        if (round == TIMER_FOREVER_LOOP || round == 1) {
            diff_us += (-1 * diff_round + 1) * interval_us;
            return addTimer(diff_us, func, interval_us, round, key, newKey);
        } else {

            round += diff_round;
            if (round <= 0) {
                TT_WARN_LOG("timer is over now: hour %d min %d sec %d interval %lu round %d key %lu",
                        hour, min, sec, interval_us, round, key);
                return false;
            }
           diff_us += (-1 * diff_round + 1) * interval_us;
           return addTimer(diff_us, func, interval_us, round, key, newKey);
        }
    }
    return true;
}


bool AtomicTimer::addHourlyTimer(int min, int sec, timer_callback func, uint64_t key, uint64_t* newKey) {
   return addFixedTimer(-1, -1, min, sec, func, HOUR_MICROSECONDS, TIMER_FOREVER_LOOP, key, newKey);
}

bool AtomicTimer::addDailyTimer(int hour, int min, int sec, timer_callback func, uint64_t key, uint64_t* newKey) {
    return addFixedTimer(-1, hour, min, sec, func, DAY_MICROSECONDS, TIMER_FOREVER_LOOP, key, newKey);
}


bool AtomicTimer::addWeeklyTimer(int day, int hour, int min, int sec, timer_callback func, uint64_t key, uint64_t* newKey) {
    return addFixedTimer(day, hour, min, sec, func, WEEK_MICROSECONDS, TIMER_FOREVER_LOOP, key, newKey);
}

void timeout_callback(int sig_no) {
    //expire(false);
    if (AtomicTimer::global_timer_func != NULL) {
        AtomicQueue<timer_callback>::iterator iter = AtomicTimer::global_timer_func->begin();
        AtomicQueue<timer_callback>::iterator end = AtomicTimer::global_timer_func->end();
        for (; iter != end; iter++) {
            (*iter)();
        }
    }
    //printf("timer is over\n");
}

void AtomicTimer::start_timer() {
    struct sigaction act;
    act.sa_handler = timeout_callback;
    act.sa_flags   |= 0;
    act.sa_flags   |= SA_RESTART;
    sigemptyset(&act.sa_mask);
    sigaction(SIGALRM, &act, NULL);

    struct itimerval val;
    val.it_value.tv_sec = AtomicTimer::global_tick_us/1000000;
    val.it_value.tv_usec = AtomicTimer::global_tick_us;
    val.it_interval = val.it_value;
    setitimer(ITIMER_REAL, &val, NULL);
}

void AtomicTimer::stop_timer() {
    struct itimerval val;
    val.it_value.tv_sec = 0;
    val.it_value.tv_usec = 0;
    val.it_interval = val.it_value;
    setitimer(ITIMER_REAL, &val, NULL);
}



bool AtomicTimer::delTimer(uint64_t key) {
    AtomicHashmap<uint64_t, std::pair<int, int> >::iterator iter = _timer_index->find(key);
    if (iter != _timer_index->end()) {
        // first delete timer_index to avoid other delete
        if (!_timer_index->Remove(key)) {
            return false;
        }
        // delete wheel
        int level = iter->second.first;
        int bucket = iter->second.second;
        AtomicHashmap<uint64_t, TimerNode*>::iterator it = _wheel[level][bucket]->find(key);
        if (it == _wheel[level][bucket]->end()) {
            return false;
        }
        if (!_wheel[level][bucket]->Remove(key)) {
            return false;
        }
        // push node to cache for reuse mem
        _cache->push(it->second);
        return true;
    }
    return false;
}

long long AtomicTimer::expire(bool multithread) {
    // check if the time is expired
    // if absoulte then check the real time
    // if not we just add the tick and process the func
    if (multithread) {
        if (__sync_bool_compare_and_swap(&AtomicTimer::global_expired, false, true)) {
            long long t = _expire();
            AtomicTimer::global_expired = false;
            return t;
        }
        return AtomicTimer::global_tick_us;
    } else {
        return _expire();
    }
}

// get level and bucket to store it
// t is the expired time
// cur is current time
int AtomicTimer::_getPos(long long t, int& level, int& bucket) {
    // if t < cur it means the timer is over
    long long cur = AtomicTimer::global_expired_absolute_time - AtomicTimer::global_tick_us;
    if (!_absolute) {
        cur = AtomicTimer::global_expired_unabsolute_time - AtomicTimer::global_tick_us;
    }
    if (t <= cur) {
        TT_WARN_LOG("t %lld < cur %lld\n", t, cur);
        return POS_EXPIRED;
    }
    // we get the diff of the two times
    // then devide it by the level step to see which level it belongs to
    long long diff = t - cur;
    if (diff >= _level_max[_level - 1]) {
        TT_WARN_LOG("time is overflow: t %lld cur %lld diff %lld max level step %lld", t, cur, diff, _level_max[_level - 1]);
        return POS_OVERFLOW;
    }
    level = 0;
    while (diff > _level_max[level]) {
        level++;
    }
    // bucket
    bucket = _cur_index[level];
    int bucket_diff = 0;
    if (level > 0) {
        bucket_diff = (diff - _level_max[level - 1])/_bucket_step[level];
    } else {
        bucket_diff = diff/_bucket_step[level];
    }
    bucket += bucket_diff;
    bucket %= _bucket[level];
    return POS_OK;
}


// this condition is very rare
// here we just do something to make sure the timer will continue work well
long long AtomicTimer::_backward(long long current, long long expired_time) {
    // if the time is backward(rare case)
    // this conditon is very special
    // maybe reset them is a better choice
    long long prev = _getExpiredTime();
        // get all the items and reset them
        AtomicHashmap<uint64_t, std::pair<int, int> >::iterator iter = _timer_index->begin();
        for (; iter != _timer_index->end(); iter++) {
            int lvl = iter->second.first;
            int buc = iter->second.second;
            AtomicHashmap<uint64_t, TimerNode*>::iterator it = _wheel[lvl][buc]->find(iter->first);
            TimerNode* node = NULL;
            if (it != _wheel[lvl][buc]->end()) {
                node = it->second;
                node->reset(current);
                _wheel[lvl][buc]->Remove(node->key);
                _timer_index->Remove(node->key);
                // reset node
                if (node->round == TIMER_FOREVER_LOOP || node->round > 0) {
                    // insert key
                    if (!_addTimer(node)) {
                        _cache->push(node);
                    }
                }
            }
        }
    // when backward happens we just restart the conuter
    return AtomicTimer::global_tick_us;
}

bool AtomicTimer::_cascadeTimers(int level, int bucket) {
    if (level < 1 || level > _level - 1) {
        //printf("cascade lvl %d buc %d error\n", level, bucket);
        return false;
    }
    /*if (0 != (bucket%_rate[level+1])) {
        return false;
    }*/
    // push timers to level - 1
    AtomicHashmap<uint64_t, TimerNode*>::iterator iter = _wheel[level][bucket]->begin();
    AtomicHashmap<uint64_t, TimerNode*>::iterator end = _wheel[level][bucket]->end();
    for (; iter != end; iter++) {
        TimerNode* node = iter->second;
         _wheel[level][bucket]->Remove(node->key);
         _timer_index->Remove(node->key);
        // printf("cas node lvl %d buc %d time %lld\n", level, bucket, node->expired_us);
        if (!_addTimer(node)) {
            TT_FATAL_LOG("timer %lu cascade feom level %d bucket %d to level %d error", node->key, level, bucket, level - 1);
            _cache->push(node);
        }
    }
    return true;
}


// here we use a better way to expire
// as we check the expired buckets num
// and just expire the buckets member
long long AtomicTimer::_expire() {
    long long current = AtomicTimer::now(_absolute);
    long long expired_time = _getExpiredTime();
    // if the time is backward(rare case)
    // this conditon is very special
    // maybe reset them is a better choice
    if (current < expired_time - AtomicTimer::global_tick_us) {
        TT_FATAL_LOG("time is backword: cur %lld expired %lld\n", current, expired_time);
        long long diff = expired_time - AtomicTimer::global_tick_us - current;
        long long new_expired = current + (diff%AtomicTimer::global_tick_us);
        if (_absolute) {
            __sync_lock_test_and_set(&AtomicTimer::global_expired_absolute_time, new_expired);
        } else {
            __sync_lock_test_and_set(&AtomicTimer::global_expired_unabsolute_time, new_expired);
        }
        return _backward(current, expired_time);
    }
    if (current >= expired_time) {
        long long diff = current - expired_time + AtomicTimer::global_tick_us;
        // calculate the buckets
        long long buckets = diff / AtomicTimer::global_tick_us;
        long long total_buckets = buckets;
        int expired_bucket_num[MAX_LEVEL_NUM];
        int old_bucket_index[MAX_LEVEL_NUM];
        int cascade_bucket_num[MAX_LEVEL_NUM];
        int level_count = 1;
        memset(&expired_bucket_num, 0, sizeof(int)*_level);
        // set the expired time
        expired_time = expired_time + buckets * AtomicTimer::global_tick_us;
        if (_absolute) {
            __sync_lock_test_and_set(&AtomicTimer::global_expired_absolute_time, expired_time);
        } else {
            __sync_lock_test_and_set(&AtomicTimer::global_expired_unabsolute_time, expired_time);
        }
        //printf("diff = %lld buckets = %lld current %lld expired %lld\n", diff, buckets, current, expired_time);
        // cascade idx
        int cascade_level = 0;
        for (int i = 0; i < _level; ++i) {
            int idx = _cur_index[i];
            old_bucket_index[i] = idx;
            idx += buckets;
            idx %= _bucket[i];
            cascade_bucket_num[i] = buckets > _bucket[i] ? _bucket[i] : buckets;
            __sync_lock_test_and_set(&_cur_index[i], idx);
            if (i != _level - 1) {
                int tmp = buckets/_rate[i+1];
                if ((old_bucket_index[i] % _rate[i + 1]) + (buckets % _rate[i + 1]) >= _rate[i+1]) {
                    ++tmp;
                }
                buckets = tmp;
            }
            ++cascade_level;
            if (buckets <= 0) {
                break;
            }
           // printf("level %d cas buckets %d\n", cascade_level, buckets);
        }
        // reset the buckets
        buckets = total_buckets;
        // calculate the expired maps
        for (int i = 0; i < _level; ++i) {
            expired_bucket_num[i] = buckets;
            if (expired_bucket_num[i] > _bucket[i]) {
                expired_bucket_num[i] = _bucket[i];
            }
            buckets -= _bucket[i];
            //printf("expired level %d cur %d num %d\n", i, old_bucket[i], bucket[i]);
            if (buckets <= 0) {
                break;
            }
            // convert buckets to next level buckets
            if (i != _level - 1) {
                buckets /= _rate[i + 1];
            }
            level_count++;
            //printf("level %d expire buckets %d\n", level_count, buckets);
        }
        //printf("expire level num %d \n", level_count);
        // expire all the items
        for (int i = 0; i < level_count; ++i) {
            //printf("expire level %d bucket num %d\n", i, bucket[i]);
            for (int j = 0; j < expired_bucket_num[i]; ++j) {
                int buc = old_bucket_index[i] + j;
                buc %= _bucket[i];
                //printf("expire level %d bucket %d\n", i, buc);
                // find the hashmap
                // expire
                AtomicHashmap<uint64_t, TimerNode*>::iterator iter = _wheel[i][buc]->begin();
                AtomicHashmap<uint64_t, TimerNode*>::iterator end = _wheel[i][buc]->end();
                for (; iter != end; iter++) {
                    TimerNode* node = iter->second;
                    _wheel[i][buc]->Remove(node->key);
                    _timer_index->Remove(node->key);
                    // try to correct error node
                    // this nodes are inserted int the changing time    
                    // condition is rare so we just readd the node
                    if (node->expired_us > expired_time) {
                        //TT_FATAL_LOG("node error pos:node %llu round %d expired %llu current %llu expired %llu\n", node->key, node->round, node->expired_us, current, expired_time);
                     //   printf("node error pos:node %llu(level %d bucket %d) round %d expired %llu current %llu expired %llu\n", node->key, i, buc, node->round, node->expired_us, current, expired_time);
                       /* for (int k = 0; k < _level; ++k) {
                            printf("index[%d] = %d\n", k, _cur_index[k]);
                        }*/
                        // add node to next interval
                        if (!_addTimer(node)) {
                            TT_WARN_LOG("readd timer error double check\n");
                            _cache->push(node);
                        }
                        continue;
                    }
                    // check round
                   // printf("before reset node (level %d buc %d time %lld current %lld expired %lld)\n", i, buc, node->expired_us, current, expired_time);
                    node->reset(current);
                    if (node->round > 0 || node->round == TIMER_FOREVER_LOOP) {
                        // add node to next interval
                        //printf("add node loop(level %d buc %d time %lld current %lld expired %lld)\n", i, buc, node->expired_us, current, expired_time);
                        if (!_addTimer(node)) {
                            TT_WARN_LOG("readd timer error: cur %llu node %llu expired %llu\n", current, node->expired_us, expired_time);
                            _cache->push(node);
                        }
                    } else {
                  //      printf("reuse node\n");
                        // save the node for reuse
                        _cache->push(node);
                    }
                    /*    for (int k = 0; k < _level; ++k) {
                            printf("index[%d] = %d\n", k, _cur_index[k]);
                        }*/
                //    printf("expire node(level %d buc %d)\n", i, buc);
                    // run func
                    (*_func)(iter->second->callback, _func_arg);

                }
            }
        }
        // cascasde nodes
        for (int i = 1; i < cascade_level; i++) {
            //printf("cas level %d expired num %d cas num %d cur index %d old index %d\n", i, expired_bucket_num[i], cascade_bucket_num[i], _cur_index[i], old_bucket_index[i]);
            for (int j = expired_bucket_num[i]; j < cascade_bucket_num[i]; ++j) {
                _cascadeTimers(i, (j+old_bucket_index[i])%_bucket[i]);
              //  printf("cas timers level %d bucket %d\n", i, (j+old_bucket_index[i])%_bucket[i]);
            }
        } 
    }
    return expired_time - current;
}




