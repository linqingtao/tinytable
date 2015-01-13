#include "common.h"
#include <ifaddrs.h>  
#include <arpa/inet.h>  

const long long COMMON_MIN_SECONDS = 60;
const long long COMMON_HOUR_SECONDS = 60 * 60;
const long long COMMON_DAY_SECONDS = 24 * 60 * 60;
const long long COMMON_WEEK_SECONDS = 7 * 24 * 60 * 60;

int triggerTimes(int weekday, int hour, int min, int sec, time_t last_time, time_t now) {
    int times = 0;
    if (now <= last_time) {return 0;}
    struct tm timenow;
    struct tm timelast;
    long long intervals = 0;
    int cur_seconds = 0;
    int last_seconds = 0;
    int trigger_seconds = 0;
    if (weekday != -1) {
        intervals = COMMON_WEEK_SECONDS;
        weekday -= 1;
        if (weekday < 0) {
            weekday = 0;
        }
    } else if (hour != -1) {
        intervals = COMMON_DAY_SECONDS;
    } else if (min != -1) {
        intervals = COMMON_HOUR_SECONDS;
    } else if (sec != -1) {
        intervals = COMMON_MIN_SECONDS;
    } else {
        return 0;
    }
    localtime_r(&now, &timenow);   
    int cur_wday = timenow.tm_wday;
    cur_wday = (cur_wday + 6) % 7;
    int cur_hour = timenow.tm_hour;
    int cur_min = timenow.tm_min;
    int cur_sec = timenow.tm_sec;
    localtime_r(&last_time, &timelast);   
    int last_wday = timelast.tm_wday;
    last_wday = (last_wday + 6) % 7;
    int last_hour = timelast.tm_hour;
    int last_min = timelast.tm_min;
    int last_sec = timelast.tm_sec;
    if (weekday != -1) {
        cur_seconds = cur_wday * COMMON_DAY_SECONDS + cur_hour * COMMON_HOUR_SECONDS + cur_min * COMMON_MIN_SECONDS + cur_sec;
        trigger_seconds = weekday * COMMON_DAY_SECONDS + hour * COMMON_HOUR_SECONDS + min * COMMON_MIN_SECONDS + sec;
        last_seconds = last_wday * COMMON_DAY_SECONDS + last_hour * COMMON_HOUR_SECONDS + last_min * COMMON_MIN_SECONDS + last_sec;
    } else if (hour != -1) {
        cur_seconds = cur_hour * COMMON_HOUR_SECONDS + cur_min * COMMON_MIN_SECONDS + cur_sec;
        trigger_seconds = hour * COMMON_HOUR_SECONDS + min * COMMON_MIN_SECONDS + sec;
        last_seconds = last_hour * COMMON_HOUR_SECONDS + last_min * COMMON_MIN_SECONDS + last_sec;
    } else if (min != -1) {
        cur_seconds = cur_min * COMMON_MIN_SECONDS + cur_sec;
        trigger_seconds = min * COMMON_MIN_SECONDS + sec;
        last_seconds = last_min * COMMON_MIN_SECONDS + last_sec;
    } else if (sec != -1) {
        cur_seconds = cur_sec;
        trigger_seconds = sec;
        last_seconds = last_sec;
    }
    last_time -= (last_seconds - trigger_seconds);
    now -= (cur_seconds - trigger_seconds);
    times = (now - last_time)/intervals;
    if (last_seconds >= trigger_seconds && cur_seconds < trigger_seconds) {
        --times;
    } else if (last_seconds < trigger_seconds && cur_seconds >= trigger_seconds) {
        ++times;
    }
    return times;
}


std::string getBroadcastIP(std::string net) {
    struct sockaddr_in *sin = NULL;
    struct ifaddrs *ifa = NULL, *ifList;

    if (getifaddrs(&ifList) < 0)
    {
        return "";
    }

    for (ifa = ifList; ifa != NULL; ifa = ifa->ifa_next)
    {
        if(ifa->ifa_addr->sa_family == AF_INET)
        {
            if (ifa->ifa_name == net) {
            sin = (struct sockaddr_in *)ifa->ifa_dstaddr;
            return inet_ntoa(sin->sin_addr);
            }
        }
    }
    freeifaddrs(ifList);
    return "";
}

