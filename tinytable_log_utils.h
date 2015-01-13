#ifndef __LOG_UTILS_H
#define __LOG_UTILS_H

#include <unistd.h>
#include <stdio.h>
#if !defined(TT_WARN_LOG)
#define TT_WARN_LOG printf
#endif
#if !defined(TT_DEBUG_LOG)
#define TT_DEBUG_LOG printf
#endif
#if !defined(TT_FATAL_LOG)
#define TT_FATAL_LOG printf
#endif
#endif
