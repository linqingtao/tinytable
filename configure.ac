#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)

AC_INIT([tinytable], 1.0, [])

# We assume all revisions are backwards incompatible.

#AC_CONFIG_SRCDIR([Likely.h])
AC_CONFIG_HEADERS([tinytable_config.h])
#AX_PREFIX_CONFIG_H([folly-config.h], [folly], [config.h])
#AC_CONFIG_AUX_DIR([build-aux])

AM_INIT_AUTOMAKE([foreign dist-bzip2 nostdinc subdir-objects])
AM_INIT_AUTOMAKE([tinytable], 1.0)
myscript = `protoc -I=. *.proto --cpp_out=.`
AC_CONFIG_FILES([${myscript}])

AC_CONFIG_MACRO_DIR([m4])

AC_PROG_INSTALL
AM_PROG_LIBTOOL

AC_LANG([C++])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_CXX_COMPILE_STDCXX_0X

# Be sure to add any -std option to CXXFLAGS before we invoke any
# AC_COMPILE_IFELSE() or similar macros. Any such macros that are invoked
# before we update CXXFLAGS will not be run with the same options that we use
# during the real build.
STD=""
if test "x$ac_cv_cxx_compile_cxx0x_cxx" = xyes; then
   STD="-std=c++0x"
fi
if test "x$ac_cv_cxx_compile_cxx0x_gxx" = xyes; then
   STD="-std=gnu++0x"
fi

CXXFLAGS="$STD $CXXFLAGS"

# Checks for libraries.
# check for boost libs
AX_BOOST_BASE([1.20.0], [], [AC_MSG_ERROR(
              [Please install boost >= 1.20.0 (thread, regex, and system)])])
AX_BOOST_THREAD
AX_BOOST_REGEX
AX_BOOST_SYSTEM
AX_BOOST_FILESYSTEM

# Checks for header files.
AC_HEADER_STDC

AC_CHECK_HEADERS([fcntl.h], [], [AC_MSG_ERROR([header fcntl.h not found])])
AC_CHECK_HEADERS([inttypes.h], [], [AC_MSG_ERROR([header inttypes.h not found])])
AC_CHECK_HEADERS([limits.h], [], [AC_MSG_ERROR([header limits.h not found])])
AC_CHECK_HEADERS([stdint.h], [], [AC_MSG_ERROR([header stdint.h not found])])
AC_CHECK_HEADERS([stdlib.h], [], [AC_MSG_ERROR([header stdlib.h not found])])
AC_CHECK_HEADERS([string.h], [], [AC_MSG_ERROR([header string.h not found])])
AC_CHECK_HEADERS([sys/time.h], [], [AC_MSG_ERROR([header sys/time.h not found])])
AC_CHECK_HEADERS([unistd.h], [], [AC_MSG_ERROR([header unistd.h not found])])
AC_CHECK_HEADERS([tinyxml.h], [], [AC_MSG_ERROR([header tinyxml.h not found])])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_C_INLINE
AC_TYPE_SIZE_T
AC_HEADER_TIME
AC_C_VOLATILE
AC_CHECK_TYPE([__int128],
  [AC_DEFINE([HAVE_INT128_T], [1], [Define if __int128 exists])],
  [AC_DEFINE([HAVE_INT128_T], [0], [Define if __int128 does not exist])])
AC_CHECK_TYPES([ptrdiff_t])

AC_CACHE_CHECK(
  [for ifunc support],
  [tinytable_cv_prog_cc_ifunc],
  [AC_COMPILE_IFELSE(
    [AC_LANG_SOURCE[
      #pragma GCC diagnostic error "-Wattributes"
      extern "C" void (*test_ifunc(void))() { return 0; }
      void func() __attribute__((ifunc("test_ifunc")));]
    ],
    [tinytable_cv_prog_cc_ifunc=yes],
    [tinytable_cv_prog_cc_ifunc=no])])

if test "$tinytable_cv_prog_cc_ifunc" = "yes"; then
  AC_DEFINE([HAVE_IFUNC], [1], [Define to 1 if the compiler supports ifunc])
fi

AC_CACHE_CHECK(
  [for final and override support],
  [tinytable_cv_c_final_override],
  [AC_COMPILE_IFELSE(
    [AC_LANG_SOURCE[class C { virtual void f() final {} virtual void g() {} };
                    class D : public C { virtual void g() override {} };]],
    [tinytable_cv_c_final_override=yes],
    [tinytable_cv_c_final_override=no])])

if test "$tinytable_cv_c_final_override" = "yes"; then
  final_val=final
  override_val=override
else
  final_val=
  override_val=
fi

AC_DEFINE_UNQUOTED(
  [FINAL], [$final_val],
  [Define to "final" if the compiler supports C++11 "final"])
AC_DEFINE_UNQUOTED(
  [OVERRIDE], [$override_val],
  [Define to "override" if the compiler supports C++11 "override"])


AC_CACHE_CHECK(
  [for libc++],
  [tinytable_cv_lib_libcpp],
  [AC_COMPILE_IFELSE(
    [AC_LANG_SOURCE[
      #include <type_traits>
      #if !_LIBCPP_VERSION
      #error No libc++
      #endif
      void func() {}]
    ],
    [tinytable_cv_lib_libcpp=yes],
    [tinytable_cv_lib_libcpp=no])])

if test "$tinytable_cv_lib_libcpp" = yes; then
  AC_DEFINE([USE_LIBCPP], [1], [Define to 1 if we are using libc++.])
fi

# Check for clock_gettime(2). This is not in an AC_CHECK_FUNCS() because we
# want to link with librt if necessary.
AC_SEARCH_LIBS([clock_gettime], [rt],
  AC_DEFINE(
    [HAVE_CLOCK_GETTIME],
    [1],
    [Define to 1 if we support clock_gettime(2).]),
  [])

# Checks for library functions.
AC_CHECK_FUNCS([getdelim \
                gettimeofday \
                memmove \
                memset \
                pow \
                strerror \
                pthread_yield \
                malloc_size \
                malloc_usable_size \
                memrchr \
                pipe2])

if test "$ac_cv_func_pthread_yield" = "no"; then
   AC_CHECK_HEADERS([sched.h])
   AC_CHECK_FUNCS([sched_yield])
fi

AM_CPPFLAGS='-I$(top_srcdir)/..'
AM_CPPFLAGS="$AM_CPPFLAGS $BOOST_CPPFLAGS"
AM_LDFLAGS="$AM_LDFLAGS $BOOST_THREAD_LIB $BOOST_FILESYSTEM_LIB"
AM_LDFLAGS="$AM_LDFLAGS $BOOST_SYSTEM_LIB $BOOST_REGEX_LIB -lpthread"

AC_SUBST([AM_CPPFLAGS])
AC_SUBST([AM_LDFLAGS])

AM_CONDITIONAL([HAVE_X86_64], [test "$build_cpu" = "x86_64"])
AM_CONDITIONAL([HAVE_LINUX], [test "$build_os" == "linux-gnu"])

# Output
AC_CONFIG_FILES([Makefile
                 client/Makefile
                 demo/Makefile])
AC_OUTPUT
