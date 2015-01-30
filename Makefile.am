ACLOCAL_AMFLAGS = -I m4

AUTOMAKE_OPTIONS=foreign

lib_LIBRARIES=libtinytable.a
libtinytable_a_SOURCES = \
	item_array.cpp \
	atomiccache.cpp \
	proto_config.cpp \
	tlsbuffer.cpp \
	memtable.cpp \
	common.cpp \
	table.cpp \
	xml_parser.cpp \
	atomicbitmap.cpp \
	sign.cpp \
	config.cpp \
	atomictimer.cpp \
	locks.cpp \
	item.cpp \
	serial_table.cpp \
	table_index.cpp \
	tinytable.pb.cc

tinytableincludedir = $(includedir)/tinytable

nobase_tinytableinclude_HEADERS = \
	sign.h \
	table_index.h \
	atomicqueue.h \
	tlsbuffer.h \
	item_array.h \
	proto_config.h \
	tinytable.pb.h \
	atomiccache.h \
	common.h \
	atomicblockqueue.h \
	tinytable_log_utils.h \
	memtable.h \
	atomicskiplist.h \
	config.h \
	xml_parser.h \
	list_map.h \
	locks.h \
	item.h \
	serial_table.h \
	random.h \
	table.h \
	atomichashmap.h \
	atomicsortmap.h \
	atomictimer.h \
	atomicbitmap.h \
	list_set.h \
	tls.h
