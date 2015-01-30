AUTOMAKE_OPTIONS=foreign

bin_PROGRAMS = demo cluster_queue queue serial
demo_SOURCES = demo.cpp
demo_LDADD=../libtinytable.a -lprotobuf -ltinyxml

cluster_queue_SOURCES = cluster_queue.cpp
cluster_queue_LDADD = ../libtinytable.a -lprotobuf -ltinyxml

queue_SOURCES = queue.cpp
queue_LDADD = ../libtinytable.a -lprotobuf -ltinyxml

serial_SOURCES = serial.cpp
serial_LDADD = ../libtinytable.a -lprotobuf -ltinyxml

INCLUDES=-I ../ -I/usr/include/
