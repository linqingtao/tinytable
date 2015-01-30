AUTOMAKE_OPTIONS=foreign

bin_PROGRAMS = client
client_SOURCES = tcp_socket.cpp main.cpp connectpool.cpp netpool.cpp process.cpp anyType.cpp sql_parser.cpp linenoise.c
#demo_LDADD=../libtinytable.a -lprotobuf -ltinyxml
client_LDADD=../libtinytable.a

INCLUDES=-I ./ -I ../ -I/usr/include/
