#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <pthread.h>
#include <time.h>
#include <string>
#include "linenoise.h"
#include "process.h"

#if defined(__DATE__) && defined(__TIME__)
static const char SERVER_BUILT[] = __DATE__ " " __TIME__;
#else
static const char SERVER_BUILT[] = "unknown";
#endif

#ifdef VERSION
const char *VERSION_ID = VERSION;
#else
const char *VERSION_ID = "unknown";
#endif


static void dn_print_version(void)
{                       
        printf(" Version\t:\t%s\n", VERSION_ID);
        printf(" Built date\t:\t%s\n", SERVER_BUILT);
}

void dn_print_help(void)   
{               
        printf( "%s", "usage\n");
        printf( "%s", "    --help     | -h     :      print help message\n");
        printf( "%s", "    --version  | -v     :      print version\n");
        printf( "%s", "    --ip       | -i     :      host ip\n");
        printf( "%s", "    --port     | -p     :      host port\n");
        printf( "%s", "    --command  | -c     :      execute command\n");
} 


int main(int argc, char **argv)
{
    char c;
    const char* const short_options = "vhc:i:p:"; 
    const struct option long_options[] = 
    {
         {"version", no_argument, NULL, 'v'},
         {"help", no_argument, NULL, 'h'},
         {"ip", optional_argument, NULL, 'i'},
         {"port", optional_argument, NULL, 'p'},
         {"command", optional_argument, NULL, 'c'},
         {0, 0, 0, 0}
    };
    std::string ip = "127.0.0.1";
    int port = 3234;
    std::string command = "";
    bool flag = false;
    while ((c = getopt_long(argc, argv, short_options, long_options, NULL)) != -1)
    {
        switch (c)
        {
            case 'h':
                dn_print_help();
                exit(1);
            case 'v':
                dn_print_version();
                exit(1);
            case 'c':
                command = optarg;
                flag = true;
                break;
            case 'i':
                ip = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            default:
                dn_print_help();
                exit(1);
        }
    }
    SocketClient client;
    if (!client.init("client", ip.c_str(), port, 1, 200, 10000, 100)) {
        printf("connect %s:%d error\n", ip.c_str(), port);
        exit(1);
    }
    char* res = NULL;
    res = new(std::nothrow) char[100 * 1024 + 1];
    if (res == NULL) {
        printf("new buffer error\n");
        exit(1);
    }
    // if input is command mode then execute the command
    if (flag) {
        // process command
        process(&client, res, ip, port, command);   
        exit(1);
    }
    linenoiseHistoryLoad(".tinytable_history");
    char *line;
    char info[100];
    snprintf(info, sizeof(info), "%s:%d> ", ip.c_str(), port);
    while((line = linenoise(info)) != NULL) {
        if (line[0] != '\0' && line[0] != '/') {
            linenoiseHistoryAdd(line); /* Add to the history. */
            linenoiseHistorySave(".tinytable_history"); /* Save the history on disk. */
        }
        process(&client, res, ip, port, line);
        free(line);
    }
    return 0;
}
