#include <stdio.h>
#include "memtable.h"
#include "config.h"


enum {A, B, C, D};

int main()
{
    if (!tinytable::MemTable::tinytable_init(true)) {
        printf("tinytable global init error\n");
        return -1;
    }
    tinytable::MemTable table;
    if (0 != table.init("./conf", "tableid.conf", "./conf")) {
        printf("init tinytable error\n");
        return -1;
    }
    // 插入3条数据
    table.insert("cluster_queue", 0, 0, "1", 4);
    table.insert("cluster_queue", 1, 0, "1", 4);
    table.insert("cluster_queue", 1, 1, "1", 4);
    SingleTable::real_iterator iter;
    SingleTable::real_iterator end;
    // 获取全部的queue
    iter = table.queue_begin("cluster_queue", IN1(A, 1));
    end = table.queue_end("cluster_queue", IN1(A, 1));
    printf("print all queue\n");
    for (; iter != end; ++iter) {
        printf("A %d B %d\n", iter->get<int>(A), iter->get<int>(B));
    }
    printf("printf all queue over\n");
 
    // 插入4条数据
    table.insert("cluster_queue", 1, 0, "1", 4);
    table.insert("cluster_queue", 1, 0, "1", 4);
    table.insert("cluster_queue", 1, 1, "1", 4);
    // 获取全部的queue数据
    iter = table.queue_begin("cluster_queue", IN1(A, 1));
    end = table.queue_end("cluster_queue", IN1(A, 1));
    printf("print all queue\n");
    for (; iter != end; ++iter) {
        printf("A %d B %d\n", iter->get<int>(A), iter->get<int>(B));
    }
    printf("printf all queue over\n");
    return 0;
}
