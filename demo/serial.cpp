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
    table.insert("serial", 0, 0, "1", 4);
    table.insert("serial", 0, 0, "1", 4);
    table.insert("serial", 0, 1, "1", 4);
    SingleTable::const_iterator iter;
    SingleTable::const_iterator end;
    // 获取全部的queue
    iter = table.begin("serial");
    end = table.end("serial");
    printf("print all serial\n");
    for (; iter != end; ++iter) {
        printf("A %d\n", iter->get<int>(A));
    }
    printf("printf all serial over\n");
    return 0;
}
