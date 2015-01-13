#include <stdio.h>
#include "memtable.h"
#include <boost/bind.hpp>

enum {
    USERS = 1,
};
enum {
    USER_ID,
    USER_NAME,
    USER_LVL,
    USER_EXP,
    USER_MONEY,
    USER_GOLD,
    USER_GANG,
    USER_ONLINE,
    USER_ALLI,
    USER_IP,
    USER_LOGINNAME,
};

void callback_30ms() {
    printf("30ms timer is expired\n");
}

void callback_500ms() {
    printf("500ms timer is expired\n");
}

void callback_3s() {
    printf("3s timer is expired\n");
}
void callback_8s() {
    printf("8s timer is expired\n");
}


int main()
{
    tinytable::MemTable::tinytable_init(true, 20000);

    tinytable::MemTable table;   
    if (0 != table.init("./conf", "tableid.conf", "./conf")) {
        printf("init table error\n");
        return -1;
    }
    // insert by table name
    if (!table.insert("users", 1ll, "测试1", 1, 0, 1000ll, 10, 1, 1, 1, "1.1.1.1", "123456")) {
        printf("write to table error\n");
        return -1;
    }
    // insert by table id
    if (!table.insert(USERS, 2ll, "测试2", 10, 10000, 10060ll, 4, 1, 1, 1, "1.1.1.2", "234567")) {
        printf("write to table error\n");
        return -1;
    }

    // serach by table name
    ItemValue ret = table.get("users", IN1(USER_ID, 1), USER_MONEY);
    if (ret.isNull()) {
        printf("find user(by name) ID(1) error\n");
    } else {
        printf("find user(by name) ID(1) money %d\n", ret.get<int>());
    }
    // search by table id
    ret = table.get(USERS, IN1(USER_ID, 1), USER_MONEY);
    if (ret.isNull()) {
        printf("find user(by id) ID(1) error\n");
    } else {
        printf("find user(by id) ID(1) money %d\n", ret.get<int>());
    }
    // search by unique key
    ret = table.get(USERS, IN1(USER_NAME, "测试2"), USER_GOLD);
    if (ret.isNull()) {
        printf("find user(by id) NAME(测试2) error\n");
    } else {
        printf("find user(by id) NAME(测试2) gold %d\n", ret.get<int>());
    }
    // id name lvl exp money gold gang online alli ip loginname
    // online gang 1 alli 1
    table.insert("users", 3ll, "测试3", 120, 1000, 1000000ll, 10000, 1, 1, 1, "1.1.1.1", "3");
    table.insert("users", 4ll, "测试4", 80, 1000, 1000000ll, 10000, 1, 1, 1, "1.1.1.1", "4");
    table.insert("users", 5ll, "测试5", 81, 1000, 1000000ll, 10000, 1, 1, 1, "1.1.1.1", "5");
    table.insert("users", 6ll, "测试6", 122, 2000, 1000001ll, 10000, 1, 1, 1, "1.1.1.1", "6");
    table.insert("users", 7ll, "测试7", 122, 1000, 1000002ll, 10000, 1, 1, 1, "1.1.1.1", "7");
    table.insert("users", 8ll, "测试8", 123, 1000, 1000004ll, 10000, 1, 1, 1, "1.1.1.1", "8");
    table.insert("users", 9ll, "测试9", 124, 1000, 1000003ll, 10000, 1, 1, 1, "1.1.1.1", "9");
    table.insert("users", 10ll, "测试10", 125, 1000, 1000006ll, 10000, 1, 1, 1, "1.1.1.1", "10");
    // online gang 2 alli 3
    table.insert("users", 11ll, "测试11", 127, 1000, 1060ll, 100, 2, 1, 3, "1.1.1.1", "11");
    table.insert("users", 12ll, "测试12", 125, 3000, 1030ll, 1000, 2, 1, 3, "1.1.1.1", "12");
    table.insert("users", 13ll, "测试13", 122, 1400, 10200ll, 10000, 2, 1, 3, "1.1.1.1", "13");
    table.insert("users", 14ll, "测试14", 80, 1500, 1000ll, 10000, 2, 1, 3, "1.1.1.1", "14");
    table.insert("users", 15ll, "测试15", 81, 1030, 100000ll, 10000, 2, 1, 3, "1.1.1.1", "15");
    table.insert("users", 16ll, "测试16", 122, 2500, 1000001ll, 10000, 2, 1, 3, "1.1.1.1", "16");
    table.insert("users", 17ll, "测试17", 122, 3000, 102ll, 10000, 2, 1, 3, "1.1.1.1", "17");
    table.insert("users", 18ll, "测试18", 123, 6600, 1000012ll, 10000, 2, 1, 3, "1.1.1.1", "18");
    table.insert("users", 19ll, "测试19", 124, 166600, 10003ll, 10000, 2, 1, 3, "1.1.1.1", "19");
    table.insert("users", 20ll, "测试20", 125, 170,6ll, 10000, 2, 1, 3, "1.1.1.1", "20");
    // offline gang 3 alli 2
    table.insert("users", 21ll, "测试21", 127, 1000, 1060ll, 100, 3, 0, 2, "1.1.1.1", "21");
    table.insert("users", 22ll, "测试22", 125, 3000, 1030ll, 1000, 3, 0, 2, "1.1.1.1", "22");
    table.insert("users", 23ll, "测试23", 122, 1400, 10200ll, 10000, 3, 0, 2, "1.1.1.1", "23");
    table.insert("users", 24ll, "测试24", 80, 1500, 1000ll, 10000, 3, 0, 2, "1.1.1.1", "24");
    table.insert("users", 25ll, "测试25", 81, 1030, 100000ll, 10000, 3, 0, 2, "1.1.1.1", "25");
    table.insert("users", 26ll, "测试26", 122, 2500, 1000001ll, 10000, 3, 0, 2, "1.1.1.1", "26");
    table.insert("users", 27ll, "测试27", 122, 3000, 102ll, 10000, 3, 0, 2, "1.1.1.1", "27");
    table.insert("users", 28ll, "测试28", 123, 6600, 1000012ll, 10000, 3, 0, 2, "1.1.1.1", "28");
    table.insert("users", 29ll, "测试29", 124, 166600, 10003ll, 10000, 3, 0, 2, "1.1.1.1", "29");
    table.insert("users", 30ll, "测试30", 125, 170,6, 10000ll, 3, 0, 2, "1.1.1.1", "30");
    // print all data
    printf("*********************************PRINT ALL USERS***********************************\n");
    SingleTable::const_iterator iter = table.begin("users");
    SingleTable::const_iterator end = table.end("users");
    for (; iter != end; ++iter) {
        printf("USER ID(%lld) name(%s) lvl(%d) exp(%d) monry (%lld) gold(%d) gang(%d) alli(%d) loginname(%s)\n",
        iter->get<int64_t>(USER_ID), iter->get<const char*>(USER_NAME), iter->get<int>(USER_LVL), iter->get<int>(USER_EXP),
        iter->get<int64_t>(USER_MONEY), iter->get<int>(USER_GOLD), iter->get<int>(USER_GANG), iter->get<int>(USER_ALLI), iter->get<std::string>(USER_LOGINNAME).c_str());
    }
    printf("*********************************PRINT ALL USERS END***********************************\n");
 



    // clear
/*    table.clear("users");
    printf("*********************************PRINT ALL USERS***********************************\n");
    iter = table.begin("users");
    end = table.end("users");
    for (; iter != end; ++iter) {
        printf("USER ID(%lld) name(%s) lvl(%d) exp(%d) monry (%lld) gold(%d) gang(%d) alli(%d) loginname(%s)\n",
        iter->get<int64_t>(USER_ID), iter->get<const char*>(USER_NAME), iter->get<int>(USER_LVL), iter->get<int>(USER_EXP),
        iter->get<int64_t>(USER_MONEY), iter->get<int>(USER_GOLD), iter->get<int>(USER_GANG), iter->get<int>(USER_ALLI), iter->get<std::string>(USER_LOGINNAME).c_str());
    }
    printf("*********************************PRINT ALL USERS END***********************************\n");
*/




    // print gang == 1
    printf("*********************************PRINT gang(1) USERS***********************************\n");
    iter = table.begin("users", IN1(USER_GANG, 1));
    end = table.end("users", IN1(USER_GANG, 1));
    for (; iter != end; ++iter) {
        printf("gang(%d) user id(%lld)\n", iter->get<int>(USER_GANG), iter->get<int64_t>(USER_ID));
    }
    printf("*********************************PRINT gang(1) USERS END***********************************\n");
    // print alli == 2
    printf("*********************************PRINT alli(2) USERS***********************************\n");
    iter = table.begin("users", IN1(USER_ALLI, 2));
    end = table.end("users", IN1(USER_ALLI, 2));
    for (; iter != end; ++iter) {
        printf("alli(%d) user id(%lld)\n", iter->get<int>(USER_ALLI), iter->get<int64_t>(USER_ID));
    }
    printf("*********************************PRINT alli(2) USERS END***********************************\n");
    // print online users
    printf("*********************************PRINT online USERS***********************************\n");
    iter = table.begin("users", IN1(USER_ONLINE, 1));
    end = table.end("users", IN1(USER_ONLINE, 1));
    for (; iter != end; ++iter) {
        printf("online(%d) user id(%lld)\n", iter->get<int>(USER_ONLINE), iter->get<int64_t>(USER_ID));
    }
    printf("*********************************PRINT online USERS END***********************************\n");
    // print lvl-exp sort
    printf("*********************************PRINT LVL-EXP SORT USERS***********************************\n");
    SingleTable::real_iterator its = table.real_begin("users", COL3(USER_ID, USER_LVL, USER_EXP));
    SingleTable::real_iterator its_end = table.real_end("users", COL3(USER_ID, USER_LVL, USER_EXP));
    for (; its != its_end; ++its) {
        printf("user(%lld) lvl(%d) exp(%d)\n", its->get<int64_t>(USER_ID), its->get<int>(USER_LVL), its->get<int>(USER_EXP));
    }
    printf("*********************************PRINT LVL-EXP SORT USERS END***********************************\n");
    // find
    SingleTable::real_iterator find_iter = table.real_find("users", COL3(USER_ID, USER_LVL, USER_EXP), 3);
    if (find_iter == its_end) {
        printf("find sort iterator error\n");
        return -1;
    }
    for (; find_iter != its_end; ++find_iter) {
        printf("find user(%lld) lvl(%d) exp(%d)\n", find_iter->get<int64_t>(USER_ID), find_iter->get<int>(USER_LVL), find_iter->get<int>(USER_EXP));
    }
    // print money sort
    printf("*********************************PRINT MONEY SORT USERS***********************************\n");
    its = table.real_begin("users", COL2(USER_ID, USER_MONEY));
    its_end = table.real_end("users", COL2(USER_ID, USER_MONEY));
    for (; its != its_end; ++its) {
        printf("user(%lld) money(%lld)\n", its->get<int64_t>(USER_ID), its->get<int64_t>(USER_MONEY));
    }
    printf("*********************************PRINT MONEY SORT USERS END***********************************\n");   
    // del
    printf("del USER(14)\n");
    if (!table.del("users", IN1(USER_ID, 14))) {
        printf("del user(14) error\n");
    } else {
        printf("del user(14) success\n");
    }
    // check del
    ret = table.get("users", IN1(USER_ID, 14), USER_GOLD);
    if (ret.isNull()) {
        printf("find user(14) null\n");
    } else {
        printf("find user(14) success\n");
    }
    // check gang del
    printf("*********************************PRINT gang(2) USERS after del user(14)***********************************\n");
    iter = table.begin("users", IN1(USER_GANG, 2));
    end = table.end("users", IN1(USER_GANG, 2));
    for (; iter != end; ++iter) {
        printf("gang(%d) user id(%lld)\n", iter->get<int>(USER_GANG), iter->get<int64_t>(USER_ID));
    }
    printf("*********************************PRINT gang(2) USERS END after del user(14)***********************************\n");
    // change user name
    if (0 != table.set("users", IN1(USER_ID, 5), USER_NAME, "修改5")) {
        printf("chang user id(5) name error\n");
    } else {
         printf("chang user id(5) name success\n");
    }
    // check chane user name
    ret = table.get(USERS, IN1(USER_NAME, "修改5"), USER_GOLD);
    if (ret.isNull()) {
        printf("find user NAME(修改5) error\n");
    } else {
        printf("find user NAME(修改5) gold %d\n", ret.get<int>());
    }
    // change login name
    if (0 != table.set("users", IN1(USER_ID, 6), USER_LOGINNAME, "登录6")) {
        printf("chang user id(6) loginname error\n");
    } else {
         printf("chang user id(6) loginname success\n");
    }
    // check change login name
    ret = table.get(USERS, IN1(USER_LOGINNAME, "登录6"), USER_GOLD);
    if (ret.isNull()) {
        printf("find user LOGINNAME(登录6) error\n");
    } else {
        printf("find user LOGINNAME(登录6) gold %d\n", ret.get<int>());
    }
    // change primary key
    if (0 != table.set("users", IN1(USER_ID, 7), USER_ID, 100)) {
        printf("chang user id(7) id error PRIMARY can not be changed!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11\n");
    } else {
         printf("chang user id(7) id success\n");
    }
    // change gang
    if (0 != table.set("users", IN1(USER_ID, 8), USER_GANG, 2)) {
        printf("chang user id(8) gang 1->2 error\n");
    } else {
         printf("chang user id(8) gang 1->2 success\n");
    }
    // print gang after change
    printf("*********************************PRINT gang(1/2) USERS after mod user(8)***********************************\n");
    iter = table.begin("users", IN1(USER_GANG, 1));
    end = table.end("users", IN1(USER_GANG, 1));
    for (; iter != end; ++iter) {
        printf("gang(%d) user id(%lld)\n", iter->get<int>(USER_GANG), iter->get<int64_t>(USER_ID));
    }
    iter = table.begin("users", IN1(USER_GANG, 2));
    end = table.end("users", IN1(USER_GANG, 2));
    for (; iter != end; ++iter) {
        printf("gang(%d) user id(%lld)\n", iter->get<int>(USER_GANG), iter->get<int64_t>(USER_ID));
    }
    printf("*********************************PRINT gang(1/2) USERS END after mod user(8)***********************************\n");

    // change online
    if (0 != table.set("users", IN1(USER_ID, 8), USER_ONLINE, 0)) {
        printf("chang user id(8) online 1->0 error\n");
    } else {
         printf("chang user id(8) online 1->0 success\n");
    }
    // print online after change
    printf("*********************************PRINT online(1/0) USERS after mod user(8)***********************************\n");
    iter = table.begin("users", IN1(USER_ONLINE, 1));
    end = table.end("users", IN1(USER_ONLINE, 1));
    for (; iter != end; ++iter) {
        printf("online(%d) user id(%lld)\n", iter->get<int>(USER_ONLINE), iter->get<int64_t>(USER_ID));
    }
    iter = table.begin("users", IN1(USER_ONLINE, 0));
    end = table.end("users", IN1(USER_ONLINE, 0));
    for (; iter != end; ++iter) {
        printf("online(%d) user id(%lld)\n", iter->get<int>(USER_ONLINE), iter->get<int64_t>(USER_ID));
    }
    printf("*********************************PRINT gang(1/2) USERS END after mod user(8)***********************************\n");
    // change money
    if (0 != table.dec("users", IN1(USER_ID, 20), USER_MONEY, 100)) {
        printf("dec user(20) money error\n");
    } else {
        printf("dec user(20) money 100 success\n");
    }
    // check change money
    ret = table.get("users", IN1(USER_ID, 20), USER_MONEY);
    if (ret.isNull()) {
        printf("get usr(20) money error\n");
    } else {
        printf("get usr(20) money %lld\n", ret.get<int64_t>());
    }
    // check exp
    if (0 != table.add("users", IN1(USER_ID, 26), USER_EXP, 1000000)) {
        printf("dec user(26) exp error\n");
    } else {
        printf("dec user(26) exp 1000000 success\n");
    }
    // check sort after change exp
    printf("*********************************PRINT LVL-EXP SORT USERS***********************************\n");
    its = table.real_begin("users", COL3(USER_ID, USER_LVL, USER_EXP));
    its_end = table.real_end("users", COL3(USER_ID, USER_LVL, USER_EXP));
    for (; its != its_end; ++its) {
        printf("user(%lld) lvl(%d) exp(%d)\n", its->get<int64_t>(USER_ID), its->get<int>(USER_LVL), its->get<int>(USER_EXP));
    }
    printf("*********************************PRINT LVL-EXP SORT USERS END***********************************\n");
    // change ip
    if (0 != table.set("users", IN1(USER_ID, 22), USER_IP, "127.0.0.1")) {
        printf("set user(22) ip=127.0.0.1 error\n");
    } else {
        printf("set user(22) ip=127.0.0.1 success\n");
    }
    // dec money
    if (0 != table.dec("users", IN1(USER_ID, 23), USER_MONEY, 10, NULL)) {
        printf("dec user(23) money to db error\n"); 
    } else {
        printf("dec user(23) money to db success\n"); 
    }
    // set ip
    if (0 != table.set("users", IN1(USER_ID, 24), USER_IP, "127.1.1.1", NULL)) {
        printf("set user(24) ip=127.1.1.1 to cache error\n");
    } else {
        printf("set user(24) ip=127.1.1.1 to cache success\n");
    }
    /************************************timer test*************************************************/
    if(!tinytable::MemTable::addTimer(30000ll, boost::bind(&callback_30ms), 0, 1, 1)) {
        printf("add 30ms timer error\n");
        return -1;
    }
    if(!tinytable::MemTable::addTimer(500000ll, boost::bind(&callback_500ms), 500000ll, 2, 2)) {
        printf("add 500ms timer error\n");
        return -1;
    }
    if(!tinytable::MemTable::addTimer(3000000ll, boost::bind(&callback_3s), 1000000ll, 3, 3)) {
        printf("add 3s timer error\n");
        return -1;
    }
     if(!tinytable::MemTable::addTimer(8000000ll, boost::bind(&callback_8s), 1000000ll, 1, 88)) {
        printf("add 8s timer error\n");
        return -1;
    }
    while (true) {
        sleep(100000);
    }
    tinytable::MemTable::tinytable_destroy();

    return 0;
}
