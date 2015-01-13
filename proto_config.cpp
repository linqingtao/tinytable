#include <stdio.h>
#include "xml_parser.h"
#include "tinytable.pb.h"
#include "proto_config.h"
#include "serial_table.h"

bool ProtoToConfig::loadTableIDConfig(const char* path, const char* file, TableIDConfig* config) {
    if (path == NULL ||file == NULL || config == NULL) {
        TT_WARN_LOG("params null: path %p file %p config %p", path, file, config);
        return false;
    }
    config->table_id_name_index.clear();
    config->table_name_id_index.clear();
    char fullfile[2049];
    snprintf(fullfile, sizeof(fullfile), "%s/%s", path, file);
    // open file
    FILE* fp = fopen(fullfile, "r");
    if (fp == NULL) {
        TT_WARN_LOG("open tableid file %s error", fullfile);
        return true;
    }
    char buffer[1024];
    int start_id = 1;
    // read line
    while (fgets(buffer, 1024, fp) != NULL) {
        char* mid = strstr(buffer, ":");
        int id;
        if (mid == NULL) {
            id = start_id++;
        } else {
            *mid = 0;
            id = atoi(mid + 1);
        }
        char* end = strstr(buffer, "\n");
        if (end != NULL) {
            *end = 0;
        }
        end = strstr(buffer, "\r");
        if (end != NULL) {
            *end = 0;
        }
        // add
        if(!config->table_id_name_index.insert(id, buffer)) {
            TT_WARN_LOG("add table %s id %d error", buffer, id);
            return false;
        }
        if (!config->table_name_id_index.insert(buffer, id)) {
            TT_WARN_LOG("add table %s id %d error", buffer, id);
            return false;
        }
    }
    fclose(fp);
    fp = NULL;
    return true;
}

bool ProtoToConfig::loadTableXmlConfig(const char* path, const char* file, TableConfig* config, TableIDConfig* idconfig) {
    if (path == NULL ||file == NULL || config == NULL) {
        TT_WARN_LOG("params null: path %p file %p config %p", path, file, config);
        return false;
    }
    tinytable::tinytable  table;
    if (!tinytable::xml2pb(path, file, &table)) {
        TT_WARN_LOG("parse xml %s/%s to pb error", path, file);
        return false;
    }
    // get id
    if (idconfig != NULL) {
        AtomicHashmap<std::string, int>::iterator tableid_iter = idconfig->table_name_id_index.find(table.name());
        if (tableid_iter == idconfig->table_name_id_index.end()) {
            TT_WARN_LOG("table %s not in tableid config", table.name().c_str());
            config->table_id = TableConfig::generateID();
        } else {
            config->table_id = tableid_iter->second;
        }
    } else {
        config->table_id = TableConfig::generateID();
    }
    // push items to config
    config->servercol = -1;
    config->load_type = table.has_loadtype() ? (table.loadtype() == "all" ? 0 : 1) : 1;
    config->updatetype = table.has_updatetype() ? (table.updatetype() == "realtime" ? 1 : 0) : 0;
    config->table_name = table.name();
    config->table_type = 2;
    if (table.has_type()) {
        if (table.type() == "db") config->table_type = 0;
        else if (table.type() == "cache") config->table_type = 1;
    }
    std::string firstsource = "cache";
    if (table.has_firstsource()) {
        firstsource = table.firstsource();
    }
    config->firstsource = firstsource == "db" ? 0 : 1;
    config->table_sign = get_sign64(config->table_name.c_str(), config->table_name.length());
    config->expired_time = table.has_expired() ? table.expired() : -1;
    int column_num = table.columns().column_size();
    config->column_num = column_num;
    config->index = new(std::nothrow) short[column_num];
    int type_len = 0;
    
    int prev = 0;
    int string_cnt = 0;
    int string_end = 0;
    int cnt = 0;

    if (NULL == config->index) {       
        TT_WARN_LOG("create index null: %d", column_num);
        goto TABLEERROR;
    }
    memset(config->index, 0, sizeof(short) * column_num);
    config->column_names = new(std::nothrow) std::string[column_num];
    if (NULL == config->column_names) {
        TT_WARN_LOG("create column_name null: %d", column_num);
        goto TABLEERROR;
    }
    config->types = new(std::nothrow) char[column_num];
    if (NULL == config->types) {
        TT_WARN_LOG("create typs null: %d", column_num);
        goto TABLEERROR;
    }
    config->limits = new(std::nothrow) int[column_num];
    if (NULL == config->limits) {
        TT_WARN_LOG("create limits error");
        goto TABLEERROR;
    }
    config->column_name_signs = new(std::nothrow) uint64_t[column_num];
    if (NULL == config->column_name_signs) {
        TT_WARN_LOG("create column name signs error");
        goto TABLEERROR;
    }
    config->column_map = new(std::nothrow) AtomicHashmap<uint64_t, int>();
    if (NULL == config->column_map) {
        TT_WARN_LOG("create column map error");
        goto TABLEERROR;
    }
    config->primary_keys.clear();
    config->unique_keys.clear();
    config->sort_keys.clear();
    config->queue_keys.clear();
    config->triggers.clear();
    config->index[0] = 0;

    // column_names
    for (int i = 0; i < column_num; i++) {
        const tinytable::column& col = table.columns().column(i);
        config->column_names[i] = col.name();
        std::string data_type = col.datatype();
        if (data_type == "bool") {
            config->types[i] = OBJECT_BOOL;
        } else if (data_type == "uint8") {
            config->types[i] = OBJECT_UINT8;
        } else if (data_type =="int8") {
            config->types[i] = OBJECT_INT8;
        } else if (data_type == "uint16") {
            config->types[i] = OBJECT_UINT16;
        } else if (data_type == "int16") {
            config->types[i] = OBJECT_INT16;
        } else if (data_type == "uint32") {
            config->types[i] = OBJECT_UINT32;
        } else if (data_type == "int32") {
            config->types[i] = OBJECT_INT32;
        } else if (data_type == "uint64") {
            config->types[i] = OBJECT_UINT64;
        } else if (data_type == "int64") {
            config->types[i] = OBJECT_INT64;
        } else if (data_type == "float") {
            config->types[i] = OBJECT_FLOAT;
        } else if (data_type == "double") {
            config->types[i] = OBJECT_DOUBLE;
        } else if (data_type == "string") {
            config->types[i] = OBJECT_STRING;
            config->string_cnt++;
        } else {
            TT_WARN_LOG("error type %s", data_type.c_str());
            goto TABLEERROR;
        }
        config->limits[i] = col.has_maxlen() ? col.maxlen() : std::numeric_limits<int>::max();
        config->column_name_signs[i] = get_sign64(col.name().c_str(), col.name().length());
        config->column_map->insert(config->column_name_signs[i], i);
        if (col.has_is_primary() && col.is_primary()) {
            config->primary_keys.insert(i);
        }
        if (col.has_serial() && col.serial()) {
            if (config->serial_col != -1) {
                TT_WARN_LOG("serial col only allow one");
                goto TABLEERROR;
            }
            config->serial_col = i;
        }

    }
    // count the column index
    // make it -6 as when there is no string
    // -6 + 8 = 2 will be the start
    string_end = -6;
    for (int i = 0; i < column_num; i++) {
        const tinytable::column& col = table.columns().column(i);
        std::string data_type = col.datatype();   
        if (data_type == "string") {
            if (string_cnt == 0) {
                config->index[i] = 2;
            } else {
                config->index[i] = config->index[prev] + 8;
            }
            prev = i;
            ++string_cnt;
            string_end = config->index[i];
        }
    }
    prev = 0;
    for (int i = 0; i < column_num; i++) {
        const tinytable::column& col = table.columns().column(i);
        std::string data_type = col.datatype();   
        if (data_type == "string") { continue;}
        if (cnt == 0) {
            config->index[i] = string_end + 8;
        } else {
            config->index[i] = config->index[prev] + type_len;
        }
        switch(config->types[i])
        {
            case OBJECT_BOOL:
            case OBJECT_INT8:
            case OBJECT_UINT8: type_len = 1;break;
            case OBJECT_INT16:
            case OBJECT_UINT16: type_len = 2; break;
            case OBJECT_INT32:
            case OBJECT_FLOAT:
            case OBJECT_UINT32: type_len = 4; break;
            case OBJECT_INT64:
            case OBJECT_UINT64:
            case OBJECT_DOUBLE:
            case OBJECT_STRING: type_len = 8; break;
            default: TT_WARN_LOG("error type %d", config->types[i]);goto TABLEERROR;
        }
        prev = i;
        ++cnt;
    }
    // servercol
    if (table.has_servercol()) {
        config->servercol = config->columnNameToID(table.servercol().c_str());
    }
    // unique cluster sort
    if (table.has_uniques()) {
        int unique_num = table.uniques().unique_size();
        for (int i = 0; i < unique_num; ++i) {
            int num = table.uniques().unique(i).col_size();
            TableConfig::UniqueIndex index;
            for (int j = 0; j < num; ++j) {
                int col_id = config->columnNameToID(table.uniques().unique(i).col(j).name().c_str());
                if (col_id == -1) {
                    TT_WARN_LOG("unique col %s error", table.uniques().unique(i).col(j).name().c_str());
                    goto TABLEERROR;
                }
                index.keys.insert(col_id);
            }
            config->unique_keys.push_back(index);
        }
    }
    if (table.has_clusters()) {
        int cluster_num = table.clusters().cluster_size();
        for (int i = 0; i < cluster_num; ++i) {
            int num = table.clusters().cluster(i).master().col_size();
            TableConfig::ClusterIndex index;
            for (int j = 0; j < num; ++j) {
                int col_id = config->columnNameToID(table.clusters().cluster(i).master().col(j).name().c_str());
                if (col_id == -1) {
                    TT_WARN_LOG("unique  cluster-- unique col %s error", table.clusters().cluster(i).master().col(j).name().c_str());
                    goto TABLEERROR;
                }
                index.unique_keys.insert(col_id);
            }
            num = table.clusters().cluster(i).slave().col_size();
            for (int j = 0; j < num; ++j) {
                int col_id = config->columnNameToID(table.clusters().cluster(i).slave().col(j).name().c_str());
                if (col_id == -1) {
                    TT_WARN_LOG("unique cluster -- cluster col %s error", table.clusters().cluster(i).slave().col(j).name().c_str());
                    goto TABLEERROR;
                }
                index.cluster_keys.insert(col_id);
            }
            config->cluster_keys.push_back(index);
        }
    }
    if (table.has_sorts()) {
        int sort_num = table.sorts().sort_size();
        for (int i = 0; i < sort_num; ++i) {
            int num = table.sorts().sort(i).master().col_size();
            TableConfig::SortIndex index;
            index.desc = true;
            if (table.sorts().sort(i).has_type() && table.sorts().sort(i).type() == "asc") {
                index.desc = false;
            }
            for (int j = 0; j < num; ++j) {
                int col_id = config->columnNameToID(table.sorts().sort(i).master().col(j).name().c_str());
                if (col_id == -1) {
                    TT_WARN_LOG("unique  sort-- unique col %s error", table.sorts().sort(i).master().col(j).name().c_str());
                    goto TABLEERROR;
                }
                index.unique_keys.insert(col_id);
            }
            num = table.sorts().sort(i).slave().col_size();
            for (int j = 0; j < num; ++j) {
                int col_id = config->columnNameToID(table.sorts().sort(i).slave().col(j).name().c_str());
                if (col_id == -1) {
                    TT_WARN_LOG("unique cluster -- cluster col %s error", table.sorts().sort(i).slave().col(j).name().c_str());
                    goto TABLEERROR;
                }
                index.sort_keys.insert(col_id);
            }
            config->sort_keys.push_back(index);
        }
    }
    if (table.has_queues()) {
        int queue_num = table.queues().queue_size();
        for (int i = 0; i < queue_num; ++i) {
            int num = table.queues().queue(i).master().col_size();
            TableConfig::QueueIndex index;
            index.desc = false;
            index.max_size = -1;
            if (table.queues().queue(i).has_type() && table.queues().queue(i).type() == "desc") {
                index.desc = true;
            }
            if (table.queues().queue(i).has_max_size() && table.queues().queue(i).max_size() > 0) {
                index.max_size = table.queues().queue(i).max_size();
            }
            for (int j = 0; j < num; ++j) {
                int col_id = config->columnNameToID(table.queues().queue(i).master().col(j).name().c_str());
                if (col_id == -1) {
                    TT_WARN_LOG("unique  queue-- unique col %s error", table.queues().queue(i).master().col(j).name().c_str());
                    goto TABLEERROR;
                }
                index.unique_keys.insert(col_id);
            }
            num = table.queues().queue(i).slave().col_size();
            for (int j = 0; j < num; ++j) {
                int col_id = config->columnNameToID(table.queues().queue(i).slave().col(j).name().c_str());
                if (col_id == -1) {
                    TT_WARN_LOG("unique cluster -- cluster col %s error", table.queues().queue(i).slave().col(j).name().c_str());
                    goto TABLEERROR;
                }
                index.queue_keys.insert(col_id);
                config->queue_index_keys.insert(col_id);
            }
            config->queue_keys.push_back(index);
        }
    }
    // timertrigger
    if (table.has_triggers()) {
        const tinytable::triggers& triggers = table.triggers();
        if (triggers.has_timertriggers()) {
            int timertrigger_num = triggers.timertriggers().timertrigger_size();
            for (int i = 0; i < timertrigger_num; ++i) {
                const tinytable::timertrigger& trig = triggers.timertriggers().timertrigger(i);
                TableConfig::TimerTrigger timer;
                timer.name = trig.has_name() ? trig.name() : "";
                std::string type_str = trig.type();
                if (type_str == "realtime") {
                    timer.type = 0;
                } else if (type_str == "lazy") {
                    timer.type = 1;
                } else {
                    TT_WARN_LOG("timer %s type %s error", timer.name.c_str(), type_str.c_str());
                    goto TABLEERROR;
                }
                timer.type = trig.type() == "realtime" ? 0 : 1;
                    std::string timecol_str = trig.timecol();
                    uint64_t sign = get_sign64(timecol_str.c_str(), timecol_str.length());
                    AtomicHashmap<uint64_t, int>::iterator iter = config->column_map->find(sign);
                    if (iter == config->column_map->end()) {
                        TT_WARN_LOG("timecol error: timer %s timecol %s error", timer.name.c_str(), timecol_str.c_str());
                        goto TABLEERROR;
                    }
                    timer.time_col = iter->second;
                timer.interval_ms = trig.condition().has_interval_ms() ? trig.condition().interval_ms() : 0;
                timer.weekday = trig.condition().has_weekday() ? trig.condition().weekday() : -1;
                timer.hour = trig.condition().has_hour() ? trig.condition().hour() : -1;
                timer.min = trig.condition().has_minute() ? trig.condition().minute() : -1;
                timer.sec = trig.condition().has_second() ? trig.condition().second() : -1;
                if (timer.weekday != -1) {
                    timer.interval = 7 * 24 * 3600;
                } else if (timer.hour != -1) {
                    timer.interval = 24 * 3600;
                } else if (timer.min != -1) {
                    timer.interval = 3600;
                } else if (timer.sec != -1) {
                    timer.interval = 60;
                } else {
                    timer.interval = 0;
                }
                // funcs
                int func_size = trig.func().colfunc_size();
                for (int j = 0; j < func_size; ++j) {
                    TableConfig::TriggerFunc func;
                    int col = -1;
                    if (trig.func().colfunc(j).has_name()) {
                        col = config->columnNameToID(trig.func().colfunc(j).name().c_str());
                        if (col < 0) {
                            TT_WARN_LOG("func col %s not exist", trig.func().colfunc(j).name().c_str());
                            goto TABLEERROR;
                        }
                        func.col = col;
                    }
                    func.ops_type = TRIGGER_OPS_SET;
                    if (trig.func().colfunc(j).has_ops_type()) {
                        if (trig.func().colfunc(j).ops_type() == "add") {
                            func.ops_type = TRIGGER_OPS_ADD;
                        } else if (trig.func().colfunc(j).ops_type() == "dec") {
                            func.ops_type = TRIGGER_OPS_DEC;
                        } else if (trig.func().colfunc(j).ops_type() == "del") {
                            func.ops_type = TRIGGER_OPS_DEL;
                        }
                    }
                    if (trig.func().colfunc(j).has_min()) {
                        switch(config->types[col])
                        {
                            case OBJECT_BOOL: func.min = trig.func().colfunc(j).min() == "true" ? true : false; break;
                            case OBJECT_INT8: func.min = (int8_t)atoi(trig.func().colfunc(j).min().c_str()); break;
                            case OBJECT_UINT8: func.min = (uint8_t)atoi(trig.func().colfunc(j).min().c_str()); break;
                            case OBJECT_INT16: func.min = (int16_t)atoi(trig.func().colfunc(j).min().c_str()); break;
                            case OBJECT_UINT16: func.min = (uint16_t)atoi(trig.func().colfunc(j).min().c_str()); break;
                            case OBJECT_INT32: func.min = (int32_t)atoi(trig.func().colfunc(j).min().c_str()); break;
                            case OBJECT_UINT32: func.min = (uint32_t)atoi(trig.func().colfunc(j).min().c_str()); break;
                            case OBJECT_INT64: func.min = (int64_t)atol(trig.func().colfunc(j).min().c_str()); break;
                            case OBJECT_UINT64: func.min = (uint64_t)atol(trig.func().colfunc(j).min().c_str()); break;
                            case OBJECT_FLOAT: func.min = (float)atof(trig.func().colfunc(j).min().c_str()); break;
                            case OBJECT_DOUBLE: func.min = (double)atof(trig.func().colfunc(j).min().c_str()); break;
                            case OBJECT_STRING: func.min = trig.func().colfunc(j).min(); break;
                        }
                    }
                    if (trig.func().colfunc(j).has_max()) {
                        switch(config->types[col])
                        {
                            case OBJECT_BOOL: func.max = trig.func().colfunc(j).max() == "true" ? true : false; break;
                            case OBJECT_INT8: func.max = (int8_t)atoi(trig.func().colfunc(j).max().c_str()); break;
                            case OBJECT_UINT8: func.max = (uint8_t)atoi(trig.func().colfunc(j).max().c_str()); break;
                            case OBJECT_INT16: func.max = (int16_t)atoi(trig.func().colfunc(j).max().c_str()); break;
                            case OBJECT_UINT16: func.max = (uint16_t)atoi(trig.func().colfunc(j).max().c_str()); break;
                            case OBJECT_INT32: func.max = (int32_t)atoi(trig.func().colfunc(j).max().c_str()); break;
                            case OBJECT_UINT32: func.max = (uint32_t)atoi(trig.func().colfunc(j).max().c_str()); break;
                            case OBJECT_INT64: func.max = (int64_t)atol(trig.func().colfunc(j).max().c_str()); break;
                            case OBJECT_UINT64: func.max = (uint64_t)atol(trig.func().colfunc(j).max().c_str()); break;
                            case OBJECT_FLOAT: func.max = (float)atof(trig.func().colfunc(j).max().c_str()); break;
                            case OBJECT_DOUBLE: func.max = (double)atof(trig.func().colfunc(j).max().c_str()); break;
                            case OBJECT_STRING: func.max = trig.func().colfunc(j).max(); break;
                        }
                    }
                    if (trig.func().colfunc(j).has_value()) {
                        switch(config->types[col])
                        {
                            case OBJECT_BOOL: func.val = trig.func().colfunc(j).value() == "true" ? true : false; break;
                            case OBJECT_INT8: func.val = (int8_t)atoi(trig.func().colfunc(j).value().c_str()); break;
                            case OBJECT_UINT8: func.val = (uint8_t)atoi(trig.func().colfunc(j).value().c_str()); break;
                            case OBJECT_INT16: func.val = (int16_t)atoi(trig.func().colfunc(j).value().c_str()); break;
                            case OBJECT_UINT16: func.val = (uint16_t)atoi(trig.func().colfunc(j).value().c_str()); break;
                            case OBJECT_INT32: func.val = (int32_t)atoi(trig.func().colfunc(j).value().c_str()); break;
                            case OBJECT_UINT32: func.val = (uint32_t)atoi(trig.func().colfunc(j).value().c_str()); break;
                            case OBJECT_INT64: func.val = (int64_t)atol(trig.func().colfunc(j).value().c_str()); break;
                            case OBJECT_UINT64: func.val = (uint64_t)atol(trig.func().colfunc(j).value().c_str()); break;
                            case OBJECT_FLOAT: func.val = (float)atof(trig.func().colfunc(j).value().c_str()); break;
                            case OBJECT_DOUBLE: func.val = (double)atof(trig.func().colfunc(j).value().c_str()); break;
                            case OBJECT_STRING: func.val = trig.func().colfunc(j).value(); break;
                        }
                    }
                    timer.funcs.push_back(func);
                }
                config->triggers.push_back(timer);
            }
        }
    }
    return true;
TABLEERROR:
    config->primary_keys.clear();
    config->unique_keys.clear();
    config->sort_keys.clear();
    config->triggers.clear();   
    if (config->index != NULL) {
        delete config->index;
        config->index = NULL;
    }
    if (config->types != NULL) {
        delete config->types;
        config->types = NULL;
    }
    if (config->limits != NULL) {
        delete config->limits;
        config->limits = NULL;
    }
    if (config->column_map != NULL) {
        delete config->column_map;
        config->column_map = NULL;
    }
    if (config->column_name_signs != NULL) {
        delete config->column_name_signs;
        config->column_name_signs = NULL;
    }
    if (config->column_names != NULL) {
        delete config->column_names;
        config->column_names = NULL;
    }
    return false;
}





