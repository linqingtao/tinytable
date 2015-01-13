#include "serial_table.h"
#include "tinytable_log_utils.h"


SingleTable* Serial::_serial = NULL;

bool Serial::global_init() {
    char types[3] = {OBJECT_STRING, OBJECT_STRING, OBJECT_INT64};
    std::string names[3] = {"table_name", "col_name", "increase_id"};
    TableConfig config(SERIAL_TABLE_NAME, 3, types, NULL, names, SERIAL_TABLE_ID);
    config.table_type = OPS_DB;
    config.load_type = LOAD_ALL;
    config.addPrimary(COL2(COL_SERIAL_TABLE_NAME, COL_SERIAL_COL_NAME));
    _serial = new(std::nothrow) SingleTable();
    if (_serial == NULL) {
        TT_WARN_LOG("new serial error");
        return false;
    }
    if (!_serial->init(&config, false)) {
        TT_WARN_LOG("init serial table error");
        delete _serial;
        _serial = NULL;
        return false;
    }
    return true;
}

bool Serial::global_destroy() {
    if (_serial != NULL) {
        delete _serial;
        _serial = NULL;
    }
    return true;
}

