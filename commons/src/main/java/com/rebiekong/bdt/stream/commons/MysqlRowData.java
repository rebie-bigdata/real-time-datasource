package com.rebiekong.bdt.stream.commons;

import java.sql.Types;

public class MysqlRowData extends RowData {

    public final static String OFFSET_HEADER_KEY = "binlogOffset";
    public final static String BINLOG_FILE_HEADER_KEY = "binlogFile";

    public long getBinlogOffset() {
        ColumnData value = getHeader().get(OFFSET_HEADER_KEY);
        if (value == null) {
            return 0;
        }
        return Long.valueOf(value.getValue());
    }

    public void setBinlogOffset(long binlogOffset) {
        ColumnData columnData = new ColumnData();
        columnData.setType(Types.BIGINT);
        columnData.setValue(String.valueOf(binlogOffset));
        columnData.setName(OFFSET_HEADER_KEY);
        getHeader().put(OFFSET_HEADER_KEY, columnData);
    }

    public String getBinlogFile() {
        ColumnData value = getHeader().get(OFFSET_HEADER_KEY);
        if (value == null) {
            return null;
        }
        return value.getValue();
    }

    public void setBinlogFile(String binlogFile) {
        ColumnData columnData = new ColumnData();
        columnData.setType(Types.VARCHAR);
        columnData.setValue(binlogFile);
        columnData.setName(BINLOG_FILE_HEADER_KEY);
        getHeader().put(BINLOG_FILE_HEADER_KEY, columnData);

    }
}
