package com.rebiekong.bdt.stream.commons;

import com.alibaba.fastjson.JSON;

import java.util.List;

public class RowData {

    private List<ColumnData> data;
    private String type;
    private String batchUUID;
    private long createTime;
    private long binlogOffset;
    private String binlogFile;
    private String source;

    public RowData(List<ColumnData> data) {
        this.data = data;
    }

    public List<ColumnData> getData() {
        return data;
    }

    public String getBatchUUID() {
        return batchUUID;
    }

    public void setBatchUUID(String batchUUID) {
        this.batchUUID = batchUUID;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getBinlogOffset() {
        return binlogOffset;
    }

    public void setBinlogOffset(long binlogOffset) {
        this.binlogOffset = binlogOffset;
    }

    public String getBinlogFile() {
        return binlogFile;
    }

    public void setBinlogFile(String binlogFile) {
        this.binlogFile = binlogFile;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String toString() {
        return JSON.toJSONString(this);
    }
}
