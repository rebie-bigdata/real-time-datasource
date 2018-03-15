package com.rebiekong.bdt.stream.commons;

import com.alibaba.fastjson.JSON;

import java.beans.Beans;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowData  extends Beans {

    private Long kafkaOffset;
    private List<ColumnData> data = new ArrayList<>();
    private Map<String, ColumnData> header = new HashMap<>();
    private String type;
    private String batchUUID;
    private long createTime;
    private String source;

    public RowData() {
    }

    public Long getKafkaOffset() {
        return kafkaOffset;
    }

    public void setKafkaOffset(Long kafkaOffset) {
        this.kafkaOffset = kafkaOffset;
    }

    public List<ColumnData> getData() {
        return data;
    }

    public void setData(List<ColumnData> data) {
        this.data = data;
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

    public Map<String, ColumnData> getHeader() {
        return header;
    }

    public void setHeader(Map<String, ColumnData> header) {
        this.header = header;
    }
}
