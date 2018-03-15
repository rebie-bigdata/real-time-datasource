package com.rebiekong.bdt.stream.commons;

import com.alibaba.fastjson.JSON;

public class ColumnData {
    private String name;
    private String value;
    private int type;
    private boolean isPrimaryKey = false;

    public static ColumnData create(String name, Boolean isPrimaryKey, Integer sqlType, String value) {
        ColumnData c = new ColumnData();
        c.setName(name);
        c.setPrimaryKey(isPrimaryKey);
        c.setType(sqlType);
        c.setValue(value);
        return c;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }

    public String toString() {
        return JSON.toJSONString(this);
    }

}
