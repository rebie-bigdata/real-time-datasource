package com.rebiekong.bdt.stream.flume.plugins;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rebiekong.bdt.stream.commons.ColumnData;
import com.rebiekong.bdt.stream.commons.MysqlRowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EntryTransformFunction implements Function<Entry, ArrayList<MysqlRowData>> {

    Logger logger = LoggerFactory.getLogger(EntryTransformFunction.class);

    private String uuid;

    public EntryTransformFunction(String uuid) {
        this.uuid = uuid;
    }

    @Override
    public ArrayList<MysqlRowData> apply(Entry entry) {

        ArrayList<MysqlRowData> result = new ArrayList<>();
        try {
            result = realAction(entry);
        } catch (InvalidProtocolBufferException e) {
            logger.error("read data from canal error", e);
        }
        return result;
    }

    private ArrayList<MysqlRowData> realAction(Entry entry) throws InvalidProtocolBufferException {

        ArrayList<MysqlRowData> result = new ArrayList<>();

        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());

        CanalEntry.EventType eventType = rowChange.getEventType();
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            List<Column> columnsList = rowData.getAfterColumnsList();
            if (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE) {
                columnsList = rowData.getAfterColumnsList();
            } else if (eventType == CanalEntry.EventType.DELETE) {
                columnsList = rowData.getBeforeColumnsList();
            }
            List<ColumnData> tmpData = columnsList.stream().map(column -> {
                ColumnData columnData = new ColumnData();
                columnData.setName(column.getName());
                columnData.setValue(column.getValue());
                columnData.setType(column.getSqlType());
                columnData.setPrimaryKey(column.getIsKey());
                return columnData;
            }).collect(Collectors.toList());

            MysqlRowData row = new MysqlRowData();
            row.setData(tmpData);
            row.setType(eventType.toString());
            row.setBatchUUID(uuid);
            row.setSource(entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName());
            row.setCreateTime(entry.getHeader().getExecuteTime());
            row.setBinlogOffset(entry.getHeader().getLogfileOffset());
            row.setBinlogFile(entry.getHeader().getLogfileName());
            result.add(row);
        }
        return result;
    }

}
