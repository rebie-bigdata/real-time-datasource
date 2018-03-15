package com.rebiekong.bdt.stream.flume.plugins;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.kafka.KafkaSinkConstants;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class CanalSource extends AbstractSource implements Configurable, PollableSource {

    private static Logger logger = LoggerFactory.getLogger(CanalSource.class);

    private CanalConnector connector = null;
    private String canalHost;
    private Integer canalPort;
    private String instanceName;
    private String userName;
    private String password;
    private String patten;
    private Integer batchNum;

    @Override
    public PollableSource.Status process() throws EventDeliveryException {
        // 读取event
        Message message = getMessage();

        long batchId = message.getId();
        if (batchId != -1 && message.getEntries().size() > 0) {
            this.getChannelProcessor().processEventBatch(
                    message.getEntries().stream()
                            .map(new EntryTransformFunction(UUID.randomUUID().toString()))
                            .flatMap(rows -> rows.stream().map(row -> {
                                Map<String, String> header = new HashMap<>();
                                header.put(KafkaSinkConstants.KEY_HEADER, row.getSource());
                                return EventBuilder.withBody(row.toString().getBytes(), header);
                            }))
                            .collect(Collectors.toList())
            );
            connector.ack(batchId);
            return PollableSource.Status.READY;
        }
        return PollableSource.Status.BACKOFF;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 1000;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 5000;
    }

    @Override
    public void configure(Context context) {
        canalHost = context.getString(CanalFlumePluginConstant.CANAL_HOST, CanalFlumePluginConstant.CANAL_HOST_DEFAULT);
        canalPort = context.getInteger(CanalFlumePluginConstant.CANAL_PORT, CanalFlumePluginConstant.CANAL_PORT_DEFAULT);
        batchNum = context.getInteger(CanalFlumePluginConstant.CANAL_BATCH_NUM, CanalFlumePluginConstant.CANAL_BATCH_NUM_DEFAULT);
        instanceName = context.getString(CanalFlumePluginConstant.CANAL_INSTANCE_NAME, CanalFlumePluginConstant.CANAL_INSTANCE_NAME_DEFAULT);
        userName = context.getString(CanalFlumePluginConstant.CANAL_USER, CanalFlumePluginConstant.CANAL_USER_DEFAULT);
        password = context.getString(CanalFlumePluginConstant.CANAL_PASSWORD, CanalFlumePluginConstant.CANAL_PASSWORD_DEFAULT);
        patten = context.getString(CanalFlumePluginConstant.CANAL_PATTEN, CanalFlumePluginConstant.CANAL_PATTEN_DEFAULT);
    }

    private Message getMessage() {
        Message message;
        try {
            message = connector.getWithoutAck(batchNum);
        } catch (CanalClientException e) {
            logger.error("read canal data error", e);
            message = new Message(-1);
            initConnector();
        }
        return message;
    }

    private void initConnector() {
        if (connector != null) {
            connector.disconnect();
            connector = null;
        }
        connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(canalHost, canalPort),
                instanceName,
                userName,
                password
        );
        connector.connect();
        connector.subscribe(patten);
        connector.rollback();
    }

    @Override
    public synchronized void start() {
        super.start();
        initConnector();
    }

    @Override
    public synchronized void stop() {
        connector.disconnect();
        super.stop();
    }
}
