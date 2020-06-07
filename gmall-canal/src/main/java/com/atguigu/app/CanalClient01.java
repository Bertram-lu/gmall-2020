package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;

public class CanalClient01 {

    public static void main(String[] args) {

       /* CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                ""
                );

        while (true) {

            canalConnector.connect();

            canalConnector.subscribe("gmall.*");

            Message message = canalConnector.get(100);

            if (message.getEntries().size() <= 0 ) {
                System.out.println("当前无可抓取数据 ， 请稍等 ...... ");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                for (CanalEntry.Entry entry : message.getEntries()) {

                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {

                        try {
                            ByteString storeValue = entry.getStoreValue();

                            String tableName = entry.getHeader().getTableName();

                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                            CanalEntry.EventType eventType = rowChange.getEventType();

                            handler(tableName,eventType,rowChange);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }*/

    }

    /*private static void handler(String tableName, CanalEntry.EventType eventType, CanalEntry.RowChange rowChange) {

        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {

                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName() , column.getValue());
                }

                System.out.println(jsonObject.toString());
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER_INFO,jsonObject.toString());
            }
        }
    }*/
}
