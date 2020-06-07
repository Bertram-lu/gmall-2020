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

public class CanalClient {

    public static void main(String[] args) {
        //获取Canal连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                ""
        );

        //抓取数据
        while (true) {
            //1.使用连接器连接Canal
            canalConnector.connect();
            //2.指定订阅的表信息
            canalConnector.subscribe("gmall.*");
            //3.抓取数据
            Message message = canalConnector.get(100);
            //4.判断当前message是否有数据
            if (message.getEntries().size() <= 0 ) {
                //无数据
                System.out.println("当前无可抓取数据，请稍等......");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //有数据，则解析message
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //判断一下，取当前操作为数据操作的内容
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        try {
                            //获取序列化的数据集合
                            ByteString storeValue = entry.getStoreValue();
                            //获取表名,用于分类数据
                            String tableName = entry.getHeader().getTableName();
                            //反序列化storeValue
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            //获取事件类型,用于过滤
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            //解析rowChange并发送到Kafka
                            handler(tableName,eventType,rowChange);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    /**
     * 解析rowChange并发送到Kafka
     *
     * @param tableName 表名,用于区分主题
     * @param eventType 事件类型,用于筛选数据
     * @param rowChange 实际的数据
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, CanalEntry.RowChange rowChange) {

        //取订单表新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            //订单数据
            sendToKafka(rowChange , GmallConstants.KAFKA_TOPIC_ORDER_INFO);
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            //订单详情数据
            sendToKafka(rowChange , GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) ||
                CanalEntry.EventType.UPDATE.equals(eventType))) {
            //用户信息数据
            sendToKafka(rowChange , GmallConstants.KAFKA_TOPIC_USER_INFO);
        }
    }

    private static void sendToKafka(CanalEntry.RowChange rowChange, String topic) {
        //解析rowChange
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            //创建JSON对象用于存放一行数据
            JSONObject jsonObject = new JSONObject();
            //遍历一行数据中的列
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(),column.getValue());
            }
            //打印数据,发送至Kafka主题
            System.out.println(jsonObject.toString());
            MyKafkaSender.send(topic,jsonObject.toString());
        }
    }


}
