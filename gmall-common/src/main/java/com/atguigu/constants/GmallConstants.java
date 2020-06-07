package com.atguigu.constants;

public class GmallConstants {

    //行为数据,启动日志主题
    public static final String KAFKA_TOPIC_STARTUP = "GMALL_STARTUP";

    //行为数据,事件日志主题
    public static final String KAFKA_TOPIC_EVENT = "GMALL_EVENT";

    //业务数据，订单数据主题
    public static final String KAFKA_TOPIC_ORDER_INFO = "GMALL_ORDER_INFO";

    //业务数据,订单详情数据主题
    public static final String KAFKA_TOPIC_ORDER_DETAIL = "GMALL_ORDER_DETAIL";

    //业务数据,用户信息数据主题
    public static final String KAFKA_TOPIC_USER_INFO = "GMALL_USER_INFO";

    //ES预警日志索引模板前缀
    public static final String ES_ALERT_INDEX = "gmall_coupon_alert";

    //ES灵活分析需求索引模板前缀
    public static final String ES_SALE_DETAIL_INDEX = "gmall2020_sale_detail";

    //ES灵活分析需求查询模板前缀
    public static final String ES_SALE_DETAIL_INDEX_QUERY = "gmall2020_sale_detail-query";

}
