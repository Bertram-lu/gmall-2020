package com.atguigu.gmallpublisher.service;

import java.util.HashMap;
import java.util.Map;

public interface PublisherService {

    public Integer getRealTimeTotal(String date);

    public Map getDauTotalHourMap(String date);

    public Double getOrderAmountTotal(String date);

    public Map getOrderAmountHour(String date);

    public HashMap<String, Object> getSaleDetail(String date , int startPage , int size , String keyWord);

}
