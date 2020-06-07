package com.atguigu.gmallpublisher.controller;

import bean.Option;
import bean.Stat;
import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date) {

        //1.创建集合用于存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //2.创建新增日活的Map对象
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", publisherService.getRealTimeTotal(date));

        //3.创建新增设备的Map对象
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);

        //4.创建新增交易额的Map对象
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", publisherService.getOrderAmountTotal(date));

        //4.将三个Map放入集合
        result.add(dauMap);
        result.add(newMidMap);
        result.add(gmvMap);

        return JSON.toJSONString(result);
    }

    //获取分时统计数据
    @RequestMapping("realtime-hours")
    public String getRealTimeHours(@RequestParam("id") String id, @RequestParam("date") String date) {

        //1.创建集合用于存放结果数据
        HashMap<String, Map> result = new HashMap<>();

        //2.获取昨天日期
        String yesterdayStr = LocalDate.parse(date).minusDays(1).toString();

        //3.声明昨天以及今天分时结果的Map
        Map todayMap = null;
        Map yesterdayMap = null;

        //4.判断是否当前获取的为日活分时数据
        if ("dau".equals(id)) {

            //获取当天的数据
            todayMap = publisherService.getDauTotalHourMap(date);

            //获取昨天的数据
            yesterdayMap = publisherService.getDauTotalHourMap(yesterdayStr);
        } else if ("order_amount".equals(id)) {

            //获取当天的数据
            todayMap = publisherService.getOrderAmountHour(date);

            //获取昨天的数据
            yesterdayMap = publisherService.getOrderAmountHour(yesterdayStr);
        }
        //5.添加昨天和今天的分时数据至result
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        //6.返回结果
        return JSON.toJSONString(result);
    }

    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,
                                @RequestParam("startpage") int startPage,
                                @RequestParam("size") int size,
                                @RequestParam("keyword") String keyWord) {
        //1.创建Map用于存放加工后的数据
        HashMap<String, Object> resultMap = new HashMap<>();

        //2.获取数据
        HashMap<String, Object> saleDetail = publisherService.getSaleDetail(date,
                startPage, size, keyWord);
        Long total = (Long) saleDetail.get("total");

        if (total > 0L) {
            Map ageMap = (Map) saleDetail.get("age");
            Map genderMap = (Map) saleDetail.get("gender");
            List details = (List) saleDetail.get("details");

            //3.解析年龄[15->20,25->29...] => ["20以下"->60.5...]
            //3.1 定义变量,用于记录各个年龄段的人数
            Long lower20 = 0L;
            Long start20to30 = 0L;
            //3.2 遍历年龄Map,将年龄分组
            for (Object o : ageMap.keySet()) {
                Integer age = Integer.parseInt((String) o);
                Long count = (Long) ageMap.get(o);
                if (age < 20) {
                    lower20 += count;
                } else if (age < 30) {
                    start20to30 += count;
                }
            }

            //3.3 计算年龄段占比
            double lower20Ratio = Math.round(lower20 * 1000 / total) / 10D;
            double start20to30Ratio = Math.round(start20to30 * 1000 / total) / 10D;
            double upper30Ratio = 100D - lower20Ratio - start20to30Ratio;

            //3.4 创建年龄的Option对象,并放入集合
            Option lower20opt = new Option("20岁以下", lower20Ratio);
            Option start20to30opt = new Option("20岁到30岁", start20to30Ratio);
            Option upper30opt = new Option("30岁及30岁以上", upper30Ratio);
            ArrayList<Option> ageList = new ArrayList<>();
            ageList.add(lower20opt);
            ageList.add(start20to30opt);
            ageList.add(upper30opt);

            //3.5 创建年龄占比的Stat对象
            Stat ageStat = new Stat("用户年龄占比", ageList);

            //4.解析性别[F->50,M->50] => ["男"->50.0,"女"->50.0]
            //4.1 获取female数据条数
            Long femaleCount = (Long) genderMap.get("F");

            //4.2 计算female占比
            double femaleRadio = Math.round(femaleCount * 1000 / total) / 10D;

            //4.3 计算male占比
            double maleRadio = 100D - femaleRadio;

            //4.4 创建男女的Option对象
            Option maleOpt = new Option("男", maleRadio);
            Option femaleOpt = new Option("女", femaleRadio);

            //4.5 创建集合用于存放Option对象
            ArrayList<Option> genderList = new ArrayList<>();
            genderList.add(maleOpt);
            genderList.add(femaleOpt);

            //4.6 创建性别占比的Stat对象
            Stat genderStat = new Stat("用户性别占比", genderList);

            //5.将加工后的数据添加至resultMap
            ArrayList<Stat> stats = new ArrayList<>();
            stats.add(ageStat);
            stats.add(genderStat);
            resultMap.put("total" , total);
            resultMap.put("stat" , stats);
            resultMap.put("detail" , details);
        }

        //6.返回结果
        return JSON.toJSONString(resultMap);
    }
}
