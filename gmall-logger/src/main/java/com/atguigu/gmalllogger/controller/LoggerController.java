package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

//@RestController = @Controller + @ResponseBody
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping("t1")
    public String test1(@RequestParam("name") String nm, @RequestParam("age") int age) {
        System.out.println(nm + ":" + age);
        return "success1";
    }

    @RequestMapping("t2")
    public String test2() {
        System.out.println("success2");
        return "success2";
    }

    @RequestMapping("log")
    public String getLog(@RequestParam("logString") String logString) {
//        System.out.println(logString);

        //1.添加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());

        String jsonString = jsonObject.toString();

        //2.写入磁盘
        log.info(jsonString);

        //3.写入Kafka
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP, jsonString);
        } else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, jsonString);
        }

        return "success";
    }

}
