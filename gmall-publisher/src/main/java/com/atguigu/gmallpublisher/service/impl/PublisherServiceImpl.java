package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.constants.GmallConstants;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getRealTimeTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {

        //1.获取分时数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.定义Map用于存放结果数据
        HashMap<String, Long> result = new HashMap<>();

        //3.遍历list将数据放入result
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        //4.将结果返回
        return result;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        //1.获取分时数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //2.定义Map用于存放结果数据
        HashMap<String, Double> result = new HashMap<>();

        //3.遍历list将数据放入result
        for (Map map : list) {
            result.put((String) map.get("CH"), (Double) map.get("SUM_AMOUNT"));
        }

        //4.将结果返回
        return result;
    }

    @Override
    public HashMap<String, Object> getSaleDetail(String date, int startPage, int size, String keyWord) {

        //1.创建查询语句构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //2.添加过滤条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        //2.1 添加日期过滤条件
        TermQueryBuilder dt = new TermQueryBuilder("dt", date);
        boolQueryBuilder.filter(dt);

        //2.2 添加关键字过滤条件
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyWord);
        matchQueryBuilder.operator(Operator.AND);
        boolQueryBuilder.must(matchQueryBuilder);

        //2.3 将过滤添加封装进searchSourceBuilder
        searchSourceBuilder.query(boolQueryBuilder);

        //3.添加聚合组
        //3.1 添加年龄聚合组
        TermsAggregationBuilder ageAggs = AggregationBuilders
                .terms("countByAge")
                .field("user_age")
                .size(100);
        searchSourceBuilder.aggregation(ageAggs);

        //3.2 添加性别聚合组
        TermsAggregationBuilder genderAggs = AggregationBuilders
                .terms("countByGender")
                .field("user_gender")
                .size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //4.分页
        searchSourceBuilder.from( (startPage - 1) * size );
        searchSourceBuilder.size(size);

        //5.创建Search对象
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(GmallConstants.ES_SALE_DETAIL_INDEX_QUERY)
                .addType("_doc")
                .build();

        SearchResult searchResult = null;

        //6.执行查询
        try {
            searchResult = jestClient.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //7.解析查询结果searchResult
        //7.1 取出总数
        assert searchResult != null;
        Long total = searchResult.getTotal();

        //7.2 取出年龄聚合组数据
        MetricAggregation aggs = searchResult.getAggregations();
        TermsAggregation countByAge = aggs.getTermsAggregation("countByAge");
        HashMap<String, Long> ageMap = new HashMap<>();
        for (TermsAggregation.Entry entry : countByAge.getBuckets()) {
            ageMap.put(entry.getKey() , entry.getCount());
        }

        //7.3 取出性别聚合组数据
        TermsAggregation countByGender = aggs.getTermsAggregation("countByGender");
        HashMap<String, Long> genderMap = new HashMap<>();
        for (TermsAggregation.Entry entry : countByGender.getBuckets()) {
            genderMap.put(entry.getKey() , entry.getCount());
        }

        //7.4 取出明细数据
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        ArrayList<Map> details = new ArrayList<>();
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }

        //8.封装获取的数据,传给Controller
        HashMap<String, Object> result = new HashMap<>();
        result.put("total" , total);
        result.put("age" , ageMap);
        result.put("gender" , genderMap);
        result.put("details" , details);
        return result;
    }
}
