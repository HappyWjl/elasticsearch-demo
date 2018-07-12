package com.example.elasticsearch.controller;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/es")
public class ElasticSearchController {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    /**
     * 这里返回结果是Map结构，建议使用这种方式查询，比较灵活,可能写起来相对麻烦一些，但对项目维护会有很大帮助
     */

    //测试
    @RequestMapping("/test")
    public String test() {
        SearchRequest searchRequest = new SearchRequest("movies");

        QueryBuilder query = QueryBuilders.termsQuery("year", "2003");
        searchRequest.source(new SearchSourceBuilder().query(query));

        ActionFuture<SearchResponse> search = elasticsearchTemplate.getClient().search(searchRequest);

        SearchHits hits = search.actionGet().getHits();

        StringBuffer result = new StringBuffer();
        for (SearchHit hit : hits) {
            Map recordMap = hit.getSourceAsMap();
            System.out.println("-------" + JSON.toJSONString(recordMap));
            result.append(JSON.toJSONString(recordMap));
        }
        return result.toString();
    }

}
