package com.example.elasticsearch.controller;

import com.example.elasticsearch.dao.EmployeeRepository;
import com.example.elasticsearch.entity.Employee;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class TestEmployeeController {

    @Autowired
    private EmployeeRepository er;

    /**
     * Employee是查询出来的对象实体
     * EmployeeRepository是根据JPA的规范查询数据的dao
     * 这里是根据对象结构查询的，不建议使用这种方式，因为维护起来后面会很麻烦，对象的字段会变多，不如Map灵活
     */

    //-------------------------------下面是学习例子，仅供参考，对实际项目无用-------------------------------
    //增加
    @RequestMapping("/add")
    public String add() {

        Employee employee = new Employee();
        employee.setId("1");
        employee.setFirstName("xuxu");
        employee.setLastName("zh");
        employee.setAge(26);
        employee.setAbout("i am in peking");
        er.save(employee);

        System.err.println("add a obj");

        return "success";
    }

    //删除
    @RequestMapping("/delete")
    public String delete() {

        Employee employee = new Employee();
        employee.setId("1");
        er.delete(employee);

        return "success";
    }

    //局部更新
    @RequestMapping("/update")
    public String update() {

        Employee employee = er.queryEmployeeById("1");
        employee.setFirstName("哈哈");
        er.save(employee);

        System.err.println("update a obj");

        return "success";
    }

    //查询
    @RequestMapping("/query")
    public Employee query() {

        Employee accountInfo = er.queryEmployeeById("1");
        return accountInfo;
    }

    //一般查询
    /**
     * 使用QueryBuilder
     * termQuery("key", obj) 完全匹配
     * termsQuery("key", obj1, obj2..)   一次匹配多个值
     * matchQuery("key", Obj) 单个匹配, field不支持通配符, 前缀具高级特性
     * multiMatchQuery("text", "field1", "field2"..);  匹配多个字段, field有通配符忒行
     * matchAllQuery();         匹配所有文件
     *
     * --match query搜索的时候，首先会解析查询字符串，进行分词，然后查询;
     * --term query,输入的查询内容是什么，就会按照什么去查询，并不会解析查询内容，对它分词。
     *
     */
    @RequestMapping("/queryByQueryBuilder1")
    public void queryByQueryBuilder1() {
        QueryBuilder query = QueryBuilders.termsQuery("year", "2003");
        Iterable<Employee> emp = er.search(query);
        for(Employee employee : emp){
            System.out.println(employee);
        }
    }

    //组合查询
    /**
     * must(QueryBuilders)   : AND
     * mustNot(QueryBuilders): NOT
     * should:               : OR
     */
    @RequestMapping("/queryByQueryBuilder2")
    public void queryByQueryBuilder2() {
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termsQuery("year", "2003"))
                .must(QueryBuilders.termsQuery("id", "5"));

        Iterable<Employee> emp = er.search(query);
        for(Employee employee : emp){
            System.out.println(employee);
        }
    }

    //只查询id
    /**
     * type ：文章类型doc type
     * ids ：id集合
     * QueryBuilders.idsQuery(String...type).ids(Collection<String> ids)
     */
    @RequestMapping("/queryByQueryBuilder3")
    public void queryByQueryBuilder3() {
        QueryBuilder query = QueryBuilders.idsQuery().addIds("5", "2");

        Iterable<Employee> emp = er.search(query);
        for(Employee employee : emp){
            System.out.println(employee.getId());
        }
    }

    /**
     * 包裹查询, 高于设定分数, 不计算相关性
     * 根据匹配权重，查询数据
     */
    @RequestMapping("/queryByQueryBuilder4")
    public void queryByQueryBuilder4() {
        QueryBuilder query = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("year", "2003")).boost(1.0f);
        Iterable<Employee> emp = er.search(query);
        for(Employee employee : emp){
            System.out.println(employee.getId());
        }
    }

    /**
     * disMax查询
     * 对子查询的结果做union, score沿用子查询score的最大值,
     * 广泛用于muti-field查询
     * tieBreaker 指的是将实际查询分值按照tieBreaker相乘，再返回。
     */
    @RequestMapping("/queryByQueryBuilder5")
    public void queryByQueryBuilder5() {
        QueryBuilder query = QueryBuilders.disMaxQuery()
                .add(QueryBuilders.termQuery("year", "2003"))
                .add(QueryBuilders.termQuery("year", "1962"))
                .boost(1.3f)
                .tieBreaker(0.7f);
        Iterable<Employee> emp = er.search(query);
        for(Employee employee : emp){
            System.out.println(employee.getId());
        }
    }

    /**
     * 模糊查询
     * 不能用通配符
     */
    @RequestMapping("/queryByQueryBuilder6")
    public Iterable<Employee> queryByQueryBuilder6() {
        QueryBuilder query = QueryBuilders.fuzzyQuery("title", "kill");
        Iterable<Employee> emp = er.search(query);
        return emp;
    }

    /**
     * moreLikeThisQuery: 实现基于内容推荐, 支持实现一句话相似文章查询
     * {
     "more_like_this" : {
     "fields" : ["title", "content"],   // 要匹配的字段, 不填默认_all
     "like_text" : "text like this one",   // 匹配的文本
     }
     }

     percent_terms_to_match：匹配项（term）的百分比，默认是0.3

     min_term_freq：一篇文档中一个词语至少出现次数，小于这个值的词将被忽略，默认是2

     max_query_terms：一条查询语句中允许最多查询词语的个数，默认是25

     stop_words：设置停止词，匹配时会忽略停止词

     min_doc_freq：一个词语最少在多少篇文档中出现，小于这个值的词会将被忽略，默认是无限制

     max_doc_freq：一个词语最多在多少篇文档中出现，大于这个值的词会将被忽略，默认是无限制

     min_word_len：最小的词语长度，默认是0

     max_word_len：最多的词语长度，默认无限制

     boost_terms：设置词语权重，默认是1

     boost：设置查询权重，默认是1

     analyzer：设置使用的分词器，默认是使用该字段指定的分词器
     */
    @RequestMapping("/queryByQueryBuilder7")
    public Iterable<Employee> queryByQueryBuilder7() {
        QueryBuilder queryBuilder = QueryBuilders.moreLikeThisQuery(new String[]{"kill"})
                .minTermFreq(1)         //最少出现的次数
                .maxQueryTerms(12);     // 最多允许查询的词语
        Iterable<Employee> emp = er.search(queryBuilder);
        return emp;
    }

    /**
     * match query搜索的时候，首先会解析查询字符串，进行分词，然后查询;
     * term query,输入的查询内容是什么，就会按照什么去查询，并不会解析查询内容，对它分词。
     */
    @RequestMapping("/queryByQueryBuilder8")
    public Iterable<Employee> queryByQueryBuilder8() {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("title", "kill");
        Iterable<Employee> emp = er.search(queryBuilder);
        return emp;
    }

    /**
     * 查询解析查询字符串 --不懂
     */
    @RequestMapping("/queryByQueryBuilder9")
    public Iterable<Employee> queryByQueryBuilder9() {
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("firstName");
        Iterable<Employee> emp = er.search(queryBuilder);
        return emp;
    }

    /**
     * 范围内查询
     * 查询年份在2000~2004之间的数据，包括2000和2004
     */
    @RequestMapping("/queryByQueryBuilder10")
    public Iterable<Employee> queryByQueryBuilder10() {
        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("year")
                .from("2000")
                .to("2004")
                .includeLower(true)     // 包含上界
                .includeUpper(true);    // 包含下届
        Iterable<Employee> emp = er.search(queryBuilder);
        return emp;
    }

    /**
     * 跨度查询 -- 不懂
     */
    @RequestMapping("/queryByQueryBuilder11")
    public Iterable<Employee> queryByQueryBuilder11() {

        QueryBuilder queryBuilder1 = QueryBuilders.spanFirstQuery(QueryBuilders.spanTermQuery("year", "2003"), 30000);     // Max查询范围的结束位置

//        QueryBuilder queryBuilder2 = QueryBuilders.spanNearQuery()
//                .clause(QueryBuilders.spanTermQuery("year", "2003")) // Span Term Queries
//                .clause(QueryBuilders.spanTermQuery("year", "2007"))
//                .clause(QueryBuilders.spanTermQuery("year", "1962"))
//                .slop(3)                                               // Slop factor
//                .inOrder(false)
//                .collectPayloads(false);
//
//        // Span Not
//        QueryBuilder queryBuilder3 = QueryBuilders.spanNotQuery()
//                .include(QueryBuilders.spanTermQuery("name", "葫芦580娃"))
//                .exclude(QueryBuilders.spanTermQuery("home", "山西省太原市2552街道"));
//
//        // Span Or
//        QueryBuilder queryBuilder4 = QueryBuilders.spanOrQuery()
//                .clause(QueryBuilders.spanTermQuery("name", "葫芦580娃"))
//                .clause(QueryBuilders.spanTermQuery("name", "葫芦3812娃"))
//                .clause(QueryBuilders.spanTermQuery("name", "葫芦7139娃"));
//
//        // Span Term
//        QueryBuilder queryBuilder5 = QueryBuilders.spanTermQuery("name", "葫芦580娃");

        Iterable<Employee> emp = er.search(queryBuilder1);
        return emp;
    }

    /**
     * 通配符查询, 支持 *
     * 匹配任何字符序列, 包括空
     * 避免* 开始, 会检索大量内容造成效率缓慢
     * --数字 2 * 3 这样搜索，有可能会抛出异常
     * --区分大小写
     */
    @RequestMapping("/queryByQueryBuilder12")
    public Iterable<Employee> queryByQueryBuilder12() {
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("firstName", "x*U");
        Iterable<Employee> emp = er.search(queryBuilder);
        return emp;
    }

    /**
     * 嵌套查询, 内嵌文档查询  -- 不懂
     */
    @RequestMapping("/queryByQueryBuilder13")
    public Iterable<Employee> queryByQueryBuilder13() {
        QueryBuilder queryBuilder = QueryBuilders.nestedQuery("location",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("location.lat", 0.962590433140581))
                        .must(QueryBuilders.rangeQuery("location.lon").lt(36.0000).gt(0.000)), ScoreMode.Total);
        Iterable<Employee> emp = er.search(queryBuilder);
        return emp;
    }

//    /**
//     * 查询遍历抽取
//     * @param queryBuilder
//     */
//    private void searchFunction(QueryBuilder queryBuilder) {
//        SearchResponse response = client.prepareSearch("twitter")
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setScroll(new TimeValue(60000))
//                .setQuery(queryBuilder)
//                .setSize(100).execute().actionGet();
//
//        while(true) {
//            response = client.prepareSearchScroll(response.getScrollId())
//                    .setScroll(new TimeValue(60000)).execute().actionGet();
//            for (SearchHit hit : response.getHits()) {
//                Iterator<Map.Entry<String, Object>> iterator = hit.getSource().entrySet().iterator();
//                while(iterator.hasNext()) {
//                    Map.Entry<String, Object> next = iterator.next();
//                    System.out.println(next.getKey() + ": " + next.getValue());
//                    if(response.getHits().hits().length == 0) {
//                        break;
//                    }
//                }
//            }
//            break;
//        }
//        testResponse(response);
//    }
//
//    /**
//     * 对response结果的分析
//     * @param response
//     */
//    public void testResponse(SearchResponse response) {
//        // 命中的记录数
//        long totalHits = response.getHits().totalHits();
//
//        for (SearchHit searchHit : response.getHits()) {
//            // 打分
//            float score = searchHit.getScore();
//            // 文章id
//            int id = Integer.parseInt(searchHit.getSource().get("id").toString());
//            // title
//            String title = searchHit.getSource().get("title").toString();
//            // 内容
//            String content = searchHit.getSource().get("content").toString();
//            // 文章更新时间
//            long updatetime = Long.parseLong(searchHit.getSource().get("updatetime").toString());
//        }
//    }
//
//    /**
//     * 对结果设置高亮显示
//     */
//    public void testHighLighted() {
//        /*  5.0 版本后的高亮设置
//         * client.#().#().highlighter(hBuilder).execute().actionGet();
//        HighlightBuilder hBuilder = new HighlightBuilder();
//        hBuilder.preTags("<h2>");
//        hBuilder.postTags("</h2>");
//        hBuilder.field("user");        // 设置高亮显示的字段
//        */
//        // 加入查询中
//        SearchResponse response = client.prepareSearch("blog")
//                .setQuery(QueryBuilders.matchAllQuery())
//                .addHighlightedField("user")        // 添加高亮的字段
//                .setHighlighterPreTags("<h1>")
//                .setHighlighterPostTags("</h1>")
//                .execute().actionGet();
//
//        // 遍历结果, 获取高亮片段
//        SearchHits searchHits = response.getHits();
//        for(SearchHit hit:searchHits){
//            System.out.println("String方式打印文档搜索内容:");
//            System.out.println(hit.getSourceAsString());
//            System.out.println("Map方式打印高亮内容");
//            System.out.println(hit.getHighlightFields());
//
//            System.out.println("遍历高亮集合，打印高亮片段:");
//            Text[] text = hit.getHighlightFields().get("title").getFragments();
//            for (org.elasticsearch.common.text.Text str : text) {
//                System.out.println(str.string());
//            }
//        }
//    }

}
