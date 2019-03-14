package com.javadev.elastic;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.DateTimeZone;

import com.alibaba.fastjson.JSONObject;

public class ElasticSearchUtil {
	
	private static String ip = "192.168.37.111";
    private static String port = "9300";
    private static String cluster_name = "zpt";
    private static TransportClient client;

    static{
        Settings settings = Settings.builder().put("cluster.name", cluster_name) // ES 集群名字
                .put("client.transport.sniff", true)// 启动嗅探功能,通过一个指定任意节点 (不必是主节点)查找出集群下所有 ES 节点
                .build();
        try {
            client = new PreBuiltTransportClient(settings)
            		.addTransportAddress(new TransportAddress(InetAddress.getByName(ip), Integer.valueOf(port)));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
    
    /* @Title: saveESByJson 
     * @Description: 保存Json数据到ES中
     * @author ll-t150
     * @date 2017年12月12日 上午10:06:50
     */ 
     public static long saveESByJson(String index, String type, String id, String json) {
         long version ;
         IndexResponse response = null;
         if (StringUtils.isBlank(id)) {
             response = client.prepareIndex(index, type).setSource(json,XContentType.JSON).get();
         } else {
             response = client.prepareIndex(index, type,id).setSource(json,XContentType.JSON).get();
         }
         version = response.getVersion();
         return version;
     }
     
     /**
     * @Title: deleteES 
     * @Description: 根据docid 删除ES中相应的记录 
     * @author ll-t150
     * @date 2017年12月12日 下午5:36:00
      */
     public static int deleteESById(String index, String type, String id) {
         DeleteResponse deleteResponse = client.prepareDelete().setIndex(index).setType(type).setId(id).get();
         return deleteResponse.status().getStatus();
     }
     
     /**
     * @Title: updateESByJson 
     * @Description: TODO(根据id来更新ES中的数据) 
     * @author ll-t150
     * @date 2017年12月12日 上午11:12:51
      */
     public static boolean updateESByJson(String index, String type, String id, String json) {
         boolean flag = false ;
         try {
              client.prepareUpdate(index,type,id).setDoc(json,XContentType.JSON).get();
              flag = true ;
         } catch (Exception e) {
             e.printStackTrace();
         }
         return flag;
     }
     
     
     /**
     * @Description: 根据Doc ID 查询ES数据
     * @author ll-t150
     * @date 2017年12月14日 上午11:03:18
      */
     public static Map<String, Object> selectESById(String index, String type, String id) {
         GetResponse response = client.prepareGet()
                 .setIndex(index).setType(type)
                 .setId(id).get();
         return response.getSource();
     }
     
     public static SearchHit[] selectByESField(String index, String type, String fieldName, String fieldValue) {
         SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
         searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);
         searchRequestBuilder.setPostFilter(QueryBuilders.termQuery(fieldName, fieldValue));
         SearchResponse searchResponse = searchRequestBuilder.get();
         SearchHits hits = searchResponse.getHits();
         SearchHit[] searchHits = hits.getHits();
         return searchHits;
     }
     
     /**
     * @Title: selectByESField 
     * @Description: TODO(根据单个字段查询ES数据) 
     * @author ll-t150
     * @date 2017年12月12日 上午11:57:56
      */
     public static <T> List<T> selectByESField(String index, String type, String fieldName, String fieldValue,
             Class<T> tclass) {
         SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
         searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);
         searchRequestBuilder.setPostFilter(QueryBuilders.termQuery(fieldName, fieldValue));
         SearchResponse searchResponse = searchRequestBuilder.get();
         SearchHits hits = searchResponse.getHits();
         SearchHit[] searchHits = hits.getHits();
         List<T> searchList = processSearchHits(searchHits, tclass);
         return searchList;
     }
     
     
     private static <T> List<T> processSearchHits(SearchHit[] searchHits, Class<T> tclass) {
         List<T> searchList = new ArrayList<>();
         try {
             if (null != searchHits && searchHits.length != 0) {
                 for (SearchHit s : searchHits) {
                     Map<String, Object> map = s.getSourceAsMap();
                     map.put("id", s.getId());
                     T obj = JSONObject.parseObject(JSONObject.toJSONString(map),tclass);
                     searchList.add(obj);
                 }
             }
         } catch (Exception e) {
             e.printStackTrace();
         }
         return searchList;
     }
    
	public static String selectInvoiceTrendReport(String index, String type, String startDate, String endDate,
			String formatType, String merchantId, DateHistogramInterval interval) {
		SearchResponse response = new SearchResponse();
		SearchRequestBuilder responsebuilder = client.prepareSearch(index).setTypes(type).setSize(0);
		// 时间范围的设定
		RangeQueryBuilder rangequerybuilder = QueryBuilders.rangeQuery("open_date").format(formatType)
				.timeZone("+08:00").from(startDate).to(endDate);
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		if (StringUtils.isNotBlank(merchantId)) {
			query.must(QueryBuilders.matchPhraseQuery("merchant_id", merchantId));
		}
		query.must(rangequerybuilder);
		AggregationBuilder aggregation = AggregationBuilders.dateHistogram("date_histogram").field("open_date")
				.dateHistogramInterval(interval).timeZone(DateTimeZone.forID("+08:00"))
				/* .timeZone("+08:00") */
				.format(formatType)
				.minDocCount(0)/* .extendedBounds(startDate, endDate) */
				.extendedBounds(new ExtendedBounds(startDate, endDate))
				/* .order(Histogram.Order.KEY_ASC) */
				.order(BucketOrder.key(true)).subAggregation(AggregationBuilders.count("count").field("invoice_id"));
		response = responsebuilder.setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(query)
				.addAggregation(aggregation).setExplain(true).execute().actionGet();

		return response.toString();
	}
    
	public static void insertOrUpdate(String index, String type,String id,String jsonString){
		 IndexRequest indexRequest = new IndexRequest(index, type, id).source(jsonString,XContentType.JSON);
         UpdateRequest uRequest2 = new UpdateRequest(index, type, id).doc(jsonString,XContentType.JSON).upsert(indexRequest);
         try {
			UpdateResponse res = client.update(uRequest2).get();
			System.out.println(res.status().getStatus());
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
     
    
}
