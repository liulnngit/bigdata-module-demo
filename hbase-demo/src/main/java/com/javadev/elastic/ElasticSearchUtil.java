package com.javadev.elastic;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

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
     * @Description: TODO(保存Json数据到ES中) 
     * @author ll-t150
     * @date 2017年12月12日 上午10:06:50
     */ 
     public static boolean saveESByJson(String index, String type, String id, String json) {
         boolean result = false;
         IndexResponse response = null;
         /*Map<String, Object> jsonMap = new HashMap<String, Object>();
         jsonMap.put("name","jim");
         jsonMap.put("age",20);*/
         if (StringUtils.isBlank(id)) {
             response = client.prepareIndex(index, type).setSource(json,XContentType.JSON).get();
         } else {
             response = client.prepareIndex(index, type).setId(id).setSource(json,XContentType.JSON).get();
         }
         String ss = response.getId();
         System.out.println(ss);
         return result;
     }
     
     /**
     * @Title: deleteES 
     * @Description: TODO(根据docid 删除ES中相应的记录) 
     * @author ll-t150
     * @date 2017年12月12日 下午5:36:00
      */
     public static boolean deleteESById(String index, String type, String id) {
         DeleteResponse deleteResponse = client.prepareDelete().setIndex(index).setType(type).setId(id).get();
         return true;
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
              client.prepareUpdate()
                    .setIndex(index)
                    .setType(type)
                    .setId(id)
                    .setDoc(json)
                    .get();
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
     
     /**
     * @Title: selectByESField 
     * @Description: TODO(根据单个字段查询ES数据) 
     * @author ll-t150
     * @date 2017年12月12日 上午11:57:56
      */
    /* public static <T> List<T> selectByESField(String index, String type, String fieldName, String fieldValue,
             Class<T> tclass) {
         SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
         searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);
         searchRequestBuilder.setPostFilter(QueryBuilders.termQuery(fieldName, fieldValue));
         SearchResponse searchResponse = searchRequestBuilder.get();
         SearchHits hits = searchResponse.getHits();
         SearchHit[] searchHits = hits.hits();
         List<T> searchList = processSearchHits(searchHits, tclass);
         return searchList;
     }*/
     
     
     /*private static <T> List<T> processSearchHits(SearchHit[] searchHits, Class<T> tclass) {
         List<T> searchList = new ArrayList<>();
         try {
             if (null != searchHits && searchHits.length != 0) {
                 for (SearchHit s : searchHits) {
                     Map<String, Object> map = s.getSource();
                     map.put("id", s.getId());
                     T obj = JSONObject.parseObject(JSONObject.toJSONString(map),tclass);
                     searchList.add(obj);
                 }
             }
         } catch (Exception e) {
             e.printStackTrace();
         }
         return searchList;
     }*/
    
     
     
     
     
    
}
