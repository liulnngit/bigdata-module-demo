package com.javadev.elastic;

import java.util.Map;

import org.elasticsearch.search.SearchHit;

/**
 * @author ll-t150
 * 验证结果：通过RowKey保存数据到ElasticSearch，Rowkey相同，新数据不会保存成功，但是会更新原数据
 * 
 */
public class Client {
	
	public static void main(String[] args) throws Exception {
		//System.out.println(ElasticSearchUtil.selectESById("t_order", "t_order", "C1099182124_9b5c21cf833344e8ad7354323ddabdf3"));
		/*long start = System.currentTimeMillis();
		ElasticSearchUtil.selectScore("finger", "finger");
		System.out.println("cost time:"+(System.currentTimeMillis()-start));*/
		
		String json ="{\"name\":\"mike\",\"age\":14}";
		json ="{\"name\":\"mike1\",\"age\":1999}";
		
		ElasticSearchUtil.insertOrUpdate("t_test", "t_test", "1235", json);
		/*long version = ElasticSearchUtil.saveESByJson("t_test", "t_test","1234", json);
		System.out.println(version);*/
		
		/*long start = System.currentTimeMillis();
		Map<String, Object> res = ElasticSearchUtil.selectESById("t_test", "t_test","1234");
		long start2 = System.currentTimeMillis();
		System.out.println(res+","+(start2-start));
		ElasticSearchUtil.selectESById("t_test", "t_test","123");
		System.out.println((System.currentTimeMillis()-start2));
		
		int status = ElasticSearchUtil.deleteESById("t_test", "t_test","124");
		System.out.println(status);
		json ="{\"name\":\"jesse\",\"age\":19}";
		ElasticSearchUtil.updateESByJson("t_test", "t_test","1234", json);
		
		SearchHit[] hits = ElasticSearchUtil.selectByESField("t_test", "t_test", "name", "jesse");
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}*/
		
		
	}
	
	
}
