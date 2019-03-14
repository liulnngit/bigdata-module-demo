package com.javadev.demo;

import java.util.List;

import com.alibaba.fastjson.JSON;
import com.javadev.elastic.ElasticSearchUtil;
import com.javadev.hbase.HBaseUtil;

public class HbaseToESDemo {
	
	public static void main(String[] args) throws Exception {
		
		/*List<FingerVO>  fingers = HBaseUtil.getFinger("t_finger");
	       for (FingerVO finger : fingers) {
	    	   String jsonStr = JSON.toJSONString(finger);
	    	   //System.out.println(jsonStr);
	    	   ElasticSearchUtil.saveESByJson("finger", "finger",null, jsonStr);
	    	   System.out.println("保存到ES完成！");
	       }*/
	}
	
	
	
}
