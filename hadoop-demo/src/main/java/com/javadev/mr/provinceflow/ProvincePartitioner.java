package com.javadev.mr.provinceflow;

import java.util.HashMap;

import javax.security.auth.kerberos.KeyTab;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * K2  V2  对应的是map输出kv的类型
 * @author
 *
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean>{

	public static HashMap<String, Integer> proviceDict = new HashMap<String, Integer>();
	static{
		proviceDict.put("136", 0);
		proviceDict.put("137", 1);
		proviceDict.put("138", 2);
		proviceDict.put("139", 3);
	}
	
	
	
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		String prefix = key.toString().substring(0, 3);
		Integer provinceId = proviceDict.get(prefix);
		
		return provinceId==null?4:provinceId;
	}



}
