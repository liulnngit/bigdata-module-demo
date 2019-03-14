package com.javadev.hdfs;

import org.apache.hadoop.conf.Configuration;

/**
 * @author ll-t150
 * 通过加载xml文件读取信息
 */
public class ConfigurationReasearch {

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		//加载配置文件
		conf.addResource("test.xml");
		System.out.println(conf.get("xxx.uu"));

		// Iterator<Entry<String, String>> it = conf.iterator();
		//
		// while(it.hasNext()){
		//
		// System.out.println(it.next());
		//
		// }

	}
}
