package com.javadev.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HbaseConnection {
	
	/*private static final String HBASE_POS = "192.168.46.163";
	private static final String ZK_POS = "192.168.46.160,192.168.46.161,192.168.46.162";*/
	private static final String HBASE_POS = "192.168.46.166";
	private static final String ZK_POS = "192.168.46.166,192.168.46.167,192.168.46.168";
	private final static String ZK_PORT_VALUE = "2181";
	
	public static Connection getConnection(){
		Connection connection = null;
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.rootdir", "hdfs://" + HBASE_POS + ":9000/hbase");
		configuration.set("hbase.zookeeper.quorum", ZK_POS);
		configuration.set("hbase.zookeeper.property.clientPort", ZK_PORT_VALUE);
		try {
			connection = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return connection;
	}
}
