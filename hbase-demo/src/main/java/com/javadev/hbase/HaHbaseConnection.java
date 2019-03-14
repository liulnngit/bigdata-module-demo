package com.javadev.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HaHbaseConnection {
	public static final Log log = LogFactory.getLog(HaHbaseConnection.class);
			 
	public static Connection getConnection(){
		 Connection connection = null;
		 try {
	            Configuration configuration = HBaseConfiguration.create();
	            configuration.set("hbase.zookeeper.quorum", "192.168.46.160,192.168.46.161,192.168.46.162");
	            
	            String confPath = System.getProperty("conf");
	            if(confPath!=null&&!confPath.equals("")){
	                Path filePath = new Path(confPath);
	                configuration.addResource(filePath+"/core-site.xml");
	                configuration.addResource(filePath+"/hdfs-site.xml");
	            }else{
	                configuration.addResource("core-site.xml");
	                configuration.addResource("hdfs-site.xml");
	            }
	            
	            connection = ConnectionFactory.createConnection(configuration);
	            log.info("the connection : " + connection);
	        } catch (Exception e) {
	            log.error("getConnection exception : "  + e);
	        } 
	        return connection;
	}
}
