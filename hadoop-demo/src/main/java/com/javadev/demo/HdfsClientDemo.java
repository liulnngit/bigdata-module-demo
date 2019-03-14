package com.javadev.demo;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
/**
 * 
 * 客户端去操作hdfs时，是有一个用户身份的
 * 默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份：-DHADOOP_USER_NAME=hadoop
 * 
 * 也可以在构造客户端fs对象时，通过参数传递进去
 * @author
 *
 */
public class HdfsClientDemo {
	FileSystem fs = null;
	@Before
	public void init() throws Exception{
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		//拿到一个文件系统操作的客户端实例对象
		/*fs = FileSystem.get(conf);*/
		//可以直接传入 uri和用户身份
		fs = FileSystem.get(new URI("hdfs://master:9000"),conf,"hadoop");
	}

	@Test
	public void testUpload() throws Exception {
		
		//Thread.sleep(500000);
		fs.copyFromLocalFile(new Path("d:/access.log"), new Path("/access.log.copy2"));
		fs.close();
	}
	
	
	@Test
	public void testDownload() throws Exception {
		
		fs.copyToLocalFile(new Path("/access.log.copy"), new Path("d:/"));
		fs.close();
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//拿到一个文件系统操作的客户端实例对象
		FileSystem fs = FileSystem.get(conf);
		
		fs.copyFromLocalFile(new Path("d:/Recovery.txt"), new Path("/Recovery.txt.copy"));
		fs.close();
	}
	

}
