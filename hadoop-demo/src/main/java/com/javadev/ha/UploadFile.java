package com.javadev.ha;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


/**
 * 如果访问的是一个ha机制的集群
 * 则一定要把core-site.xml和hdfs-site.xml配置文件放在客户端程序的classpath下(src/main/resources)
 * 以让客户端能够理解hdfs://ns1/中  “ns1”是一个ha机制中的namenode对——nameservice
 * 以及知道ns1下具体的namenode通信地址
 * @author
 *	需要配置hosts 进行主机名和ip进行映射
 *		windows的路径 C:\Windows\System32\drivers\etc
 *		Linux的配置路径：/etc/hosts
 *
 */
public class UploadFile {
	
	public static void main(String[] args) throws Exception  {
		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://btc/");
		FileSystem fs = FileSystem.get(new URI("hdfs://btc/"),conf,"hadoop");
		//fs.copyFromLocalFile(new Path("d:/access.log"), new Path("hdfs://btc/"));
		//long startTime = System.currentTimeMillis();
		InputStream in = null;
		try {
			in = fs.open(new Path("hdfs://btc/access.log"));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally{
			IOUtils.closeStream(in);
		}
		System.out.println();
		System.out.println("====over "+(System.currentTimeMillis()-startTime));
		fs.close();
		    
	}
	
	

}
