package com.javadev.rpc;


/**
 * hdfs客户端跟namenode之间进行远程过程调用使用的协议——接口
 * @author
 *
 */
public interface ClientNameNodeProtocol {
	
	public static final long versionID = 100L;
	
	public String getMetaData(String path);

}
