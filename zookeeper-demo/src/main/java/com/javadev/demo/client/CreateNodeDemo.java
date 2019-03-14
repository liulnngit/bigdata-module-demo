package com.javadev.demo.client;

import org.I0Itec.zkclient.ZkClient;
/**
 * zkClient 创建节点
 * @author ll-t150
 *
 */
public class CreateNodeDemo {
	public static void main(String[] args) {
		ZkClient client = new ZkClient("192.168.46.160:2181", 5000);
		String path = "/zk-client/c1";
		// 递归创建节点
		client.createPersistent(path, true);
	}
}
