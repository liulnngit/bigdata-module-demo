package com.javadev.rpc.controller;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import com.javadev.rpc.LoginServiceInterface;


public class UserLoginController {
	
	public static void main(String[] args) throws Exception {
		
		//先拿到调用端这一边的socke程序的代理对象
		LoginServiceInterface loginService = RPC.getProxy(LoginServiceInterface.class, 1L, new InetSocketAddress("hdp-node01", 1314), new Configuration());
		
		String result = loginService.login("angelababy", "真的好想你");
		
		System.out.println(result);
		
		
		
	}

}
