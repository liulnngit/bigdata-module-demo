package com.javadev.demo.zk;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * 解释怎么去连接zk并监听事件 
 * @author ll-t150
 */
public class ZKDemo implements Watcher{
	private static final CountDownLatch cdl = new CountDownLatch(1);
	
	public static void main(String[] args) throws IOException {
		ZooKeeper zk = new ZooKeeper("192.168.46.160:2181", 5000, new ZKDemo());
		System.out.println(zk.getState());
		try {
			//记住：countdownlatch 使用的是await方法，该方法是不会阻塞当前线程的。
			cdl.await();
		} catch (InterruptedException e) {
			// new zookeeper过程由于ZKDemo实现了Watch监听，会实现process方法，所以会coundown，而不会一直等待
			System.out.println("ZK Session established.");
		}
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println("Receive watched event:"+event);
		if(KeeperState.SyncConnected == event.getState()){
			cdl.countDown();
		}
	}
	
	
	
}
