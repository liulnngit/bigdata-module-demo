package com.javadev.demo.zk;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ZKSessionDemo implements Watcher {
	private static final CountDownLatch cdl = new CountDownLatch(1);

	public static void main(String[] args) throws IOException, InterruptedException {
		ZooKeeper zk = new ZooKeeper("192.168.46.160:2181", 5000, new ZKSessionDemo());
		cdl.await();
		long sessionId = zk.getSessionId();
		byte[] passwd = zk.getSessionPasswd();

		zk = new ZooKeeper("192.168.46.160:2181", 5000, new ZKSessionDemo(), 1l, "test".getBytes());
		zk = new ZooKeeper("192.168.46.160:2181", 5000, new ZKSessionDemo(), sessionId, passwd);
		Thread.sleep(Integer.MAX_VALUE);
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println("Receive watched event:" + event);
		if (KeeperState.SyncConnected == event.getState()) {
			cdl.countDown();
		}
	}
}
