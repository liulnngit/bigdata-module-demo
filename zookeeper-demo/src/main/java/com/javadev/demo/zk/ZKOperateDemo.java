package com.javadev.demo.zk;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 解释如何去创建znode并监听事件
 * @author ll-t150
 */
public class ZKOperateDemo implements Watcher{
	private static final CountDownLatch cdl = new CountDownLatch(1);
	
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		ZooKeeper zk = new ZooKeeper("192.168.46.160:2181",5000,new ZKOperateDemo());
		cdl.await();
		String path1 = zk.create("/zk-test-", "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		System.out.println("Success create path:"+path1);
		String path2 = zk.create("/zk-test-", "456".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("Success create path:"+path2);
	}
	
	@Override
	public void process(WatchedEvent event) {
		System.out.println("Reveived The Event:"+event);
		if(KeeperState.SyncConnected == event.getState()){
			cdl.countDown();
		}
	}
	
	
}
