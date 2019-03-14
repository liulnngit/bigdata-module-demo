package com.javadev.hbase;

import java.util.List;

import com.javadev.demo.FingerVO;

public class Client {
	   
	   public static void main(String[] args) throws Exception {
	        //System.out.println(HBaseUtil.getFromRowkeyValuesToJson("t_finger", "3a560983e01293ffae27d5525e4437d4").toJSONString());
		   	//HBaseUtil.getValueByTable("t_test1");
	       List<FingerVO>  fingers = HBaseUtil.getFinger("t_finger");
		   //System.out.println("c5e31f30f84846f78ed657d1a276c202_620186050214531989".length()*370000/8/1000/1024);
	       for (FingerVO finger : fingers) {
	    	   System.out.println(finger.toString());
	       }
	       
	    }
	   
	   
}


