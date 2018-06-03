package com.yooseongc.hadoop;

import org.junit.Test;

import com.yooseongc.hadoop.dataexpo.ch05.DepartureDelayCount;
import com.yooseongc.hadoop.util.FlightInfoRecord;

public class MRLauncherTest {

	@Test
    public void test () throws Exception {
 
//		ClassLoader cl = MRLauncher.class.getClassLoader();
//        String jarAbsolutePath = cl.getResource("com.yooseongc.hadoop-0.0.1-SNAPSHOT-jar-with-dependencies.jar").toString();
//        System.out.println(jarAbsolutePath);
//        
//        MRLauncher mrl = new MRLauncher();
//        String [] args = new String[3];
// 
//        args[0] = jarAbsolutePath;
//        args[1] = "/user/hadoop/input";
//        args[2] = "/user/hadoop/dep_delay_count";
// 
//        mrl.invokeMR(DepartureDelayCount.class.getName(), args);
    }
	
	@Test
	public void enumTest() {
		for (FlightInfoRecord c : FlightInfoRecord.values()) {
			System.out.println("key: " + c + "    value: " + c.ordinal());
		}
	}

}
