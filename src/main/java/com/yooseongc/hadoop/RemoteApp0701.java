package com.yooseongc.hadoop;

import java.security.PrivilegedAction;

import org.apache.hadoop.security.UserGroupInformation;

public class RemoteApp0701 {

	static final String HADOOP_USER = "hadoop";
	
	public static void main(String[] args) {
		
        
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(HADOOP_USER);
    	ugi.doAs(new PrivilegedAction<Void>() {

			public Void run() {
				String [] args = new String[4];
				MRLauncher mrl = new MRLauncher();
	            args[0] = "com.yooseongc.hadoop.dataexpo.ch07.MapSideJoin";
	            args[1] = "meta/carriers.csv";
	            args[2] = "input";
	            args[3] = "map_join2";
	            mrl.invokeMR("remoteJob", args);
				return null;
			} 
    		
    		
    	});
		
	}

}
