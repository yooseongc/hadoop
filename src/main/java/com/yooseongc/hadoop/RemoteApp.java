package com.yooseongc.hadoop;

import java.security.PrivilegedAction;

import org.apache.hadoop.security.UserGroupInformation;

public class RemoteApp {

	static final String HADOOP_USER = "hadoop";
	
	public static void main(String[] args) {
		
        
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(HADOOP_USER);
    	ugi.doAs(new PrivilegedAction<Void>() {

			public Void run() {
				String [] args = new String[3];
				MRLauncher mrl = new MRLauncher();
	            args[0] = "com.yooseongc.hadoop.dataexpo.ch06.SequenceFileTotalSort";
	            args[1] = "2008_sequencefile";
	            args[2] = "2008_totalsort";
	            mrl.invokeMR("remoteJob", args);
				return null;
			} 
    		
    		
    	});
		
	}

}
