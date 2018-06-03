package com.yooseongc.hadoop;

import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

public class HDFSClientTest {

	
	static final String FS_DEFAULT_NAME = "hdfs://wikibooks01:9000";
    static final String MAPRED_JOB_TRACKER = "hdfs://wikibooks01:9001";
    static final String HADOOP_USER = "hadoop";
    
    /*@Test
	public void clientConnectionTest() throws IOException {
		HDFSClient client = HDFSClient.getInstance(FS_DEFAULT_NAME, MAPRED_JOB_TRACKER, HADOOP_USER);
		System.out.println(client.toString());
		client.close();
	}
    */
    
    @Test
    public void inspectTest() throws IOException {
    	
    	UserGroupInformation ugi = UserGroupInformation.createRemoteUser(HADOOP_USER);
    	
    	ugi.doAs(new PrivilegedAction<Void>() {
			public Void run() {
				HDFSClient client;
				try {
					client = HDFSClient.getInstance().connect(FS_DEFAULT_NAME, MAPRED_JOB_TRACKER, HADOOP_USER);
					System.out.println("URI : " + client.getFileSystem().getUri());
					client.inspectPath("hdfs://wikibooks01:9000/user/hadoop");
					client.close();
				} catch (IOException e) {
					e.printStackTrace();
				} 
				return null;
			}
		});
    	
    	FileSystem.printStatistics();
    }

}
