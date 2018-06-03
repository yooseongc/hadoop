package com.yooseongc.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

 
public class MRLauncher {
 
    static final String FS_DEFAULT_NAME = "hdfs://wikibooks01:9000";
    static final String MAPRED_JOB_TRACKER = "hdfs://wikibooks01:9001";
    static final String HADOOP_USER = "hadoop";
 
    public int invokeMR(String jobName, String[] args) {
 
        int res = 1;
        Configuration conf = new Configuration();
        conf.set("fs.default.name", FS_DEFAULT_NAME);
        conf.set("mapred.job.tracker", MAPRED_JOB_TRACKER);
        conf.set("hadoop.job.ugi", HADOOP_USER);
 
            try {
            	System.out.println(args[0] + ", " + args[1] + ", " + args[2]);
	        	Class<?> target = Class.forName(args[0]);
	    		Tool job = (Tool) target.newInstance();
	    		res = ToolRunner.run(conf, job, args);
	    		System.out.println("MR-Job["+jobName+"] Result code : "+res);
			} catch (Exception e) {
				e.printStackTrace();
			}
 
        return res;
    }
 
}