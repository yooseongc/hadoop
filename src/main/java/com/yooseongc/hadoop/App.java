package com.yooseongc.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class App {
	public static void main(String[] args) throws Exception {
		Class<?> target = Class.forName(args[0]);
		Tool job = (Tool) target.newInstance();
		int res = ToolRunner.run(new Configuration(), job, args);
		System.out.println("MR-Job Result code : " + res);
	}
}
