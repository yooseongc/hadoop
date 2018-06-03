package com.yooseongc.hadoop.util;


public class ArgumentUtil {

	public static void showUsage() {
		System.err.println("Usage: <job class full path> <input> <ouput>");
		System.exit(2);  // bad argument
	}
	
	public static void showUsageWithCache() {
		System.err.println("Usage: <job class full path>  <cacheFile Path> <input> <ouput>");
		System.exit(2);  // bad argument
	}
	
}
