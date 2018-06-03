package com.yooseongc.hadoop.dataexpo.ch06;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import com.yooseongc.hadoop.util.ArgumentUtil;

public class SearchValueList extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if (otherArgs.length != 3) {
			ArgumentUtil.showUsage();
		}
		
		Path path = new Path(args[1]);
		FileSystem fs = path.getFileSystem(getConf());
		System.out.println(fs.getUri());
		
		Reader[] readers = MapFileOutputFormat.getReaders(fs, path, getConf());
		
		IntWritable key = new IntWritable(Integer.parseInt(args[2]));
		Text value = new Text();
		
		Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();
		Reader reader = readers[partitioner.getPartition(key, value, readers.length)];
		
		Writable entry = reader.get(key, value);
		if (entry == null) System.out.println("The requested key was not found.");
		
		IntWritable nextKey = new IntWritable();
		do {
			System.out.println(value.toString());
		} while (reader.next(nextKey, value) && key.equals(nextKey));
		
		return 0;
	}
}
