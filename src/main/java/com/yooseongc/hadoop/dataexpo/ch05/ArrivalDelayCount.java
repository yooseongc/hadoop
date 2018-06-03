package com.yooseongc.hadoop.dataexpo.ch05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;


/**
 * Runner for job
 * @author yooseongc
 *
 */
public class ArrivalDelayCount implements Tool {

	public int run(String[] args) {
		if (args.length != 3) {
			System.err.println("Usage: ArrivalDelayCount <input> <ouput>");
			System.exit(2);  // bad argument
		}
		
		
		try {
			// define job and config
			Job job = new Job(getConf(), ArrivalDelayCount.class.getName());
			// path setting
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			System.out.println("path setting finished.");
			
			// set classes 
			job.setJarByClass(ArrivalDelayCount.class);
			job.setMapperClass(ArrivalDelayCountMapper.class);
			job.setReducerClass(DelayCountReducer.class);
			System.out.println("MR setting finished.");
			
			// set dataformat classes (read and write text)
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			System.out.println("text output setting finished.");
			
			// output key, value classes
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			System.out.println("output key,value setting finished.");
			
			
			System.out.println("job start.");
			job.waitForCompletion(true); // run job and wait until finished with verbose
			
			return 0;
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}
		
	}

	public Configuration getConf() {
		return new Configuration();
	}

	public void setConf(Configuration conf) {
		return;
	}
	
	
}
