package com.yooseongc.hadoop.dataexpo.ch07;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import com.yooseongc.hadoop.util.ArgumentUtil;

public class MapSideJoin extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if (otherArgs.length != 4) {
			ArgumentUtil.showUsageWithCache();
		}
		
		Job job = new Job(getConf(), "MapSideJoin");
		DistributedCache.addCacheFile(new Path(otherArgs[1]).toUri(), job.getConfiguration());
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		
		job.setJarByClass(MapSideJoin.class);
		job.setMapperClass(MapperWithMapSideJoin.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
		return 0;
	}

}
