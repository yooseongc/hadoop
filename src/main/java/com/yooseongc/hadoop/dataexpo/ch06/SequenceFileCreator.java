package com.yooseongc.hadoop.dataexpo.ch06;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import com.yooseongc.hadoop.util.ArgumentUtil;
import com.yooseongc.hadoop.util.FlightInfo;
import com.yooseongc.hadoop.util.FlightInfoParser;

public class SequenceFileCreator extends Configured implements Tool {

	static class DistanceMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

		private IntWritable outputKey = new IntWritable();
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			
			try {
				FlightInfo info = FlightInfoParser.parse(value);
				if (info.isDistanceAvailable()) {
					outputKey.set(info.getDistance());
					output.collect(outputKey, value);
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				outputKey.set(0);
				output.collect(outputKey, value);
				e.printStackTrace();
			} catch (Exception e) {
				outputKey.set(0);
				output.collect(outputKey, value);
				e.printStackTrace();
			}
		}
		
	}

	public int run(String[] args) throws Exception {
		
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if (otherArgs.length != 3) {
			ArgumentUtil.showUsage();
		}
		
		JobConf conf = new JobConf(SequenceFileCreator.class);
		conf.setJobName("SequenceFileCreator");
		
		conf.setMapperClass(DistanceMapper.class);
		conf.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		SequenceFileOutputFormat.setCompressOutput(conf, true);
		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);
		JobClient.runJob(conf);
		return 0;
	}

}
