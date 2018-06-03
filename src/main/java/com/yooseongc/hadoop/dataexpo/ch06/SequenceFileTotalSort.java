package com.yooseongc.hadoop.dataexpo.ch06;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import com.yooseongc.hadoop.util.ArgumentUtil;

public class SequenceFileTotalSort extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if (otherArgs.length != 3) {
			ArgumentUtil.showUsage();
		}
		
		JobConf conf = new JobConf(getConf(), SequenceFileTotalSort.class);
		conf.setJobName(args[0]);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setPartitionerClass(TotalOrderPartitioner.class);
		
		SequenceFileOutputFormat.setCompressOutput(conf, true);
		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);
		
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		
		Path inputDir = FileInputFormat.getInputPaths(conf)[0];
		inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf));
		Path partitionFile = new Path(inputDir, "_partitions");
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		
		InputSampler.Sampler<IntWritable, Text> sampler = 
				new InputSampler.RandomSampler<IntWritable, Text>(0.1, 1000, 10);
		InputSampler.writePartitionFile(conf, sampler);
		
		URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
		DistributedCache.addCacheFile(partitionUri, conf);
		DistributedCache.createSymlink(conf);
		
		JobClient.runJob(conf);
		return 0;
	}

}
