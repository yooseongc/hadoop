package com.yooseongc.hadoop.dataexpo.ch06;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupKeyPartitiioner extends Partitioner<DateKey, IntWritable> {

	@Override
	public int getPartition(DateKey key, IntWritable val, int numPartitions) {
		int hash = key.getYear().hashCode();
		return hash % numPartitions;
	}

}
