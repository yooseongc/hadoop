package com.yooseongc.hadoop.dataexpo.ch07;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TaggedGroupKeyPartitioner extends Partitioner<TaggedKey, Text> {

	@Override
	public int getPartition(TaggedKey key, Text val, int num) {
		int hash = key.getCarrierCode().hashCode();
		return hash % num;
	}

}
