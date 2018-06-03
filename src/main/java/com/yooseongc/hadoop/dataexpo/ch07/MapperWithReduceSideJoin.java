package com.yooseongc.hadoop.dataexpo.ch07;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.yooseongc.hadoop.util.FlightInfo;
import com.yooseongc.hadoop.util.FlightInfoParser;

public class MapperWithReduceSideJoin extends Mapper<LongWritable, Text, TaggedKey, Text> {

	private TaggedKey outputKey = new TaggedKey();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		FlightInfo info = FlightInfoParser.parse(value);
		outputKey.setCarrierCode(info.getUniqueCarrier());
		outputKey.setTag(1);
		context.write(outputKey, value);
	}
	
}
