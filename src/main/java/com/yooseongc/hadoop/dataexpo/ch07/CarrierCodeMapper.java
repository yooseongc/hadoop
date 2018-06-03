package com.yooseongc.hadoop.dataexpo.ch07;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.yooseongc.hadoop.util.CarrierCodeParser;

public class CarrierCodeMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {

	TaggedKey outputKey = new TaggedKey();
	Text outputValue = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		CarrierCodeParser parser = new CarrierCodeParser(value);
		outputKey.setCarrierCode(parser.getCarrierCode());
		outputKey.setTag(0);
		outputValue.set(parser.getCarrierName());
		
		context.write(outputKey, outputValue);
	}
	
}
