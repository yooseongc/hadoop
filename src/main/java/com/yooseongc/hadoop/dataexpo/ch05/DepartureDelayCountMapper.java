package com.yooseongc.hadoop.dataexpo.ch05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.yooseongc.hadoop.util.FlightInfo;
import com.yooseongc.hadoop.util.FlightInfoParser;

/**
 * 
 * This mapper maps (line, Text data) * 1 => (yyyy,mm, count) * 1
 * A taskTracker will use this mapper by line.
 * @author yooseongc
 *
 */
public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	
	// output key, value member
	private final static IntWritable outputValue = new IntWritable(1); // for count
	private Text outputKey = new Text(); // save key : yyyymm
	
	// input and context will be parameter
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		FlightInfo info = FlightInfoParser.parse(value);
		if (info.isDepartureDelayAvailable()) {
			outputKey.set(info.getYear() + "," + info.getMonth());
			context.write(outputKey, outputValue);
		}
		
	}
	
}
