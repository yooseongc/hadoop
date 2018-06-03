package com.yooseongc.hadoop.dataexpo.ch06;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.yooseongc.hadoop.dataexpo.ch05.DelayCounter;
import com.yooseongc.hadoop.util.FlightInfo;
import com.yooseongc.hadoop.util.FlightInfoParser;

public class DelayCountMapperWithDateKey extends Mapper<LongWritable, Text, DateKey, IntWritable> {

	private DateKey outputKey = new DateKey();
	private final static IntWritable outputValue = new IntWritable(1);
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FlightInfo info = FlightInfoParser.parse(value);
		
		if (info.isDepartureDelayAvailable()) {
			if (info.getDepartureDelayTime() > 0) {
				outputKey.setYear("D," + info.getYear());
				outputKey.setMonth(info.getMonth());
				context.write(outputKey, outputValue);
			} else if (info.getDepartureDelayTime() == 0) {
				context.getCounter(DelayCounter.SCHEDULED_DEPARTURE).increment(1);
			} else if (info.getDepartureDelayTime() < 0) {
				context.getCounter(DelayCounter.EARLY_DEPARTURE).increment(1);
			}
		} else {
			context.getCounter(DelayCounter.NOT_AVAILABLE_DEPARTURE).increment(1);
		}
		
		if (info.isArriveDelayAvailable()) {
			if (info.getArriveDelayTime() > 0) {
				outputKey.setYear("A," + info.getYear());
				outputKey.setMonth(info.getMonth());
				context.write(outputKey, outputValue);
			} else if (info.getArriveDelayTime() == 0) {
				context.getCounter(DelayCounter.SCHEDULED_ARRIVAL).increment(1);
			} else if (info.getDepartureDelayTime() < 0) {
				context.getCounter(DelayCounter.EARLY_ARRIVAL).increment(1);
			}
		} else {
			context.getCounter(DelayCounter.NOT_AVAILABLE_ARRIVAL).increment(1);
		}
	}
	
}
