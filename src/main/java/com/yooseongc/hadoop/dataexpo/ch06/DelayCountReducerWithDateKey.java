package com.yooseongc.hadoop.dataexpo.ch06;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DelayCountReducerWithDateKey extends Reducer<DateKey, IntWritable, DateKey, IntWritable> {

	private MultipleOutputs<DateKey, IntWritable> mos;
	private DateKey outputKey = new DateKey();
	private IntWritable result = new IntWritable();
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<DateKey, IntWritable>(context);
	}
	
	public void reduce(DateKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		String[] cols = key.getYear().split(",");
		int sum = 0;
		Integer bMonth = key.getMonth();
		
		if ("D".equals(cols[0])) {
			for (IntWritable value : values) {
				if (bMonth != key.getMonth()) {
					result.set(sum);
					outputKey.setYear(cols[1]);
					outputKey.setMonth(bMonth);
					mos.write("departure", outputKey, result);
					sum = 0;
				}
				sum += value.get();
				bMonth = key.getMonth();
			}
			
			if (bMonth == key.getMonth()) {
				result.set(sum);
				outputKey.setYear(cols[1]);
				outputKey.setMonth(bMonth);
				mos.write("departure", outputKey, result);
			}
		} else {
			for (IntWritable value : values) {
				if (bMonth != key.getMonth()) {
					result.set(sum);
					outputKey.setYear(cols[1]);
					outputKey.setMonth(bMonth);
					mos.write("arrival", outputKey, result);
					sum = 0;
				}
				sum += value.get();
				bMonth = key.getMonth();
			}
			
			if (bMonth == key.getMonth()) {
				result.set(sum);
				outputKey.setYear(cols[1]);
				outputKey.setMonth(bMonth);
				mos.write("arrival", outputKey, result);
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
 	
}
