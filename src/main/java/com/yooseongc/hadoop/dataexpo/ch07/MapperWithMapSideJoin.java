package com.yooseongc.hadoop.dataexpo.ch07;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.yooseongc.hadoop.util.CarrierCodeParser;
import com.yooseongc.hadoop.util.FlightInfo;
import com.yooseongc.hadoop.util.FlightInfoParser;

public class MapperWithMapSideJoin extends Mapper<LongWritable, Text, Text, Text> {

	private Hashtable<String, String> joinMap = new Hashtable<String, String>();
	private Text outputKey = new Text();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		try {
			
			// 분산 캐시 조회
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			// 조인 데이터 생성
			if (cacheFiles != null && cacheFiles.length > 0) {
				String line;
				BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
				try {
					while ((line = br.readLine()) != null) {
						CarrierCodeParser codeParser = new CarrierCodeParser(line);
						joinMap.put(codeParser.getCarrierCode(), codeParser.getCarrierName());
					}
				} finally {
					br.close();
				}
			} else {
				System.out.println("cache files are null");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		FlightInfo info = FlightInfoParser.parse(value);
		outputKey.set(info.getUniqueCarrier());
		context.write(outputKey, new Text(joinMap.get(info.getUniqueCarrier()) + "\t" + value.toString()));
	}
	
}
