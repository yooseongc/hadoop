package com.yooseongc.hadoop.util;

import org.apache.hadoop.io.Text;

public class FlightInfoParser {

	public static FlightInfo parse(Text text) {
		
		try {
			
			String[] columns = text.toString().split(",");
			FlightInfo info = new FlightInfo();
			info.setYear(Integer.parseInt(columns[FlightInfoRecord.year.ordinal()]));
			info.setMonth(Integer.parseInt(columns[FlightInfoRecord.month.ordinal()]));
			info.setUniqueCarrier(columns[FlightInfoRecord.uniqueCarrier.ordinal()]);
			
			if (isAvailable(columns[FlightInfoRecord.departureDelayTime.ordinal()])) {
				info.setDepartureDelayTime(Integer.parseInt(columns[FlightInfoRecord.departureDelayTime.ordinal()]));
				info.setDepartureDelayAvailable(true);
			} else {
				info.setDepartureDelayAvailable(false);
			}
			
			if (isAvailable(columns[FlightInfoRecord.arriveDelayTime.ordinal()])) {
				info.setArriveDelayTime(Integer.parseInt(columns[FlightInfoRecord.arriveDelayTime.ordinal()]));
				info.setArriveDelayAvailable(true);
			} else {
				info.setArriveDelayAvailable(false);
			}
			
			if (isAvailable(columns[FlightInfoRecord.distance.ordinal()])) {
				info.setDistance(Integer.parseInt(columns[FlightInfoRecord.distance.ordinal()]));
				info.setDistanceAvailable(true);
			} else {
				info.setDistanceAvailable(false);
			}
			
			return info;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
	private static boolean isAvailable(String column) {
		return !"NA".equals(column);
	}
	
}
