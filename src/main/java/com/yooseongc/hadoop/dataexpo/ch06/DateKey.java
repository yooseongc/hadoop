package com.yooseongc.hadoop.dataexpo.ch06;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class DateKey implements WritableComparable<DateKey> {

	private String year;
	private Integer month;
	
	public DateKey() { }
	public DateKey(String year, Integer month) {
		this.year = year;
		this.month = month;
	}
	
	public void readFields(DataInput input) throws IOException {
		year = WritableUtils.readString(input);
		month = input.readInt();
	}

	public void write(DataOutput output) throws IOException {
		WritableUtils.writeString(output, year);
		output.writeInt(month);
	}
	public int compareTo(DateKey key) {
		int result = year.compareTo(key.year);
		return result = (result == 0) ? month.compareTo(key.month) : result;
	}
	
	
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public Integer getMonth() {
		return month;
	}
	public void setMonth(Integer month) {
		this.month = month;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder().append(year).append(",").append(month)).toString();
	}

}
