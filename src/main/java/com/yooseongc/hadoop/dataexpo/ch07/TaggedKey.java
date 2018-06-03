package com.yooseongc.hadoop.dataexpo.ch07;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class TaggedKey implements WritableComparable<TaggedKey> {

	private String carrierCode;  // join key
	private Integer tag;         // which dataset?
	
	public TaggedKey() { }
	
	public TaggedKey(String carrierCode, int tag) {
		this.carrierCode = carrierCode;
		this.tag = tag;
	}
	
	
	
	public String getCarrierCode() {
		return carrierCode;
	}

	public void setCarrierCode(String carrierCode) {
		this.carrierCode = carrierCode;
	}

	public Integer getTag() {
		return tag;
	}

	public void setTag(Integer tag) {
		this.tag = tag;
	}

	public void readFields(DataInput input) throws IOException {
		carrierCode = WritableUtils.readString(input);
		tag = input.readInt();
	}

	public void write(DataOutput output) throws IOException {
		WritableUtils.writeString(output, carrierCode);
		output.writeInt(tag);
	}

	public int compareTo(TaggedKey key) {
		int result = this.carrierCode.compareTo(key.carrierCode);
		return (result == 0) ? this.tag.compareTo(key.tag) : result;
	}

}
