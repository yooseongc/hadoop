package com.yooseongc.hadoop.dataexpo.ch06;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupKeyComparator extends WritableComparator {

	protected GroupKeyComparator() {
		super(DateKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateKey k1 = (DateKey) a;
		DateKey k2 = (DateKey) b;
		
		return k1.getYear().compareTo(k2.getYear());
	}

}
