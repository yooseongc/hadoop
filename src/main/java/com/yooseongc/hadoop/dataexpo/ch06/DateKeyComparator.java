package com.yooseongc.hadoop.dataexpo.ch06;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateKeyComparator extends WritableComparator {

	protected DateKeyComparator() {
		super(DateKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateKey k1 = (DateKey) a;
		DateKey k2 = (DateKey) b;
		
		int cmp = k1.getYear().compareTo(k2.getYear());
		return (cmp != 0) 
				? cmp 
				: k1.getMonth() == k2.getMonth() 
				? 0
				: (k1.getMonth() < k2.getMonth() ? -1 : 1);
	}

}
