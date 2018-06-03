package com.yooseongc.hadoop.dataexpo.ch07;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaggedKeyComparator extends WritableComparator {

	protected TaggedKeyComparator() {
		super(TaggedKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TaggedKey k1 = (TaggedKey) w1;
		TaggedKey k2 = (TaggedKey) w2;
		int cmp = k1.getCarrierCode().compareTo(k2.getCarrierCode());
		
		return (cmp != 0) ? cmp : k1.getTag().compareTo(k2.getTag());
	}
}
