package com.dianping.fkv;

public class Record {

	private int index;

	private byte[] key;

	private byte[] value;

	public int getIndex() {
		return index;
	}

	public void setIndex(int startIndex) {
		this.index = startIndex;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

}
