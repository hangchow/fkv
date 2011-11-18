package com.dianping.fkv;

public class Record {

	private int index;

	private byte[] key;

	private byte[] value;

	private String stringKey;

	private String stringValue;

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

	public String getStringKey() {
		return stringKey;
	}

	public String getStringValue() {
		return stringValue;
	}

	public void setStringKey(String stringKey) {
		this.stringKey = stringKey;
	}

	public void setStringValue(String stringValue) {
		this.stringValue = stringValue;
	}

}
