/**
 * 
 */
package com.dianping.fkv;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.fkv.store.FkvFileStore;
import com.dianping.fkv.store.FkvStore;

/**
 * Fixed length key-value store implement.
 * 
 * @author sean.wang
 * @since Nov 16, 2011
 */
public class FkvImpl implements Fkv {
	private static final Log log = LogFactory.getLog(FkvImpl.class);

	private Map<String, Record> activeCache;
	private Deque<Record> deletedCache;
	private FkvStore store;
	private ReadWriteLock lock = new ReentrantReadWriteLock();
	private int endIndex = 0;
	private static final int STATUS_LENGTH = 1;
	private static final int SPLIT_LENGTH = 1;
	private int keyLength;
	private int valueLength;
	private int recordLength;
	private int maxRecordSize;

	public Map<String, Record> getActiveCache() {
		return this.activeCache;
	}

	public Deque<Record> getDeletedCache() {
		return this.deletedCache;
	}

	public FkvStore getStore() {
		return store;
	}

	public int getEndIndex() {
		return endIndex;
	}

	public int getKeyLength() {
		return keyLength;
	}

	public int getValueLength() {
		return valueLength;
	}

	public int getRecordLength() {
		return recordLength;
	}

	public int getMaxRecordSize() {
		return maxRecordSize;
	}

	public FkvImpl(File dbFile, int fixedKeyLength, int fixedValueLength) throws IOException {
		this(dbFile, 0, fixedKeyLength, fixedValueLength);
	}

	public FkvImpl(File dbFile, int maxRecordSize, int keyLength, int valueLength) throws IOException {
		this.keyLength = keyLength;
		this.valueLength = valueLength;
		this.recordLength = STATUS_LENGTH + keyLength + valueLength + SPLIT_LENGTH;
		this.maxRecordSize = maxRecordSize;
		// init store
		this.store = new FkvFileStore(dbFile, this.recordLength * maxRecordSize);
		// init active cache
		this.activeCache = new HashMap<String, Record>(maxRecordSize);
		// init deleted stack
		this.deletedCache = new ArrayDeque<Record>();
		deserial(keyLength, valueLength);
	}

	protected void deserial(int keyLength, int valueLength) {
		if (this.store.isNeedDeserial()) {
			byte[] recordBuf = new byte[recordLength];
			int index = 0;
			while (this.store.remaining() > 0) {
				store.get(recordBuf);
				if (store.isValidRecord(recordBuf)) {
					byte[] keyBuf = new byte[keyLength];
					System.arraycopy(recordBuf, STATUS_LENGTH, keyBuf, 0, keyLength);
					byte[] valueBuf = new byte[valueLength];
					System.arraycopy(recordBuf, STATUS_LENGTH + keyLength, valueBuf, 0, valueLength);
					Record record = this.createNewRecord(new String(keyBuf), new String(valueBuf), index);
					if (store.isDelete(recordBuf)) {
						this.deletedCache.push(record);
					} else {
						this.activeCache.put(record.getStringKey(), record);
					}
					index += recordLength;
				} else {
					log.error("break because error record:" + new String(recordBuf));
					break;
				}
			}
			this.endIndex = index;
		}
	}

	@Override
	public int size() {
		return this.activeCache.size();
	}

	public int getDeletedSize() {
		return this.deletedCache.size();
	}

	@Override
	public String get(String key) {
		try {
			lock.readLock().lock();
			Record record = this.activeCache.get(key);
			if (record == null) {
				return null;
			}
			return record.getStringValue();
		} finally {
			lock.readLock().unlock();
		}
	}

	public Record getRecord(String key) {
		Record record = null;
		try {
			lock.readLock().lock();
			record = this.activeCache.get(key);
		} finally {
			lock.readLock().unlock();
		}
		return record;
	}

	@Override
	public void put(String key, String value) {
		int keyLength = this.keyLength;
		if (key == null || key.getBytes().length != keyLength) {
			throw new IllegalArgumentException("key:" + key);
		}
		if (value == null || value.getBytes().length != this.valueLength) {
			throw new IllegalArgumentException("value:" + value);
		}
		if (size() >= this.maxRecordSize) {
			throw new StackOverflowError("size:" + size());
		}
		try {
			lock.writeLock().lock();
			Record record = this.activeCache.get(key);
			if (record == null) {
				putNewRecord(key, value);
			} else {
				byte[] valueBytes = value.getBytes();
				record.setValue(valueBytes);
				record.setStringValue(value);
				putRecordValue(record);
			}
		} finally {
			lock.writeLock().unlock();
		}

	}

	private void putNewRecord(String key, String value) {
		int index;
		if (this.deletedCache.isEmpty()) { // no deleted record
			index = endIndex;
			endIndex += this.recordLength;
		} else {
			Record deletedRecord = this.deletedCache.pop();
			index = deletedRecord.getIndex();
		}
		Record newRecord = createNewRecord(key, value, index);
		storeNewRecord(newRecord); // first store record
		cacheNewRecord(key, newRecord); // second cache record
	}

	private void cacheNewRecord(String key, Record newRecord) {
		this.activeCache.put(key, newRecord);
	}

	private void storeNewRecord(Record newRecord) {
		putRecordStart(newRecord); // (byte)1
		putRecordKey(newRecord); // key
		putRecordValue(newRecord); // value
		putRecordEnded(newRecord);// \n
	}

	private Record createNewRecord(String key, String value, int index) {
		Record newRecord = new Record();
		newRecord.setValue(value.getBytes());
		newRecord.setStringValue(value);
		newRecord.setKey(key.getBytes());
		newRecord.setStringKey(key);
		newRecord.setIndex(index);
		return newRecord;
	}

	private void putRecordStart(Record record) {
		this.store.active(record.getIndex());
	}

	private void putRecordEnded(Record record) {
		this.store.next(record.getIndex() + STATUS_LENGTH + keyLength + valueLength);
	}

	private void putRecordKey(Record record) {
		this.store.put(record.getIndex() + STATUS_LENGTH, record.getKey());

	}

	private void putRecordValue(Record record) {
		this.store.put(record.getIndex() + STATUS_LENGTH + keyLength, record.getValue());
	}

	@Override
	public void delete(String key) {
		try {
			lock.writeLock().lock();
			Record r = this.activeCache.get(key);
			if (r != null) {
				this.store.delete(r.getIndex());
				Record deletedRecord = this.activeCache.remove(key);
				this.deletedCache.add(deletedRecord);
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public void close() throws IOException {
		this.store.close();
	}

}
