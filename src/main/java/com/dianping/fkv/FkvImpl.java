/**
 * 
 */
package com.dianping.fkv;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
		return activeCache;
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
		this.activeCache = new ConcurrentHashMap<String, Record>(maxRecordSize);
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
					index += recordLength;
					Record r = new Record();
					byte[] keyBuf = new byte[keyLength];
					System.arraycopy(recordBuf, STATUS_LENGTH, keyBuf, 0, keyLength);
					r.setKey(keyBuf);
					r.setIndex(index);
					byte[] valueBuf = new byte[valueLength];
					System.arraycopy(recordBuf, STATUS_LENGTH + keyLength, valueBuf, 0, valueLength);
					r.setValue(valueBuf);
					if (store.isDelete(recordBuf)) {
						this.deletedCache.push(r);
					} else {
						this.activeCache.put(new String(r.getKey()), r);
					}
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
	
	public int deleteSize() {
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
			return new String(record.getValue());
		} finally {
			lock.readLock().unlock();
		}
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
				putRecordValue(record);
			}
		} finally {
			lock.writeLock().unlock();
		}

	}

	protected void putRecordValue(Record record) {
		this.store.put(record.getIndex() + STATUS_LENGTH + keyLength, record.getValue());
	}

	protected void putNewRecord(String key, String value) {
		Record newRecord = new Record();
		newRecord.setValue(value.getBytes());
		int index;
		if (this.deletedCache.isEmpty()) { // no deleted record
			index = endIndex;
			endIndex += this.recordLength;
		} else {
			Record deletedRecord = this.deletedCache.pop();
			index = deletedRecord.getIndex();
		}
		newRecord.setIndex(index);
		this.store.active(index); // (byte)1
		this.store.put(index + STATUS_LENGTH, key.getBytes()); // key
		putRecordValue(newRecord); // value
		this.store.next(index + STATUS_LENGTH + keyLength + valueLength); // \n
		this.activeCache.put(key, newRecord);
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
