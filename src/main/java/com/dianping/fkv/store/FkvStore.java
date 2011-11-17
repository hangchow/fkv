package com.dianping.fkv.store;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author sean.wang
 * @since Nov 16, 2011
 */
public interface FkvStore {
	byte[] get(int startIndex, int size);

	void put(int startIndex, byte[] value);

	void delete(int startIndex);

	void active(int startIndex);

	void next(int startIndex);

	void close() throws IOException;

	boolean isNeedDeserial();

	ByteBuffer getBuffer();

	boolean isValidRecord(byte[] record);
	
	boolean isDelete(byte[] record);

	void rewind();

	int remaining();

	void get(byte[] bytes);
}
