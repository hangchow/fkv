package fkv.store;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author sean.wang
 * @since Nov 16, 2011
 */
public interface FkvStore {
	byte[] get(int startIndex, int size);

	void put(int startIndex, byte[] value);
	
	void put(int startIndex, byte value);

	void close() throws IOException;

	boolean isNeedDeserial();

	ByteBuffer getBuffer();

	void rewind();

	int remaining();

	void get(byte[] bytes);
}
