/**
 * 
 */
package com.dianping.fkv.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author sean.wang
 * @since Nov 16, 2011
 */
public class FkvFileStore implements FkvStore {

	private RandomAccessFile in;
	private FileChannel ch;
	private MappedByteBuffer buffer;
	private final static int DEFAULT_DB_SIZE = 1024 * 1024;
	private static final byte STATUS_DELETE = '0';
	private static final byte STATUS_ACTIVE = '1';
	private static final byte NL = '\n';
	private boolean needDeserial = false;

	public FkvFileStore(File dbFile) throws IOException {
		this(dbFile, 0);
	}

	public FkvFileStore(File dbFile, int dbSize) throws IOException {
		if (!dbFile.exists()) {
			dbFile.createNewFile();
		}
		in = new RandomAccessFile(dbFile, "rw");
		ch = in.getChannel();
		long mappedSize = dbFile.length();
		if (mappedSize <= 0) {
			if (dbSize <= 0) {
				mappedSize = DEFAULT_DB_SIZE;
			} else {
				mappedSize = dbSize;
			}
		} else {
			this.needDeserial = true;
		}
		buffer = ch.map(FileChannel.MapMode.READ_WRITE, 0, mappedSize);
	}

	@Override
	public byte[] get(int startIndex, int size) {
		byte[] value = new byte[size];
		buffer.position(startIndex);
		buffer.get(value);
		return value;
	}

	@Override
	public void put(int startIndex, byte[] value) {
		buffer.position(startIndex);
		buffer.put(value);
	}

	@Override
	public void delete(int startIndex) {
		buffer.position(startIndex);
		buffer.put(STATUS_DELETE);
	}

	@Override
	public void active(int startIndex) {
		buffer.position(startIndex);
		buffer.put(STATUS_ACTIVE);
	}

	@Override
	public void next(int startIndex) {
		buffer.position(startIndex);
		buffer.put(NL);
	}

	@Override
	public void close() throws IOException {
		ch.close();
		in.close();
	}

	@Override
	public boolean isNeedDeserial() {
		return needDeserial;
	}

	@Override
	public ByteBuffer getBuffer() {
		return this.buffer;
	}

	@Override
	public boolean isValidRecord(byte[] recordBuf) {
		if (recordBuf[0] != STATUS_ACTIVE && recordBuf[0] != STATUS_DELETE) {
			return false;
		}
		if (recordBuf[recordBuf.length - 1] != NL) {
			return false;
		}
		return true;
	}

	@Override
	public boolean isDelete(byte[] record) {
		if (record[0] == STATUS_DELETE) {
			return true;
		}
		return false;
	}

	@Override
	public int remaining() {
		return this.buffer.remaining();
	}

	@Override
	public void rewind() {
		this.buffer.rewind();
	}

	@Override
	public void get(byte[] bytes) {
		this.buffer.get(bytes);
	}

}
