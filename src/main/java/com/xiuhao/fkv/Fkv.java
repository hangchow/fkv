/**
 * 
 */
package com.xiuhao.fkv;

import java.io.IOException;

/**
 * @author sean.wang
 * @since Nov 15, 2011
 */
public interface Fkv {

	/**
	 * get record by key
	 * 
	 * @param key
	 * @return
	 */
	String get(String key);

	/**
	 * put record
	 * 
	 * @param key
	 * @param value
	 */
	void put(String key, String value);

	/**
	 * delete record
	 * 
	 * @param key
	 */
	void delete(String key);

	/**
	 * active record size
	 * 
	 * @return
	 */
	int size();

	/**
	 * close fkv
	 * 
	 * @throws IOException
	 */
	void close() throws IOException;

	/**
	 * delete all records
	 */
	void clear();

}
