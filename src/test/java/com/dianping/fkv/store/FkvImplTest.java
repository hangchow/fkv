/**
 * 
 */
package com.dianping.fkv.store;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dianping.fkv.FkvImpl;

/**
 * @author sean.wang
 * @since Nov 17, 2011
 */
public class FkvImplTest {

	FkvImpl fkv;

	File dbFile;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		dbFile = new File("/tmp/fkvtest.db");
		dbFile.delete();
		fkv = new FkvImpl(dbFile, 10000, 8, 10);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		fkv.close();
		dbFile.delete();
	}

	/**
	 * Test method for {@link com.dianping.fkv.FkvImpl#get(java.lang.String)}.
	 */
	@Test
	public void testPutGet() {
		String key = "01234567";
		String value = "0123456789";
		fkv.put(key, value);
		Assert.assertEquals(value, fkv.get(key));
	}

	/**
	 * Test method for {@link com.dianping.fkv.FkvImpl#delete(java.lang.String)}.
	 */
	@Test
	public void testDelete() {
		String key = "01234567";
		String value = "0123456789";
		fkv.put(key, value);
		fkv.delete(key);
		Assert.assertNull(fkv.get(key));
	}

	/**
	 * Test method for {@link com.dianping.fkv.FkvImpl#get(java.lang.String)}.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testDeserial() throws IOException {
		String key = "01234567";
		String value = "0123456789";
		String key2 = "01234568";
		String value2 = "0123456780";
		fkv.put(key, value);
		fkv.put(key2, value2);
		fkv.delete(key2);
		fkv.close();
		fkv = new FkvImpl(dbFile, 10000, 8, 10);
		Assert.assertEquals(1, fkv.size());
		Assert.assertEquals(1, fkv.deleteSize());
		Assert.assertEquals(null, fkv.get(key2));
		Assert.assertEquals(value, fkv.get(key));
	}

	/**
	 * Test method for {@link com.dianping.fkv.FkvImpl#get(java.lang.String)}.
	 */
	@Test
	public void testPutSameKeyPerf() {
		String key = "01234567";
		String value = "0123456789";
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			fkv.put(key, value);
		}
		System.out.println("testPutSameKeyPerf:" + (System.currentTimeMillis() - start));
	}
	
	/**
	 * Test method for {@link com.dianping.fkv.FkvImpl#get(java.lang.String)}.
	 */
	@Test
	public void testGetSameKeyPerf() {
		String key = "01234567";
		String value = "0123456789";
		fkv.put(key, value);
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			fkv.get(key);
		}
		System.out.println("testGetSameKeyPerf:" + (System.currentTimeMillis() - start));
	}

	/**
	 * Test method for {@link com.dianping.fkv.FkvImpl#get(java.lang.String)}.
	 */
	@Test
	public void testPutDiffKeyPerf() {
		String value = "0123456789";
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			fkv.put("" + (10000000 + i), value);
		}
		System.out.println("testPutDiffKeyPerf:" + (System.currentTimeMillis() - start));
	}

	/**
	 * Test method for {@link com.dianping.fkv.FkvImpl#get(java.lang.String)}.
	 */
	@Test
	public void testPutDeletePerf() {
		String key = "01234567";
		String value = "0123456789";
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10000 / 2; i++) {
			fkv.put(key, value);
			fkv.delete(key);
		}
		System.out.println("testPutDeletePerf:" + (System.currentTimeMillis() - start));
	}

	/**
	 * Test method for {@link com.dianping.fkv.FkvImpl#get(java.lang.String)}.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testPutDeleteInOneRecordSize() throws IOException {
		String key = "01234567";
		String value = "0123456789";
		fkv = new FkvImpl(dbFile, 1, 8, 10);
		for (int i = 0; i < 1000; i++) {
			fkv.put(key, value);
			fkv.delete(key);
		}
	}

}
