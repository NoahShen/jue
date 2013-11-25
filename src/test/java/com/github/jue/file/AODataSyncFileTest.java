/**
 * 
 */
package com.github.jue.file;

import java.io.File;
import java.nio.ByteBuffer;

import org.junit.Test;

/**
 * @author noah
 *
 */
public class AODataSyncFileTest {
	
	@Test
	public void testWriteData() throws Exception {
		File blockFile = new File("/tmp/AODataSyncFile");
		AODataSyncFile file = new AODataSyncFile(blockFile, 
												AODataSyncFile.DEFAULT_BLOCK_SIZE, 
												true, 
												AODataSyncFile.DEFAULT_MAX_CACHE_CAPACITY, 
												4096);
		
		ByteBuffer newHeader = ByteBuffer.allocate(22);
		for (int i = 0; i < 22; ++i) {
			byte b = 8;
			newHeader.put(b);
		}
		newHeader.flip();
		
		ByteBuffer newData = ByteBuffer.allocate(1024);
		for (int i = 0; i < 1024; ++i) {
			byte b = 2;
			newData.put(b);
		}
		newData.flip();
		
		for (int i = 0; i < 11; ++i) {
			newHeader.rewind();
			newData.rewind();
			file.appendData(newHeader, newData);
		}
		Thread.sleep(10 * 1000);
		file.close();

	}
}
