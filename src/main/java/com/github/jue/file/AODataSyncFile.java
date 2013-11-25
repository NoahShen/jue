/**
 * 
 */
package com.github.jue.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.github.jue.util.ConcurrentLRUCache;

/**
 * Append-only data sync file class
 * @author noah
 *
 */
public class AODataSyncFile {
	/**
	 * 最大缓存数
	 */
	public static final int DEFAULT_MAX_CACHE_CAPACITY = 100000;
	/**
	 * 默认的文件块大小
	 */
	public static final int DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024; //64MB
	
	/**
	 * 默认新数据的缓冲区大小
	 */
	public static final int DEFAULT_NEW_DATA_BUFFER_SIZE = 512 * 1024 * 1024; //512MB
	
	/**
	 * 文件
	 */
	private File file;
	
	/**
	 * 文件Channel对象
	 */
	private FileChannel fileChannel;
	

	/**
	 * 块大小
	 */
	private final int blockSize;

	/**
	 * 是否缓存文件块
	 */
	private final boolean blockCache;
	
	/**
	 * 缓存
	 */
	private ConcurrentLRUCache<Long, ByteBuffer> cache;
	
	/**
	 * 新写入数据的缓存
	 */
	private ByteBuffer dataBuffer;
	
	/**
	 * 头文件的缓存
	 */
	private ByteBuffer headerBuffer;

	/**
	 * 缓存锁
	 */
	private final ReentrantReadWriteLock bufferLock = new ReentrantReadWriteLock();
	
	/**
	 * 缓存读锁
	 */
	private final ReadLock readBufferLock = bufferLock.readLock();
	
	/**
	 * 缓存写锁
	 */
	private final WriteLock writeBufferLock = bufferLock.writeLock();
	
	
	private SyncBuffterStrategy syncStrategy;
	
	/**
	 * 创建AODataSyncFile
	 * @param file
	 * @param blockSize
	 * @param blockCache
	 * @param maxCacheCapacity
	 * @param newDataBufferSize
	 * @throws FileNotFoundException
	 */
	public AODataSyncFile(File file, int blockSize, boolean blockCache, int maxCacheCapacity, int newDataBufferSize) throws FileNotFoundException {
		super();
		this.file = file;
		@SuppressWarnings("resource")
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		this.fileChannel = raf.getChannel();
		this.blockSize = blockSize;
		this.blockCache = blockCache;
		if (blockCache) {
			cache = new ConcurrentLRUCache<Long, ByteBuffer>(maxCacheCapacity);
		}
		dataBuffer = ByteBuffer.allocate(newDataBufferSize);
		dataBuffer.flip();// position:0,limit:0
		headerBuffer = ByteBuffer.allocate(FileHeader.HEADER_SIZE);
		
		syncStrategy = new SyncBuffterStrategy();
	}

	/**
	 * 返回文件大小，包括缓存
	 * @return
	 * @throws IOException 
	 */
	public long size() throws IOException {
		return fileChannel.size() + dataBuffer.limit();
	}
	
	public File getFile() {
		return file;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public boolean isBlockCache() {
		return blockCache;
	}
	
	/**
	 * 写入数据，返回写入的地址
	 * @param dataBuffer
	 * @return 写入的地址
	 * @throws IOException
	 */
	public long appendData(ByteBuffer newHeaderBuffer, ByteBuffer newDataBuffer) throws IOException {
		writeBufferLock.lock();
		try	{
			return syncStrategy.append(newHeaderBuffer, newDataBuffer);
		} finally {
			writeBufferLock.unlock();
		}
	}

	/**
	 * 将缓存数据写入磁盘
	 * @throws IOException 
	 */
	private void writeBufferToFile() throws IOException {
		dataBuffer.rewind();
		headerBuffer.rewind();
		long writePos = fileChannel.size();
		fileChannel.write(dataBuffer, writePos);
		dataBuffer.clear().flip();
		fileChannel.write(headerBuffer, 0);
		headerBuffer.clear();
	}
	
	/**
	 * 将新数据写入缓存
	 * @param newHeaderBuffer
	 * @param newDataBuffer
	 * @return
	 * @throws IOException
	 */
	private long writeDataToBuffer(ByteBuffer newHeaderBuffer, ByteBuffer newDataBuffer) throws IOException {
		long writePos = this.size();
		int size = newDataBuffer.remaining();

		int newPos = this.dataBuffer.limit();
		this.dataBuffer.limit(newPos + size);
		this.dataBuffer.position(newPos);
		this.dataBuffer.put(newDataBuffer);

		this.headerBuffer.clear();
		this.headerBuffer.put(newHeaderBuffer);

		clearBlockCache(writePos, size);
		return writePos;
	}
	
	private void clearBlockCache(long pos, int size) {
		// 清空缓存
		if (blockCache) {
			long[] blockIdxs = getBlockIndexes(pos, size);
			for (int i = 0; i < blockIdxs.length; ++i) {
				cache.remove(blockIdxs[i]);
			}
		}
	}
	
	/**
	 * 获取即将写入的数据对应的文件块的位置
	 * 
	 * @param pos
	 *            文件位置
	 * @param size
	 *            要获取的长度
	 * @return
	 */
	private long[] getBlockIndexes(long pos, int size) {
		long removeHeaderPos = pos - FileHeader.HEADER_SIZE;
		// 起始文件块的位置
		long startBlockIndex = removeHeaderPos / blockSize;
		// 确定结束的块位置
		long endBlockIndex = (removeHeaderPos + size) / blockSize;
		// 需要读取的文件块的个数
		int count = (int) (endBlockIndex - startBlockIndex + 1);
		// 获取各文件块位置
		long[] indexes = new long[count];
		for (int i = 0; i < count; ++i, ++startBlockIndex) {
			indexes[i] = startBlockIndex;
		}
		return indexes;
	}
	
	public void close() throws IOException {
		writeBufferLock.lock();
		try	{	
			syncStrategy.close();
			fileChannel.close();
			cache.clear();
			cache = null;
			dataBuffer.clear();
			headerBuffer.clear();
		} finally {
			writeBufferLock.unlock();
		}
	}
	
	/**
	 * 使用缓存策略，当写入数据时，只写入缓存，除非缓存的数据超过最大缓存时候，或者在close的时候，才会写入磁盘
	 * @author noah
	 *
	 */
	public class SyncBuffterStrategy {
		
		public long append(ByteBuffer newHeaderBuffer, ByteBuffer newDataBuffer) throws IOException {
			// 需要写入的数据长度
			int dataSize = newDataBuffer.remaining();
			if (dataSize > AODataSyncFile.this.dataBuffer.capacity()) {
				throw new IllegalArgumentException("The new data size is bigger than databuffer capacity!");
			}
			
			// 超出缓存最大限制，先写入磁盘
			if (AODataSyncFile.this.dataBuffer.limit() + dataSize > AODataSyncFile.this.dataBuffer.capacity()) {
				writeBufferToFile();
			}
			return writeDataToBuffer(newHeaderBuffer, newDataBuffer);
		}
		
		public void close() throws IOException {
			writeBufferToFile();
		}

	}
}
