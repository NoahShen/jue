/**
 * 
 */
package com.github.jue.compression.quicklz;

import com.github.jue.compression.DataCompress;

/**
 * @author SHENPEIQI807
 *
 */
public class QuickLZDataCompress implements DataCompress {

	@Override
	public byte[] compress(byte[] data) throws Exception {
		return QuickLZ.compress(data);
	}

	@Override
	public byte[] decompress(byte[] data) throws Exception {
		return QuickLZ.decompress(data);
	}

}
