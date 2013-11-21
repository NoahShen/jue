/**
 * 
 */
package com.github.jue.util;

import com.github.jue.JueConstant;
import com.github.jue.compression.DataCompress;
import com.github.jue.doc.DocObject;
import com.github.jue.file.ADrop;
import com.github.jue.file.KeyRecord;
import com.github.jue.file.ValueRecord;

/**
 * 文档对象工具类
 * @author noah
 *
 */
public class DocUtils {
	
	/**
	 * 将doc转换成ValueRecord
	 * @param deleted
	 * @param docObj
	 * @param rev
	 * @return
	 * @throws Exception 
	 */
	public static ValueRecord docObjToValueRecord(boolean deleted, DocObject docObj, int rev, DataCompress compress) throws Exception {
		byte flag = deleted ? ADrop.FALSE_BYTE : ADrop.TRUE_BYTE;
		byte[] docBytes = null;
		ValueRecord valueRecord = null;
		if (docObj != null) {
			String json = docObj.toString();
			docBytes = json.getBytes(JueConstant.CHARSET);
			if (compress != null) {
				docBytes = compress.compress(docBytes);
			}
		} else {
			docBytes = new byte[0];
		}		
		valueRecord = new ValueRecord(flag, docBytes, rev);
		return valueRecord;
	}

	/**
	 * 将ValueRecord转换成doc
	 * @param valueRecord
	 * @param compress
	 * @return
	 * @throws Exception
	 */
	public static DocObject valueRecordToDocObj(ValueRecord valueRecord, DataCompress compress) throws Exception {
		if (!valueRecord.isDeleted()) {// 数据存在
			byte[] values = valueRecord.getValue();
			if (compress != null) {
				values = compress.decompress(values);
			}
			String valueStr = new String(values, JueConstant.CHARSET);
			DocObject docObj = new DocObject(valueStr);
			return docObj;
		}
		return null;
	}
	
	/**
	 * 创建KeyRecord
	 * @param deleted
	 * @param keyBytes
	 * @param rev
	 * @param revRootNode
	 * @param lastestValue
	 * @return
	 */
	public static KeyRecord createKeyRecord(boolean deleted, byte[] keyBytes, int rev, long revRootNode, long lastestValue) {
		byte flag = deleted ? ADrop.FALSE_BYTE : ADrop.TRUE_BYTE;
		return new KeyRecord(flag, keyBytes, revRootNode, rev, lastestValue);
	}
	
	
}
