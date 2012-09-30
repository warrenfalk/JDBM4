package net.kotek.jdbm;

import java.io.IOException;

/**
 * Interface to backend byte-level serial storage.
 * @author Warren Falk <warren@warrenfalk.com>
 *
 */
public interface Storage {
	long size() throws IOException;
	
	void close() throws IOException;
	
	void putBytes(long address, byte[] buffer, int offset, int length) throws IOException;
	
	long getLong(long address) throws IOException;
	
	void putLong(long address, long value) throws IOException;
	
	byte getByte(long address) throws IOException;
	
	void putByte(long address, byte value) throws IOException;

	void getBytes(long address, byte[] buffer, int offset, int length) throws IOException;

	short getShort(long address) throws IOException;

	int getInt(long address) throws IOException;

	float getFloat(long address) throws IOException;

	double getDouble(long address) throws IOException;
}
