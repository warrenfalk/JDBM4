package net.kotek.jdbm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Implements a 62-bit address space using 32-bit buffers by dividing the
 * total space into 30-bit segments, each with one 32-bit buffer.
 * @author Warren Falk <warren@warrenfalk.com>
 *
 */
public abstract class SegmentedStorage implements Storage {
	ByteBuffer[] segments;
	long capacity;
	long size;
	
	final static int SEGMENT_BITS = 30;
	final static int SEGMENT_SIZE = 1 << SEGMENT_BITS;
	final static int ADDRESS_MASK = SEGMENT_SIZE - 1;

	public SegmentedStorage() {
		segments = new ByteBuffer[8];
	}
	
	abstract void grow(long requiredCapacity);
	
	@Override
	public long size() throws IOException {
		return size;
	}

	@Override
	public void putBytes(long address, byte[] buffer, int offset, int length) {
		long end = address + length;
		if (end > capacity)
			grow(end);
		if (end > size)
			size = end;
		ByteBuffer segment = segments[(int)(address / SEGMENT_SIZE)];
		segment.position((int)(address % SEGMENT_SIZE));
        segment.put(buffer, offset, length);
	}

	@Override
	public long getLong(long address) throws IOException {
		if (address > size())
			return 0L;
		int segment = (int)(address >> SEGMENT_BITS);
		int offset = (int)(address & ADDRESS_MASK);
		return segments[segment].getLong(offset);
	}

	@Override
	public void putLong(long address, long value) throws IOException {
		long end = address + 8;
		if (end > capacity)
			grow(end);
		if (end > size)
			size = end;
		int segment = (int)(address >> SEGMENT_BITS);
		int offset = (int)(address & ADDRESS_MASK);
		segments[segment].putLong(offset, value);
	}

	@Override
	public byte getByte(long address) throws IOException {
		if (address > size())
			return 0;
		int segment = (int)(address >> SEGMENT_BITS);
		int offset = (int)(address & ADDRESS_MASK);
		return segments[segment].get(offset);
	}

	@Override
	public void putByte(long address, byte value) throws IOException {
		long end = address + 1;
		if (end > capacity)
			grow(end);
		if (end > size)
			size = end;
		int segment = (int)(address >> SEGMENT_BITS);
		int offset = (int)(address & ADDRESS_MASK);
		segments[segment].put(offset, value);
	}

	@Override
	public void getBytes(long address, byte[] buffer, int bufoffset, int length) throws IOException {
		if (address > size())
			Arrays.fill(buffer, bufoffset, length, (byte)0);
		int segment = (int)(address >> SEGMENT_BITS);
		int offset = (int)(address & ADDRESS_MASK);
		ByteBuffer bb = segments[segment];
		// TODO: not thread safe
		bb.position(offset);
		bb.get(buffer, bufoffset, length);
	}

	@Override
	public short getShort(long address) throws IOException {
		if (address > size())
			return 0;
		int segment = (int)(address >> SEGMENT_BITS);
		int offset = (int)(address & ADDRESS_MASK);
		return segments[segment].getShort(offset);
	}

	@Override
	public int getInt(long address) throws IOException {
		if (address > size())
			return 0;
		int segment = (int)(address >> SEGMENT_BITS);
		int offset = (int)(address & ADDRESS_MASK);
		return segments[segment].getInt(offset);
	}

	@Override
	public float getFloat(long address) throws IOException {
		if (address > size())
			return 0.0f;
		int segment = (int)(address >> SEGMENT_BITS);
		int offset = (int)(address & ADDRESS_MASK);
		return segments[segment].getFloat(offset);
	}

	@Override
	public double getDouble(long address) throws IOException {
		if (address > size())
			return 0.0;
		int segment = (int)(address >> SEGMENT_BITS);
		int offset = (int)(address & ADDRESS_MASK);
		return segments[segment].getDouble(offset);
	}
}
