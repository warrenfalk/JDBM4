package net.kotek.jdbm;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;

/**
 * Filesystem-based implementation of Storage interface
 * @author Warren Falk <warren@warrenfalk.com>
 *
 */
public class FileStorage extends SegmentedStorage {
	FileChannel channel;

	final static int GROW_SIZE = 1 << 23;
	
	public FileStorage(File file) throws IOException {
		super();
		channel = new RandomAccessFile(file, "rw").getChannel();
		size = channel.size();
		mapAllSegments();
	}
	
	private void mapAllSegments() throws IOException {
		long size = size();
		long address = 0;
		int segment = 0;
		while (address < size) {
			if (segment == segments.length)
				segments = Arrays.copyOf(segments, segments.length << 1);
			long segsize = ((size - address + GROW_SIZE - 1) / GROW_SIZE) * GROW_SIZE;
			segments[segment] = channel.map(MapMode.READ_WRITE, address, segsize);
			capacity = address + segsize;
			segment++;
			address += SEGMENT_SIZE;
		}
	}

	@Override
	public void close() throws IOException {
		if (channel != null)
			channel.close();
		channel = null;
	}

	@Override
	void grow(long requiredCapacity) {
		try {
			if (requiredCapacity < capacity)
				return;
			long newCapacity = Math.max(requiredCapacity, capacity + GROW_SIZE);
			int i = 0;
			while (capacity < newCapacity) {
				long address = i << SEGMENT_BITS;
				if (address > newCapacity)
					break;
				if (i >= segments.length)
					segments = Arrays.copyOf(segments, segments.length * 2);
				ByteBuffer segment = segments[i];
				int segcap = segment == null ? 0 : segment.capacity();
				int newsegcap = (int)Math.min(newCapacity - address, SEGMENT_SIZE);
				if (newsegcap > segcap)
					segment = segments[i] = channel.map(MapMode.READ_WRITE, address, newsegcap);
				i++;
			}
			capacity = newCapacity;
		}
		catch (IOException e) {
			throw new IOError(e);
		}
	}
}
