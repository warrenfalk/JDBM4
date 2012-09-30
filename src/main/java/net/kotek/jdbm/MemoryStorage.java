package net.kotek.jdbm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * In-memory-based implementation of Storage interface
 * @author Warren Falk <warren@warrenfalk.com>
 *
 */
public class MemoryStorage extends SegmentedStorage {
	public MemoryStorage() {
	    segments[0] = ByteBuffer.allocate(1<<16);
	    capacity = segments[0].capacity();
	}

	@Override
	public void close() throws IOException {
	}
	
	@Override
	void grow(long requiredCapacity) {
		long newCapacity = Math.max(requiredCapacity, capacity * 2);
		for (int i = 0; (i << SEGMENT_BITS) <= newCapacity; i++) {
			if (i >= segments.length)
				segments = Arrays.copyOf(segments, segments.length * 2);
			ByteBuffer segment = segments[i];
			int segcap = segment == null ? 0 : segment.capacity();
			int segaddress = i << SEGMENT_BITS;
			if (segaddress > newCapacity)
				break;
			int segnewcap = (int)Math.min(SEGMENT_SIZE, newCapacity - segaddress);
			if (segnewcap > segcap) {
				ByteBuffer newseg = ByteBuffer.allocate(segnewcap);
				segment.rewind();
				newseg.put(segment);
				segment = segments[i] = newseg;
			}
		}
		capacity = newCapacity;
	}
}
