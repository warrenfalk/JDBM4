package net.kotek.jdbm;

import java.io.DataInput;
import java.io.IOException;

/**
 * Wraps Storage interface with DataInput interface
 */
public class StorageDataInput implements DataInput {
    Storage storage;
    long address;

    public StorageDataInput(final Storage dataStorage, final long dataPos) {
        this.storage = dataStorage;
        this.address = dataPos;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
    	storage.getBytes(address, b, off, len);
    	address += len;
    }

    @Override
    public int skipBytes(final int n) throws IOException {
        address += n;
        return n;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return readByte() != 0;
    }

    @Override
    public byte readByte() throws IOException {
        return storage.getByte(address++);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xff;
    }

    @Override
    public short readShort() throws IOException {
        short value = storage.getShort(address);
        address += 2;
        return value;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return readShort() & 0xFFFF;
    }

    @Override
    public char readChar() throws IOException {
    	// TODO: read UTF16 here or even UTF8
        return (char)readInt();
    }

    @Override
    public int readInt() throws IOException {
        int value = storage.getInt(address);
        address += 4;
        return value;
    }

    @Override
    public long readLong() throws IOException {
        long value = storage.getLong(address);
        address += 8;
        return value;
    }

    @Override
    public float readFloat() throws IOException {
        float value = storage.getFloat(address);
        address += 4;
        return value;
    }

    @Override
    public double readDouble() throws IOException {
        double value = storage.getDouble(address);
        address += 8;
        return value;
    }

    @Override
    public String readLine() throws IOException {
        return readUTF();
    }

    @Override
    public String readUTF() throws IOException {
        return SerializerBase.deserializeString(this);
    }
}
