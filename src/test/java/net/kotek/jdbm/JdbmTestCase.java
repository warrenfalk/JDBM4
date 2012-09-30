package net.kotek.jdbm;


import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * JUnit test case which provides JDBM specific staff
 */
abstract public class JdbmTestCase {


    String fileName;

    File testDir;

    RecordStore recman;

     @Before
     public void setUp() throws Exception {
        testDir = new File(new File(System.getProperty("java.io.tmpdir")), "testdb");
        testDir.mkdirs();
        fileName = testDir.getPath()+"test"+Math.random();

        recman = openRecordManager();
    }

    protected RecordStore openRecordManager() {
        try {
			return new RecordStore(new FileStorage(new File(fileName + ".d")), new FileStorage(new File(fileName + ".i")));
		} catch (IOException e) {
			throw new IOError(e);
		}
    }

    @After
    public void tearDown() throws Exception {
        recman.close();
        for(File f:testDir.listFiles()){
            if(f!=null && !f.delete())f.deleteOnExit();
        }
    }


    void reopenStore() {
        recman.close();
        recman = openRecordManager();
    }


    ByteBufferDataInput swap(DataOutput2 d){
        byte[] b = d.copyBytes();
        return new ByteBufferDataInput(ByteBuffer.wrap(b),0);
    }


    int countIndexRecords() throws IOException{
        int ret = 0;
        final long indexFileSize = ((SegmentedStorage)recman.indexStorage).segments[0].getLong(RecordStore.RECID_CURRENT_INDEX_FILE_SIZE*8);
        for(int pos = RecordStore.INDEX_OFFSET_START * 8;
            pos<indexFileSize;
            pos+=8){
            if(0!=recman.indexValGet(pos / 8)){
                ret++;
            }
        }
        return ret;
    }

    long getIndexRecord(long recid) throws IOException{
        return recman.indexValGet(recid);
    }

    List<Long> getLongStack(long recid) throws IOException{
        ArrayList<Long> ret =new ArrayList<Long>();

        long pagePhysid = recman.indexValGet(recid) & RecordStore.PHYS_OFFSET_MASK;

        ByteBuffer dataBuf = ((SegmentedStorage)recman.dataStorage).segments[((int) (pagePhysid / SegmentedStorage.SEGMENT_SIZE))];

        while(pagePhysid!=0){
            final byte numberOfRecordsInPage = dataBuf.get((int) (pagePhysid% SegmentedStorage.SEGMENT_SIZE));

            for(int rec = numberOfRecordsInPage; rec>0;rec--){
                final long l = dataBuf.getLong((int) (pagePhysid% SegmentedStorage.SEGMENT_SIZE+ rec*8));
                ret.add(l);
            }

            //read location of previous page
            pagePhysid = dataBuf.getLong((int)(pagePhysid% SegmentedStorage.SEGMENT_SIZE)) & RecordStore.PHYS_OFFSET_MASK;
        }


        return ret;
    }

    int readUnsignedShort(ByteBuffer buf, long pos) throws IOException {
        return (( (buf.get((int) pos) & 0xff) << 8) |
                ( (buf.get((int) (pos+1)) & 0xff)));
    }


    final List<Long> arrayList(long... vals){
        ArrayList<Long> ret = new ArrayList<Long>();
        for(Long l:vals){
            ret.add(l);
        }
        return ret;
    }

    final Map<Long, Integer> getDataContent(){
        Map<Long,Integer> ret = new TreeMap<Long, Integer>();
        final long indexFileSize = ((SegmentedStorage)recman.indexStorage).segments[0].getLong(RecordStore.RECID_CURRENT_INDEX_FILE_SIZE*8);
        for(long recid = RecordStore.INDEX_OFFSET_START ;
            recid*8<indexFileSize;
            recid++){
            Integer val = recman.recordGet(recid, Serializer.HASH_DESERIALIZER);
            ret.put(recid, val);
        }
        return ret;
    }


}
