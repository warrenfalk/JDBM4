package net.kotek.jdbm;


import java.io.IOError;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RecordStore implements RecordManager {

    Storage dataStorage;
    Storage indexStorage;

    static final long PHYS_OFFSET_MASK = 0x0000FFFFFFFFFFFFL;




    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** File header. First 4 bytes are 'JDBM', last two bytes are store format version */
    static final long HEADER = (long)'J' <<(8*7)  + (long)'D' <<(8*6) + (long)'B' <<(8*5) + (long)'M' <<(8*4) + CC.STORE_FORMAT_VERSION;


    static final int RECID_CURRENT_PHYS_FILE_SIZE = 1;
    static final int RECID_CURRENT_INDEX_FILE_SIZE = 2;

    /** offset in index file which points to FREEINDEX list (free slots in index file) */
    static final int RECID_FREE_INDEX_SLOTS = 3;

    //TODO slots 4 to 18 are currently unused
    static final int RECID_NAMED_RECODS = 4;
    /**
     * This recid is reserved for user usage. You may put whatever you want here
     * It is only used by JDBM during unit tests, not at production
     * */
    static final int RECID_USER_WHOTEVER =19;

    static final int RECID_FREE_PHYS_RECORDS_START = 20;

    static final int NUMBER_OF_PHYS_FREE_SLOT =1000 + 1535;

    static final int MAX_RECORD_SIZE = 65535;




    /** must be smaller then 127 */
    static final byte LONG_STACK_NUM_OF_RECORDS_PER_PAGE = 100;

    static final int LONG_STACK_PAGE_SIZE =  8 + LONG_STACK_NUM_OF_RECORDS_PER_PAGE * 8;

    /** offset in index file from which normal physid starts */
    static final int INDEX_OFFSET_START = RECID_FREE_PHYS_RECORDS_START +NUMBER_OF_PHYS_FREE_SLOT;





    public RecordStore() {
    	this(new MemoryStorage(), new MemoryStorage());
    }

    public RecordStore(Storage indexStorage, Storage dataStorage) {


        try{
            writeLock_lock();
            
            this.indexStorage = indexStorage;
            this.dataStorage = dataStorage;

            if (dataStorage.size() == 0) {
                writeInitValues();
            }
            else {
                //check headers
                if(CC.ASSERT){
                    if(dataStorage.size()<8 || indexStorage.size()<8 ||
                            dataStorage.getLong(0)!=HEADER ||
                            indexStorage.getLong(0)!=HEADER ){
                         throw new IOException("Wrong file header, probably not JDBM store.");
                    }
                }
            }            	

        }catch (IOException e){
            throw new IOError(e);
        }finally {
            writeLock_unlock();
        }

    }

    private void writeInitValues() throws IOException {
        //write headers
        dataStorage.putLong(0, HEADER);
        indexValPut(0L,HEADER);

        //and set current sizes
        indexValPut(RECID_CURRENT_PHYS_FILE_SIZE, 8L);
        indexValPut(RECID_CURRENT_INDEX_FILE_SIZE, INDEX_OFFSET_START * 8);

        forceRecordUpdateOnGivenRecid(RECID_NAMED_RECODS, new byte[]{});
    }


    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);
            if(CC.ASSERT && out.pos>1<<16) throw new InternalError("Record bigger then 64KB");

            try{
                writeLock_lock();
                //update index file
                long recid = freeRecidTake();

                //get physical record
                // first 16 bites is record size, remaining 48 bytes is record offset in phys file
                final long indexValue = out.pos!=0?
                        freePhysRecTake(out.pos):
                        0L;

                indexValPut(recid, indexValue);

                final long dataPos = indexValue & PHYS_OFFSET_MASK;

                dataStorage.putBytes(dataPos, out.buf, 0, out.pos);

                return recid;
            }finally {
                writeLock_unlock();
            }
        }catch(IOException e){
            throw new IOError(e);
        }
    }

    protected long freeRecidTake() throws IOException {
        writeLock_checkLocked();
        long recid = longStackTake(RECID_FREE_INDEX_SLOTS);
        if(recid == 0){
            //could not reuse recid, so create new one
            final long indexSize = indexValGet(RECID_CURRENT_INDEX_FILE_SIZE);
            recid = indexSize/8;
            if(CC.ASSERT && indexSize%8!=0) throw new InternalError();

            indexValPut(RECID_CURRENT_INDEX_FILE_SIZE, indexSize+8);
        }
        return recid;
    }


    protected void freeRecidPut(long recid) throws IOException {
        longStackPut(RECID_FREE_INDEX_SLOTS, recid);
    }


    @Override
    public <A> A  recordGet(long recid, Serializer<A> serializer) {
        try{
            try{
                readLock_lock();

                final long indexValue = indexValGet(recid) ;
                final long dataPos = indexValue & PHYS_OFFSET_MASK;
                final int dataSize = (int) (indexValue>>>48);
                if(dataPos == 0) return null;

                StorageDataInput in = new StorageDataInput(dataStorage, dataPos);
                final A value = serializer.deserialize(in,dataSize);

                if(CC.ASSERT &&  in.address != dataPos + dataSize)
                        throw new InternalError("Data were not fully read, recid:"+recid+", serializer:"+serializer);

                return value;
            }finally{
                readLock_unlock();
            }


        }catch(IOException e){
            throw new IOError(e);
        }
    }


    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer){
       try{
           DataOutput2 out = new DataOutput2();
           serializer.serialize(out,value);

           //TODO special handling for zero size records
           if(CC.ASSERT && out.pos>1<<16) throw new InternalError("Record bigger then 64KB");
           try{
               writeLock_lock();

               //check if size has changed
               final long oldIndexVal = indexValGet(recid);
               if(oldIndexVal >>>48 == out.pos ){
                   //size is the same, so just write new data
                   final long dataPos = oldIndexVal&PHYS_OFFSET_MASK;
                   dataStorage.putBytes(dataPos, out.buf, 0, out.pos);
               }else{
                   //size has changed, so write into new location
                   final long newIndexValue = freePhysRecTake(out.pos);
                   final long dataPos = newIndexValue&PHYS_OFFSET_MASK;
                   dataStorage.putBytes(dataPos, out.buf, 0, out.pos);
                   //update index file with new location
                   indexValPut(recid,newIndexValue);

                   //and set old phys record as free
                   if(oldIndexVal!=0)
                        freePhysRecPut(oldIndexVal);
               }
           }finally {
               writeLock_unlock();
           }
       }catch(IOException e){
           throw new IOError(e);
       }

   }

   @Override
   public void recordDelete(long recid){
        try{
            writeLock_lock();
            final long oldIndexVal = indexValGet(recid);
            indexValPut(recid, 0L);
            freeRecidPut(recid);
            if(oldIndexVal!=0)
                freePhysRecPut(oldIndexVal);
        } catch (IOException e) {
        	throw new IOError(e);
		}finally {
            writeLock_unlock();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Long getNamedRecid(String name) {
        Map<String, Long> recids = (Map<String, Long>) recordGet(RECID_NAMED_RECODS, Serializer.BASIC_SERIALIZER);
        if(recids == null) return null;
        return recids.get(name);
    }

    protected final Object nameRecidLock = new Object();

    @Override
    @SuppressWarnings("unchecked")
   public void setNamedRecid(String name, Long recid) {
        synchronized (nameRecidLock){
            Map<String, Long> recids = (Map<String, Long>) recordGet(RECID_NAMED_RECODS, Serializer.BASIC_SERIALIZER);
            if(recids == null) recids = new HashMap<String, Long>();
            if(recid!=null)
                recids.put(name, recid);
            else
                recids.remove(name);
            recordUpdate(RECID_NAMED_RECODS, recids, Serializer.BASIC_SERIALIZER);
        }
    }


    @Override
    public void close() {
        try{
            writeLock_lock();

            dataStorage.close();
            indexStorage.close();

        }catch(IOException e){
            throw new IOError(e);
        }finally {
            writeLock_unlock();
        }
    }


    long longStackTake(final long listRecid) throws IOException {
        final long listPhysid = indexValGet(listRecid) &PHYS_OFFSET_MASK;
        if(listPhysid == 0)
            return 0; //there is no such list, so just return 0

        writeLock_checkLocked();

        final byte numberOfRecordsInPage = dataStorage.getByte(listPhysid);
        final long ret = dataStorage.getLong (listPhysid+numberOfRecordsInPage*8);

        //was it only record at that page?
        if(numberOfRecordsInPage == 1){
            //yes, delete this page
            final long previousListPhysid =dataStorage.getLong(listPhysid) &PHYS_OFFSET_MASK;
            if(previousListPhysid !=0){
                //update index so it points to previous page
                indexValPut(listRecid, previousListPhysid | (((long) LONG_STACK_PAGE_SIZE) << 48));
            }else{
                //zero out index
                indexValPut(listRecid, 0L);
            }
            //put space used by this page into free list
            freePhysRecPut(listPhysid | (((long)LONG_STACK_PAGE_SIZE)<<48));
        }else{
            //no, it was not last record at this page, so just decrement the counter
            dataStorage.putByte(listPhysid, (byte)(numberOfRecordsInPage-1));
        }
        return ret;

    }

   void longStackPut(final long listRecid, final long offset) throws IOException {
       writeLock_checkLocked();

       //index position was cleared, put into free index list
        final long listPhysid2 = indexValGet(listRecid) &PHYS_OFFSET_MASK;

        if(listPhysid2 == 0){ //empty list?
            //yes empty, create new page and fill it with values
            final long listPhysid = freePhysRecTake(LONG_STACK_PAGE_SIZE) &PHYS_OFFSET_MASK;
            if(CC.ASSERT && listPhysid == 0) throw new InternalError();
            //set previous Free Index List page to zero as this is first page
            dataStorage.putLong(listPhysid, 0L);
            //set number of free records in this page to 1
            dataStorage.putByte(listPhysid, (byte)1);

            //set  record
            dataStorage.putLong(listPhysid + 8, offset);
            //and update index file with new page location
            indexValPut(listRecid, (((long) LONG_STACK_PAGE_SIZE) << 48) | listPhysid);
        }else{

            final byte numberOfRecordsInPage = dataStorage.getByte(listPhysid2);
            if(numberOfRecordsInPage == LONG_STACK_NUM_OF_RECORDS_PER_PAGE){ //is current page full?
                //yes it is full, so we need to allocate new page and write our number there

                final long listPhysid = freePhysRecTake(LONG_STACK_PAGE_SIZE) &PHYS_OFFSET_MASK;
                if(CC.ASSERT && listPhysid == 0) throw new InternalError();
                //set location to previous page
                dataStorage.putLong(listPhysid, listPhysid2);
                //set number of free records in this page to 1
                dataStorage.putByte(listPhysid, (byte)1); // TODO: is this long/byte thing at the same address a packing strategy? if so, let's OR these together instead so we don't have to worry about endianness
                //set free record
                dataStorage.putLong(listPhysid +  8, offset);
                //and update index file with new page location
                indexValPut(listRecid, (((long) LONG_STACK_PAGE_SIZE) << 48) | listPhysid);
            }else{
                //there is space on page, so just write released recid and increase the counter
            	dataStorage.putLong(listPhysid2 + 8 + 8 * numberOfRecordsInPage, offset);
            	dataStorage.putByte(listPhysid2, (byte)(numberOfRecordsInPage + 1));
            }
        }
   }


    final int freePhysRecSize2FreeSlot(final int size){
        if(CC.ASSERT && size>MAX_RECORD_SIZE) throw new IllegalArgumentException("too big record");
        if(CC.ASSERT && size<0) throw new IllegalArgumentException("negative size");

        if(size<1535)
            return size-1;
        else if(size == MAX_RECORD_SIZE)
            return NUMBER_OF_PHYS_FREE_SLOT-1;
        else
            return 1535 -1 + (size-1535)/64;
    }


    final long freePhysRecTake(final int requiredSize) throws IOException{
        writeLock_checkLocked();


        int slot = freePhysRecSize2FreeSlot(requiredSize);
        //check if this slot can contain smaller records,
        if(requiredSize>1 && slot==freePhysRecSize2FreeSlot(requiredSize-1))
            slot ++; //yes, in this case we have to start at next slot with bigger record and divide it

        while(slot< NUMBER_OF_PHYS_FREE_SLOT){

            final long v = longStackTake(RECID_FREE_PHYS_RECORDS_START +slot);
            if(v!=0){
                //we found it, check if we need to split record
                final int foundRecSize = (int) (v>>>48);
                if(foundRecSize!=requiredSize){

                    //yes we need split
                    final long newIndexValue =
                            ((long)(foundRecSize - requiredSize)<<48) | //encode size into new free record
                            (v & PHYS_OFFSET_MASK) +   requiredSize; //and encode new free record phys offset
                    freePhysRecPut(newIndexValue);
                }

                //return offset combined with required size
                return (v & PHYS_OFFSET_MASK) |
                        (((long)requiredSize)<<48);
            }else{
                slot++;
            }
        }

        //No free records found, so just tack this record on the end
        long physFileSize = indexValGet(RECID_CURRENT_PHYS_FILE_SIZE);
        if(CC.ASSERT && physFileSize <=0) throw new InternalError();
        indexValPut(RECID_CURRENT_PHYS_FILE_SIZE, physFileSize + requiredSize);
        
        return ((long)requiredSize << 48) | physFileSize;
    }


    final void freePhysRecPut(final long indexValue) throws IOException{
        if(CC.ASSERT && (indexValue &PHYS_OFFSET_MASK)==0) throw new InternalError("zero indexValue: ");
        final int size =  (int) (indexValue>>>48);

        final long listRecid = RECID_FREE_PHYS_RECORDS_START + freePhysRecSize2FreeSlot(size);
        longStackPut(listRecid, indexValue);
    }

    final long indexValGet(final long recid) throws IOException {
    	long address = recid * 8;
    	return indexStorage.getLong(address);
    }

    final void indexValPut(final long recid, final  long val) throws IOException {
    	long address = recid * 8;
    	indexStorage.putLong(address, val);
    }


    protected void writeLock_lock() {
        lock.writeLock().lock();
    }

    protected void writeLock_unlock() {
        lock.writeLock().unlock();
    }

    protected void writeLock_checkLocked() {
        if(CC.ASSERT && !lock.writeLock().isHeldByCurrentThread()) throw new IllegalAccessError("no write lock");
    }



    protected void readLock_unlock() {
        lock.readLock().unlock();
    }

    protected void readLock_lock() {
        lock.readLock().lock();
    }


    protected void forceRecordUpdateOnGivenRecid(final long recid, final byte[] value) throws IOException {
        try{
            writeLock_lock();
            //check file size
            final long currentIndexFileSize = indexValGet(RECID_CURRENT_INDEX_FILE_SIZE);
            if(recid * 8 >currentIndexFileSize){
                //TODO grow index file with buffers overflow
                long newIndexFileSize = recid*8;
                indexValPut(RECID_CURRENT_INDEX_FILE_SIZE, newIndexFileSize);
            }
            //size has changed, so write into new location
            final long newIndexValue = freePhysRecTake(value.length);
            final long dataPos = newIndexValue&PHYS_OFFSET_MASK;
            dataStorage.putBytes(dataPos, value, 0, value.length);

            long oldIndexValue = indexValGet(recid);
            //update index file with new location
            indexValPut(recid,newIndexValue);

            //and set old phys record as free
            if(oldIndexValue!=0)
                freePhysRecPut(oldIndexValue);
        }finally {
            writeLock_unlock();
        }
    }
}