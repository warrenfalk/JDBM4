package net.kotek.jdbm;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class JdbmUtilTest  {


    @Test public void testPackInt() throws Exception {

        DataOutput2 out = new DataOutput2();
        ByteBufferDataInput in = new ByteBufferDataInput(ByteBuffer.wrap(out.buf,0, out.pos),0);
        for(int i = 0;i>-1; i = i + 1 + i/1111){  //overflow is expected
            out.pos = 0;

            JdbmUtil.packInt(out, i);
            in.pos = 0;
            in.buf.clear();

            int i2 = JdbmUtil.unpackInt(in);

            Assert.assertEquals(i, i2);

        }

    }

    @Test public void testPackLong() throws Exception {

        DataOutput2 out = new DataOutput2();
        ByteBufferDataInput in = new ByteBufferDataInput(ByteBuffer.wrap(out.buf,0, out.pos),0);
        for(long i = 0;i>-1L  ; i=i+1 + i/111){  //overflow is expected
            out.pos = 0;

            JdbmUtil.packLong(out, i);
            in.pos = 0;
            in.buf.clear();

            long i2 = JdbmUtil.unpackLong(in);
            Assert.assertEquals(i, i2);

        }
    }

    @Test public void testArrayPut(){
        assertEquals(asList(1,2,3,4,5), asList(JdbmUtil.arrayPut(new Integer[]{1,2,4,5}, 2,3)));
        assertEquals(asList(1,2,3,4,5), asList(JdbmUtil.arrayPut(new Integer[]{2,3,4,5}, 0,1)));
        assertEquals(asList(1,2,3,4,5), asList(JdbmUtil.arrayPut(new Integer[]{1,2,3,4}, 4,5)));
    }




}
