package org.jgroups.tests;

import org.jgroups.ByteArrayPayload;
import org.jgroups.Global;
import org.jgroups.util.Triple;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * @author Bela Ban
 * @since  5.0.0
 */
@Test(groups=Global.FUNCTIONAL)
public class ByteArrayPayloadTest {

    public void testCreation() {
        for(Triple<byte[],Integer,Integer> triple: Arrays.asList(
          new Triple<byte[],Integer,Integer>(null, 1, 2),
          new Triple<>(new byte[5], -1, 1),
          new Triple<>(new byte[5], 0, 6),
          new Triple<>(new byte[5], 2, 4),
          new Triple<>(new byte[5], 5, 0))
          ) {
            try {
                ByteArrayPayload buf=new ByteArrayPayload(triple.getVal1(), triple.getVal2(), triple.getVal3());
            }
            catch(IllegalArgumentException | NullPointerException ex) {
                System.out.printf("got %s as expected: %s\n", ex.getClass().getSimpleName(), ex.getMessage());
            }
        }
        for(Triple<byte[],Integer,Integer> triple: Arrays.asList(
          new Triple<>(new byte[5], 0, 5),
          new Triple<>(new byte[5], 2, 3),
          new Triple<>(new byte[5], 4, 1))) {
            ByteArrayPayload buf=new ByteArrayPayload(triple.getVal1(), triple.getVal2(), triple.getVal3());
            System.out.printf("buf: %s\n", buf);
            assert buf != null;
        }
    }

    public void testCopy() {
        final byte[] buf="hello world".getBytes();
        final byte[] hello="hello".getBytes(), world="world".getBytes();
        ByteArrayPayload pl=new ByteArrayPayload(buf, 0, buf.length);
        ByteArrayPayload pl2=pl.copy();
        assert Arrays.equals(pl2.getBuf(), pl.getBuf());

        pl=new ByteArrayPayload(buf, 0, 5);
        pl2=pl.copy();
        assert pl2.getLength() == hello.length;
        assert Arrays.equals(hello, pl2.getBuf());

        pl=new ByteArrayPayload(buf, 6, 5);
        pl2=pl.copy();
        assert pl2.getLength() == world.length;
        assert Arrays.equals(world, pl2.getBuf());
    }
}
