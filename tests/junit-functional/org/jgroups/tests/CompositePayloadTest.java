package org.jgroups.tests;

import org.jgroups.ByteArrayPayload;
import org.jgroups.CompositePayload;
import org.jgroups.Global;
import org.jgroups.Payload;
import org.jgroups.util.Buffer;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author Bela Ban
 * @since  5.0.0
 */
@Test(groups=Global.FUNCTIONAL)
public class CompositePayloadTest {
    ByteArrayPayload p1=new ByteArrayPayload(), p2=new ByteArrayPayload(), p3=new ByteArrayPayload();

    public void testCreation() {
        Payload[] array=new Payload[3];
        array[0]=new ByteArrayPayload();
        array[2]=new ByteArrayPayload();

        CompositePayload pl=new CompositePayload(2);
        assert pl.size() == 0;

        try {
            pl=new CompositePayload(array);
            System.out.println("pl = " + pl);
            assert false : "should have thrown an exception";
        }
        catch(IllegalArgumentException ex) {
            System.out.printf("received exception as expected: %s\n", ex);
        }

        array[1]=array[0];
        pl=new CompositePayload(array);
        System.out.println("pl = " + pl);
        assert pl.size() == 3;
    }

    public void testAdd() {
        CompositePayload pl=new CompositePayload(2);
        assert pl.size() == 0;
        pl.add(new ByteArrayPayload());
        assert pl.size() == 1;
        pl.add(new ByteArrayPayload());
        assert pl.size() == 2;
        pl.add(new ByteArrayPayload());
        assert pl.size() == 3;

        pl=new CompositePayload(createPayloadArray(3));
        pl.add(new ByteArrayPayload());
        assert pl.size() == 4;
    }


    public void testAddAtHead() {
        CompositePayload pl=new CompositePayload(2);
        assert pl.size() == 0;
        pl.addAtHead(p1);
        assert pl.size() == 1;
        pl.addAtHead(p2);
        assert pl.size() == 2;
        pl.addAtHead(p3);
        assert pl.size() == 3;

        assert pl.get(0) == p3;
        assert pl.get(1) == p2;
        assert pl.get(2) == p1;

        pl=new CompositePayload(createPayloadArray(3));
        pl.addAtHead(p1);
        assert pl.size() == 4;
        assert pl.get(0) == p1;
    }

    public void testRemove() {
        CompositePayload pl=new CompositePayload(2);
        Payload retval=pl.remove();
        assert retval == null && pl.size() == 0;

        pl.add(p1).add(p2);

        retval=pl.remove();
        assert retval == p2;
        assert pl.size() == 1;

        retval=pl.remove();
        assert retval == p1;
        assert pl.size() == 0;
    }


    public void testRemoveAtHead() {
        CompositePayload pl=new CompositePayload(2);
        Payload retval=pl.removeAtHead();
        assert retval == null && pl.size() == 0;

        pl.add(p1).add(p2);

        retval=pl.removeAtHead();
        assert retval == p1;
        assert pl.size() == 1;

        retval=pl.removeAtHead();
        assert retval == p2;
        assert pl.size() == 0;
    }

    public void testGetInput() throws IOException {
        String hello="hello ", world="world", combined=hello + world;
        byte[] hello_arr=hello.getBytes(), world_arr=world.getBytes();
        CompositePayload pl=new CompositePayload(new ByteArrayPayload(hello_arr), new ByteArrayPayload(world_arr));
        DataInput in=new DataInputStream(pl.getInput());
        byte[] tmp=new byte[combined.length()];

        in.readFully(tmp, 0, tmp.length);
        String s=new String(tmp);
        assert s.equals(combined);
    }

    public void testIterator() {
        CompositePayload pl=new CompositePayload(createPayloadArray(3, 20));
        int total=0, index=0;
        for(Payload p: pl) {
            total++;
            assert pl.get(index++) == p;
        }
        assert total == 3;
        assert pl.stream().count() == 3;
    }

    public void testSerialization() throws Exception {
        CompositePayload pl=new CompositePayload(2);
        _testSerialization(pl);
        pl=new CompositePayload(createPayloadArray(3));
        _testSerialization(pl);
        pl=new CompositePayload(createPayloadArray(3, 512));
        _testSerialization(pl);
    }


    protected void _testSerialization(CompositePayload pl) throws Exception {
        int size=pl.serializedSize();
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
        pl.writeTo(out);
        assert size == out.position();

        Buffer buf2=out.getBuffer();
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf2.getBuf(), buf2.getOffset(), buf2.getLength());
        CompositePayload pl2=pl.getClass().newInstance();
        pl2.readFrom(in);

        assert pl.getType() == pl2.getType();
        assert pl.size() == pl2.size();
        assert pl.serializedSize() == pl2.serializedSize();
    }

    protected void assertEquals(byte[] first, byte[] second, int length) {
        for(int i=0; i < length; i++)
            assert first[i] == second[i];
    }

    protected Payload[] createPayloadArray(int size) {
        Payload[] retval=new Payload[size];
        for(int i=0; i < retval.length; i++)
            retval[i]=new ByteArrayPayload();
        return retval;
    }

    protected Payload[] createPayloadArray(int size, int buflen) {
        Payload[] retval=new Payload[size];
        for(int i=0; i < retval.length; i++)
            retval[i]=new ByteArrayPayload(new byte[buflen]);
        return retval;
    }
}
