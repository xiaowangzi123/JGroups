
package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.PingHeader;
import org.jgroups.protocols.TpHeader;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


@Test(groups=Global.FUNCTIONAL)
public class StreamableTest {

    static final short PING_ID=100;
    static final short UDP_ID=101;

    public void testStreamable() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        byte[] tmp;
        Message m1=new Message(null, new ByteArrayPayload(buf, 0, 4));
        Message m2=new Message(null, new ByteArrayPayload(buf, 4, 3));

        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutputStream out=new DataOutputStream(output);
        m1.writeTo(out);
        out.close();
        tmp=output.toByteArray();
        output.close();

        ByteArrayInputStream input=new ByteArrayInputStream(tmp);
        DataInputStream in=new DataInputStream(input);
        Message m3, m4;

        m3=new Message(false);
        m3.readFrom(in);

        Assert.assertEquals(4, m3.getLength());
        ByteArrayPayload pl=m3.getPayload();
        Assert.assertEquals(4, pl.getLength());
        Assert.assertEquals(0, pl.getOffset());

        output=new ByteArrayOutputStream();
        out=new DataOutputStream(output);
        m2.writeTo(out);
        out.close();
        tmp=output.toByteArray();
        output.close();

        System.out.println("-- serialized buffer is " + tmp.length + " bytes");
        input=new ByteArrayInputStream(tmp);
        in=new DataInputStream(input);

        m4=new Message();
        m4.readFrom(in);

        Assert.assertEquals(3, m4.getLength());
        pl=m4.getPayload();
        Assert.assertEquals(3, pl.getLength());
        Assert.assertEquals(0, pl.getOffset());
    }




    public void testStreamable2() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message msg=new Message(null, new ByteArrayPayload(buf, 0, 4));
        stream(msg);
    }


    public void testStreamable3() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message msg=new Message(null, new ByteArrayPayload(buf, 4, 3));
        stream(msg);
    }


    public static void testNullBuffer() throws Exception {
        Message msg=new Message();
        stream(msg);
    }



    public static void testNonNullBuffer() throws Exception {
        Message msg=new Message(null, new ByteArrayPayload("Hello world".getBytes()));
        stream(msg);
    }



    public static void testNonNullAddress() throws Exception {
        stream(new Message(null, new ByteArrayPayload("Hello world".getBytes())).src(UUID.randomUUID()));
    }


    public static void testHeaders() throws Exception {
        Address dest=UUID.randomUUID();
        Address src=UUID.randomUUID();
        Message msg=new Message(dest, new ByteArrayPayload("Hello world".getBytes())).src(src);
        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ);
        msg.putHeader(PING_ID, hdr);
        TpHeader udp_hdr=new TpHeader("bla");
        msg.putHeader(UDP_ID, udp_hdr);
        stream(msg);
    }




    public static void testMergeView() throws Exception {
        Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B"), c=Util.createRandomAddress("C"),
          d=Util.createRandomAddress("D"), e=Util.createRandomAddress("E"), f=Util.createRandomAddress("F");

        View v1=View.create(a,1,a,b,c);
        View v2=new MergeView(d, 2, Collections.singletonList(d), new ArrayList<>());
        View v3=View.create(e, 3, e,f);
        View v4=new MergeView(e, 4, Collections.singletonList(d), null);
        View v5=new View(e, 5, Collections.singletonList(d));
        List<View> subgroups=Arrays.asList(v1,v2,v3,v4,v5);


        MergeView view_all=new MergeView(a,5,Arrays.asList(a,b,c,d,e,f),subgroups);
        System.out.println("MergeView: " + view_all);
        List<View> sub=view_all.getSubgroups();
        assert sub.get(0) instanceof View;
        assert sub.get(1) instanceof MergeView;
        assert sub.get(2) instanceof View;
        assert sub.get(3) instanceof MergeView;
        assert sub.get(4) instanceof View;

        byte[] buf=Util.streamableToByteBuffer(view_all);
        assert buf != null;
        assert buf.length > 0;

        MergeView merge_view=Util.streamableFromByteBuffer(MergeView.class, buf);
        assert merge_view != null;
        System.out.println("MergeView: " + merge_view);
        for(View v: merge_view.getSubgroups()) {
            assert !(v instanceof MergeView);
        }
    }

    private static void stream(Message msg) throws Exception {
        int length;
        byte[] tmp;
        Message msg2;
        Address src, dest=msg.getDest();
        int num_headers=getNumHeaders(msg);

        length=msg.getLength();
        src=msg.getSrc();

        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutputStream out=new DataOutputStream(output);
        msg.writeTo(out);
        out.close();
        tmp=output.toByteArray();
        output.close();

        System.out.println("-- serialized buffer is " + tmp.length + " bytes");

        ByteArrayInputStream input=new ByteArrayInputStream(tmp);
        DataInputStream in=new DataInputStream(input);

        msg2=new Message();
        msg2.readFrom(in);

        Assert.assertEquals(length, msg2.getLength());
        assert match(dest, msg2.getDest());
        assert match(src, msg2.getSrc());
        Assert.assertEquals(num_headers, getNumHeaders(msg2));
    }

    private static int getNumHeaders(Message msg) {
        return msg.getNumHeaders();
    }


    private static boolean match(Address a1, Address a2) {
        if(a1 == null && a2 == null)
            return true;
        if(a1 != null)
            return a1.equals(a2);
        else
            return a2.equals(a1);
    }

}
