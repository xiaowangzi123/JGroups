package org.jgroups;

import org.jgroups.util.ByteArray;
import org.jgroups.util.Headers;
import org.jgroups.util.Util;

import java.util.function.Supplier;

/**
 * Message without payload; optimized for sending only headers
 * @author Bela Ban
 * @since  5.0
 */
public class EmptyMessage extends BaseMessage {

    public EmptyMessage(Address dest) {
        super(dest);
    }

    public EmptyMessage() {
    }

    public EmptyMessage(boolean create_headers) {
        super(create_headers);
    }

    public byte                        getType() {return Message.EMPTY_MSG;}
    public Supplier<? extends Message> create()  {return EmptyMessage::new;}

    public <T extends Message> T copy(boolean copy_buffer, boolean copy_headers) {
        EmptyMessage retval=new EmptyMessage();
        retval.dest_addr=dest_addr;
        retval.src_addr=src_addr;
        short tmp_flags=this.flags;
        byte tmp_tflags=this.transient_flags;
        retval.flags=tmp_flags;
        retval.transient_flags=tmp_tflags;
        //noinspection NonAtomicOperationOnVolatileField
        retval.headers=copy_headers && headers != null? Headers.copy(this.headers) : createHeaders(Util.DEFAULT_HEADERS);
        return (T)retval;
    }

    public boolean               hasPayload()                         {return false;}
    public boolean               hasArray()                           {return false;}
    public byte[]                getArray()                           {return null;}
    public int                   getOffset()                          {return 0;}
    public int                   getLength()                          {return 0;}
    public <T extends Message> T setArray(byte[] b, int off, int len) {throw new UnsupportedOperationException();}
    public <T extends Message> T setArray(ByteArray buf)              {throw new UnsupportedOperationException();}
    public <T> T                 getObject()                          {throw new UnsupportedOperationException();}
    public <T extends Message> T setObject(Object obj)                {throw new UnsupportedOperationException();}
}
