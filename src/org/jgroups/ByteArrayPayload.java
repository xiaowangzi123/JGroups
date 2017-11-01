package org.jgroups;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;

/**
 * {@link Payload} which wraps a byte[] array with an offset and a length
 * @author Bela Ban
 * @since  5.0.0
 */
public class ByteArrayPayload implements Payload {
    protected byte[] buf;
    protected int    offset, length;

    public ByteArrayPayload() {
    }

    public ByteArrayPayload(byte[] buf) {
        this(buf, 0, buf.length);
    }

    public ByteArrayPayload(byte[] buf, int offset, int length) {
        set(buf, offset, length);
    }

    public byte        getType()     {return Payload.BYTE_ARRAY;}
    public byte[]      getBuf()      {return buf;}
    public int         getOffset()   {return offset;}
    public int         getLength()   {return length;}
    public InputStream getInput()    {return new ByteArrayInputStream(buf, offset, length);}
    public boolean     hasArray()    {return true;}
    public int         arrayOffset() {return offset;}
    public byte[]      array()       {return buf;}

    /** Returns a ref to the same buffer if offset is 0 and the length is the same as the underlying byte[] array,
     *  otherwise a copy of the subrange is made
     */
    public ByteArrayPayload copy() {
        if(offset == 0 && length == buf.length)
            return new ByteArrayPayload(buf, offset, length);
        byte[] tmp=new byte[length];
        System.arraycopy(buf, offset, tmp, 0, length);
        return new ByteArrayPayload(tmp, 0, tmp.length);
    }

    public int size() {
        return length;
    }

    public int serializedSize() {
        return Global.INT_SIZE + length;
    }

    /** Writes the buffer to the output stream. No need to check for a null buf, as Message will already take care of
     * that and not invoke this method if the payload is null */
    public void writeTo(DataOutput out) throws Exception {
        out.writeInt(length);
        if(length > 0)
            out.write(buf, offset, length);
    }

    /** Populates the buffer from the input stream. The stream will contain length and buf (Message took care of that) */
    public void readFrom(DataInput in) throws Exception {
        int len=in.readInt();
        if(len > 0) {
            buf=new byte[len];
            in.readFully(buf, 0, len);
            length=len;
        }
    }

    public String toString() {
        return String.format("%s [off=%d len=%d]", getClass().getSimpleName(), offset, length);
    }

    protected void set(byte[] b, int off, int len) {
        if(off < 0 || off >= b.length || off+len > b.length)
            throw new IllegalArgumentException(String.format("illegal offset (%d) or length (%d)", off, len));
        this.buf=b;
        this.offset=off;
        this.length=len;
    }
}
