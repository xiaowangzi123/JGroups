package org.jgroups;

import org.jgroups.util.ByteArrayDataInputStream;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * {@link Payload} which wraps a byte[] array with an offset and a length
 * @author Bela Ban
 * @since  5.0.0
 */
public class ByteArrayPayload extends Payload {
    protected byte[] buf;
    protected int    offset, length;

    public ByteArrayPayload(byte[] buf) {
        this(buf, 0, buf.length);
    }

    public ByteArrayPayload(byte[] buf, int offset, int length) {
        set(buf, offset, length);

    }

    public byte[]    getBuf()    {return buf;}
    public int       getOffset() {return offset;}
    public int       getLength() {return length;}
    public DataInput getInput()  {return new ByteArrayDataInputStream(buf, offset, length);}

    /** Shallow copy: returns a ref to the same buffer (same semantics as in the original Message) */
    public ByteArrayPayload copy() {
        return new ByteArrayPayload(buf, offset, length);
    }

    public int serializedSize() {
        return length;
    }

    /** Writes the buffer to the output stream. No need to check for a null buf, as Message will already take care of
     * that and not invoke this method if the payload is null */
    public void writeTo(DataOutput out) throws Exception {
        out.writeInt(length);
        out.write(buf, offset, length);
    }

    /** Populates the buffer from the input stream. The stream will contain length and buf (Message took care of that) */
    public void readFrom(DataInput in) throws Exception {
        int len=in.readInt();
        buf=new byte[len];
        in.readFully(buf, 0, len);
        length=len;
    }

    public String toString() {
        return String.format("off=%d len=%d", offset, length);
    }

    protected void set(byte[] b, int off, int len) {
        if(off < 0 || off >= b.length || off+len > b.length)
            throw new IllegalArgumentException(String.format("illegal offset (%d) or length (%d)", off, len));
        this.buf=b;
        this.offset=off;
        this.length=len;
    }
}
