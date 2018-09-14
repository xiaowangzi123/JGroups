
package org.jgroups;


import org.jgroups.util.ByteArray;
import org.jgroups.util.Headers;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * A {@link Message} with a heap-based ({@link java.nio.ByteBuffer}) as payload.<br/>
 * <br/>
 * Note that the payload of an NioMessage must not be modified after sending it (ie. {@link JChannel#send(Message)};
 * serialization depends on position and limit to be correct.
 *
 * @since  5.0
 * @author Bela Ban
 */
public class NioMessage extends BaseMessage {

    /** The payload */
    protected ByteBuffer buf;


    /**
     * Constructs a message given a destination address
     * @param dest The Address of the receiver. If it is null, then the message is sent to all cluster members.
     *            Otherwise, it is sent to a single member.
     */
    public NioMessage(Address dest) {
        setDest(dest);
        headers=createHeaders(Util.DEFAULT_HEADERS);
    }

   /**
    * Constructs a message given a destination and source address and the payload byte buffer
    * @param dest The Address of the receiver. If it is null, then the message is sent to all cluster members.
    *             Otherwise, it is sent to a single member.
    * @param buf The payload. Note that this buffer must not be modified (e.g. buf[0]='x' is not
    *            allowed) since we don't copy the contents.
    */
    public NioMessage(Address dest, ByteBuffer buf) {
        this(dest);
        this.buf=buf;
    }


   /**
    * Constructs a message given a destination and source address and the payload object
    * @param dest The Address of the receiver. If it is null, then the message is sent to all cluster members.
    *             Otherwise, it is sent to a single member.
    * @param obj The object that will be marshalled into the byte buffer. Has to be serializable (e.g. implementing
    *            Serializable, Externalizable or Streamable, or be a basic type (e.g. Integer, Short etc)).
    */
    public NioMessage(Address dest, Object obj) {
        this(dest);
        setObject(obj);
    }


    public NioMessage() {
        this(true);
    }


    public NioMessage(boolean create_headers) {
        if(create_headers)
            headers=createHeaders(Util.DEFAULT_HEADERS);
    }

    public Supplier<? extends Message> create() {return NioMessage::new;}
    public ByteBuffer getBuffer()               {return buf;}
    public byte       getType()                 {return Message.NIO_MSG;}
    public boolean    hasPayload()              {return buf != null;}
    public boolean    hasArray()                {return buf != null && buf.hasArray();}
    public int        getOffset()               {return hasArray()? buf.arrayOffset() : 0;}
    public int        getLength()               {return buf != null? buf.remaining() : 0;}

    /** Returns the array of the buffer if the ByteBuffer has an array, null otherwise */
    public byte[]     getArray()                {return buf.hasArray()? buf.array() : null;}



    /**
     * Sets the internal buffer to point to a subset of a given buffer.<p/>
     * <em>
     * Note that the byte[] buffer passed as argument must not be modified. Reason: if we retransmit the
     * message, it would still have a ref to the original byte[] buffer passed in as argument, and so we would
     * retransmit a changed byte[] buffer !
     * </em>
     *
     * @param b The reference to a given buffer. If null, we'll reset the buffer to null
     * @param offset The initial position
     * @param length The number of bytes
     */
    public <T extends Message> T setArray(byte[] b, int offset, int length) {
        buf=ByteBuffer.wrap(b, offset, length);
        return (T)this;
    }

    /**
     * Sets the buffer<p/>
     * Note that the byte[] buffer passed as argument must not be modified. Reason: if we retransmit the
     * message, it would still have a ref to the original byte[] buffer passed in as argument, and so we would
     * retransmit a changed byte[] buffer !
     */
    public <T extends Message> T setArray(ByteArray buf) {
        if(buf != null)
            this.buf=ByteBuffer.wrap(buf.getArray(), buf.getOffset(), buf.getLength());
        return (T)this;
    }



    /**
     * Takes an object and uses Java serialization to generate the byte[] buffer which is set in the
     * message. Parameter 'obj' has to be serializable (e.g. implementing Serializable,
     * Externalizable or Streamable, or be a basic type (e.g. Integer, Short etc)).
     */
    public <T extends Message> T setObject(Object obj) {
        if(obj == null) return (T)this;
        if(obj instanceof byte[])
            return setArray((byte[])obj, 0, ((byte[])obj).length);
        if(obj instanceof ByteArray)
            return setArray((ByteArray)obj);
        try {
            byte[] tmp=Util.objectToByteBuffer(obj);
            return setArray(tmp, 0, tmp.length);
        }
        catch(Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }


    public <T extends Object> T getObject() {
        return getObject(null);
    }

    /**
     * Uses custom serialization to create an object from the buffer of the message. Note that this is dangerous when
     * using your own classloader, e.g. inside of an application server ! Most likely, JGroups will use the system
     * classloader to deserialize the buffer into an object, whereas (for example) a web application will want to use
     * the webapp's classloader, resulting in a ClassCastException. The recommended way is for the application to use
     * their own serialization and only pass byte[] buffer to JGroups.<p/>
     * As of 3.5, a classloader can be passed in. It will be used first to find a class, before contacting
     * the other classloaders in the list. If null, the default list of classloaders will be used.
     * @return the object
     */
    public <T extends Object> T getObject(ClassLoader loader) {
        try {
            return hasArray()? Util.objectFromByteBuffer(getArray(), getOffset(), getLength(), loader) : null;
        }
        catch(Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }



   /**
    * Create a copy of the message.<br/>
    * Note that for headers, only the arrays holding references to the headers are copied, not the headers themselves !
    * The consequence is that the headers array of the copy hold the *same* references as the original, so do *not*
    * modify the headers ! If you want to change a header, copy it and call {@link NioMessage#putHeader(short,Header)} again.
    *
    * @param copy_buffer
    * @param copy_headers
    *           Copy the headers
    * @return Message with specified data
    */
    public <T extends Message> T copy(boolean copy_buffer, boolean copy_headers) {
        NioMessage retval=new NioMessage(dest_addr);
        retval.src_addr=src_addr;
        short tmp_flags=this.flags;
        byte tmp_tflags=this.transient_flags;
        retval.flags=tmp_flags;
        retval.transient_flags=tmp_tflags;

        if(copy_buffer && buf != null)
            retval.buf=buf.duplicate();

        //noinspection NonAtomicOperationOnVolatileField
        retval.headers=copy_headers && headers != null? Headers.copy(this.headers) : createHeaders(Util.DEFAULT_HEADERS);
        return (T)retval;
    }




    /* ----------------------------------- Interface Streamable  ------------------------------- */

    public int size() {return super.size() +sizeOfPayload();}


    @Override public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        writePayload(out);
    }


   /**
    * Writes the message to the output stream, but excludes the dest and src addresses unless the
    * src address given as argument is different from the message's src address
    * @param excluded_headers Don't marshal headers that are part of excluded_headers
    */
    @Override public void writeToNoAddrs(Address src, DataOutput out, short... excluded_headers) throws Exception {
        super.writeToNoAddrs(src, out, excluded_headers);
        writePayload(out);
    }


    @Override public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        readPayload(in);
    }

    protected int sizeOfPayload() {
        return Global.INT_SIZE + getLength() + (buf != null? Global.BYTE_SIZE : 0);
    }

    protected void writePayload(DataOutput out) throws Exception {
        out.writeInt(buf != null? getLength() : -1);
        if(buf != null) {
            out.writeBoolean(buf.isDirect());
            if(buf.hasArray()) {
                byte[] buffer=buf.array();
                int offset=buf.arrayOffset()+buf.position(), length=buf.remaining();
                out.write(buffer, offset, length);
            }
            else {
                // We need to duplicate the buffer, or else writing its contents to the output stream would modify
                // position; this would break potential retransmission
                // We still need a transfer buffer as there is no way to transfer contents of a ByteBuffer directly to
                // an output stream; once we have a transport that directly supports ByteBuffers, we can change this
                ByteBuffer copy=buf.duplicate();
                byte[] transfer_buf=new byte[Math.max(copy.remaining()/10, 128)];
                while(copy.remaining() > 0) {
                    int bytes=Math.min(transfer_buf.length, copy.remaining());
                    copy.get(transfer_buf, 0, bytes);
                    out.write(transfer_buf, 0, bytes);
                }
            }
        }
    }

    protected void readPayload(DataInput in) throws Exception {
        int len=in.readInt();
        if(len < 0)
            return;
        boolean is_direct=in.readBoolean();
        byte[] tmp=new byte[len];
        in.readFully(tmp, 0, tmp.length);
        if(is_direct) {
            // todo: replace with factory; so users can provide their own allocation mechanism (e.g. pooling)
            buf=ByteBuffer.allocateDirect(len)
              .put(tmp, 0, tmp.length);
            buf.flip();
        }
        else
            buf=ByteBuffer.wrap(tmp, 0, tmp.length);
    }



}
