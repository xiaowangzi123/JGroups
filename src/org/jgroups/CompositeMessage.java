
package org.jgroups;


import org.jgroups.util.ByteArray;
import org.jgroups.util.Headers;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A message with an array of {@link ByteArray} instances. This is useful when multiple byte arrays are to be passed
 * into a message. In versions prior to 5.0, the byte arrays had to be copied into a single, larger, byte array in
 * order to be passed to the message.<br/>
 * When serialized, all byte arrays are written to the output stream. When getting de-serialized, depending on flag
 * {@link #collapse_arrays}, a {@link BytesMessage} with a single array or a {@link CompositeMessage} with multiple
 * arrays is created.
 * <br/>
 * @author Bela Ban
 * @since  5.0
 */
public class CompositeMessage extends BaseMessage {

    /** When true, the message is serialized as a {@link BytesMessage} and the byte arrays are written to the output
     * stream as one large byte array. When false, the message is written as a {@link CompositeMessage} and the
     * byte arrays are written separately (and will thus be read as separate arrays, too).
     */
    protected boolean           collapse_arrays;
    protected ByteArray[]       arrays;
    protected int               index;



    /**
    * Constructs a message given a destination address
    * @param dest The Address of the receiver. If it is null, then the message is sent to the group. Otherwise, it is
    *             sent to a single member.
    */
    public CompositeMessage(Address dest) {
        setDest(dest);
        headers=createHeaders(Util.DEFAULT_HEADERS);
    }

   /**
    * Constructs a message given a destination and source address and the payload byte buffer
    * @param dest The Address of the receiver. If it is null, then the message is sent to the group. Otherwise, it is
    *             sent to a single member.
    * @param buf The payload. Note that this buffer must not be modified (e.g. buf[0]='x' is not
    *           allowed) since we don't copy the contents.
    */
    public CompositeMessage(Address dest, byte[] buf) {
        this(dest, buf, 0, buf != null? buf.length : 0);
    }


   /**
    * Constructs a message. The index and length parameters provide a reference to a byte buffer, rather than a copy,
    * and refer to a subset of the buffer. This is important when we want to avoid copying. When the message is
    * serialized, only the subset is serialized.</p>
    * <em>
    * Note that the byte[] buffer passed as argument must not be modified. Reason: if we retransmit the
    * message, it would still have a ref to the original byte[] buffer passed in as argument, and so we would
    * retransmit a changed byte[] buffer !
    * </em>
    *
    * @param dest The Address of the receiver. If it is null, then the message is sent to the group. Otherwise, it is
    *             sent to a single member.
    * @param buf A reference to a byte buffer
    * @param offset The index into the byte buffer
    * @param length The number of bytes to be used from <tt>buf</tt>. Both index and length are checked
    *           for array index violations and an ArrayIndexOutOfBoundsException will be thrown if invalid
    */
    public CompositeMessage(Address dest, byte[] buf, int offset, int length) {
        this(dest);
        setArray(buf, offset, length);
    }


    public CompositeMessage(Address dest, ByteArray buf) {
        this(dest);
        setArray(buf);
    }


   /**
    * Constructs a message given a destination and source address and the payload object
    * @param dest The Address of the receiver. If it is null, then the message is sent to the group. Otherwise, it is
    *             sent to a single member.
    * @param obj The object that will be marshalled into the byte buffer. Has to be serializable (e.g. implementing
    *            Serializable, Externalizable or Streamable, or be a basic type (e.g. Integer, Short etc)).
    */
    public CompositeMessage(Address dest, Object obj) {
        this(dest);
        setObject(obj);
    }


    public CompositeMessage() {
        this(true);
    }


    public CompositeMessage(boolean create_headers) {
        if(create_headers)
            headers=createHeaders(Util.DEFAULT_HEADERS);
    }

    public Supplier<? extends Message> create() {
        return CompositeMessage::new;
    }

    public byte    getType()                 {return Message.COMPOSITE_MSG;}
    public boolean hasPayload()              {return arrays != null && index > 0;}
    public boolean hasArray()                {return false;}
    public int     getOffset()               {throw new UnsupportedOperationException();}
    public byte[]  getArray()                {throw new UnsupportedOperationException();}
    public int     getLength() {
        int total=0;
        for(int i=0; i < index && arrays != null; i++)
            total+=arrays[i].getLength();
        return total;
    }

    public <T extends Message> T          setArray(byte[] b, int off, int len)  {throw new UnsupportedOperationException();}
    public <T extends Message> T          setArray(ByteArray buf)               {throw new UnsupportedOperationException();}
    public <T extends Message> T          setObject(Object obj)                 {throw new UnsupportedOperationException();}
    public <T extends Object>  T          getObject()                           {throw new UnsupportedOperationException();}
    public boolean                        collapseArrays()                      {return collapse_arrays;}
    public <T extends CompositeMessage> T collapseArrays(boolean flag)          {this.collapse_arrays=flag; return (T)this;}


    public CompositeMessage add(byte[] array, int offset, int length) {
        return add(new ByteArray(array, offset, length));
    }

    /** Adds the byte array at the end of the array. Increases the array if needed */
    public CompositeMessage add(ByteArray b) {
        ensureCapacity(index);
        arrays[index++]=Objects.requireNonNull(b);
        return this;
    }

    public CompositeMessage addAtHead(byte[] array, int offset, int length) {
        return addAtHead(new ByteArray(array, offset, length));
    }

    /** Adds the byte array at the head of the array. Increases the array if needed and shifts
     * ByteArrays behind the new ByteArray by one */
    public CompositeMessage addAtHead(ByteArray b) {
        Objects.requireNonNull(b);
        ensureCapacity(index);
        System.arraycopy(arrays, 0, arrays, 1, index);
        arrays[0]=b;
        index++;
        return this;
    }

    /** Removes a ByteArray at the end */
    public ByteArray remove() {
        ByteArray retval=null;
        if(index == 0 || arrays == null)
            return null;
        retval=arrays[--index];
        arrays[index]=null;
        return retval;
    }

    /** Removes a payload at the head */
    public ByteArray removeAtHead() {
        if(index == 0 || arrays == null)
            return null;
        ByteArray retval=arrays[0];
        System.arraycopy(arrays, 1, arrays, 0, index-1);
        arrays[--index]=null;
        return retval;
    }

    public ByteArray get(int index) {
        return arrays[index];
    }


    /**
     * Create a copy of the message. If offset and length are used (to refer to another buffer), the
     * copy will contain only the subset offset and length point to, copying the subset into the new
     * copy.<p/>
     * Note that for headers, only the arrays holding references to the headers are copied, not the headers themselves !
     * The consequence is that the headers array of the copy hold the *same* references as the original, so do *not*
     * modify the headers ! If you want to change a header, copy it and call {@link CompositeMessage#putHeader(short,Header)} again.
    *
    * @param copy_buffer
    * @param copy_headers
    *           Copy the headers
    * @return Message with specified data
    */
    public <T extends Message> T copy(boolean copy_buffer, boolean copy_headers) {
        CompositeMessage retval=new CompositeMessage(false);
        retval.dest_addr=dest_addr;
        retval.src_addr=src_addr;
        short tmp_flags=this.flags;
        byte tmp_tflags=this.transient_flags;
        retval.flags=tmp_flags;
        retval.transient_flags=tmp_tflags;
        //noinspection NonAtomicOperationOnVolatileField
        retval.headers=copy_headers && headers != null? Headers.copy(this.headers) : createHeaders(Util.DEFAULT_HEADERS);

        if(copy_buffer && arrays != null) {
            ByteArray[] copy=arrays != null? new ByteArray[arrays.length] : null;
            if(copy != null) {
                for(int i=0; i < arrays.length; i++) {
                    if(arrays[i] != null)
                        copy[i]=arrays[i].copy();
                }
            }
            retval.arrays=copy;
            retval.index=index;
        }
        return (T)retval;
    }




    /* ----------------------------------- Interface Streamable  ------------------------------- */

    public int size() {
        int retval=super.size() + Global.INT_SIZE; // length
        if(arrays != null) {
            for(int i=0; i < index; i++)
                retval+=arrays[index].getLength();
        }
        return retval;
    }


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
        index=in.readInt();
        if(index > 0) {
            arrays=new ByteArray[index]; // a bit of additional space should we add byte arrays


        }
    }

    protected void writePayload(DataOutput out) throws IOException {
        out.writeInt(index);
        if(arrays != null) {
            for(int i=0; i < index; i++)
                out.write(arrays[i].getArray(), arrays[i].getOffset(), arrays[i].getLength());
        }
    }

    /* --------------------------------- End of Interface Streamable ----------------------------- */

    protected void ensureCapacity(int size) {
        if(arrays == null)
            arrays=new ByteArray[size+1];
        else if(size >= arrays.length)
            arrays=Arrays.copyOf(arrays, size+1);
    }


}
