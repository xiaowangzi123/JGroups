package org.jgroups;

import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Contains multiple payloads; payloads can be removed and added dynamically. But once, this payload is set in a
 * message, it should not be changed anymore.<br/>
 * Since this class is not synchronized, concurrent modifications need external synchronization.
 * @author Bela Ban
 * @since  5.0.0
 */
public class CompositePayload implements Payload, Iterable<Payload> {
    protected Payload[]      payloads;
    protected int            index; // points to the next position at which to add a payload
    protected PayloadFactory factory; // used to create payloads (if set)
    protected static final String NO_ARRAY_MSG=String.format("%s has no backing byte[] array", CompositePayload.class.getSimpleName());

    public CompositePayload() {
    }

    public CompositePayload(int capacity) {
        payloads=new Payload[capacity];
    }

    public CompositePayload(Payload ... payloads) {
        if(payloads != null && payloads.length > 0) {
            for(int i=0; i < payloads.length; i++) {
                if(payloads[i] == null)
                    throw new IllegalArgumentException("all payloads of a payload array must be non-null");
            }
            this.payloads=payloads;
            this.index=payloads.length;
        }
        else
            this.payloads=new Payload[2];
    }


    public byte             getType()                    {return Payload.COMPOSITE;}
    public int              numPayloads()                {return index;}
    public CompositePayload setFactory(PayloadFactory f) {this.factory=f; return this;}
    public boolean          hasArray()                   {return false;}
    public int              arrayOffset()                {throw new UnsupportedOperationException(NO_ARRAY_MSG);}
    public byte[]           array()                      {throw new UnsupportedOperationException(NO_ARRAY_MSG);}
    public InputStream      getInput()                   {return new SequenceInputStream(new PayloadEnumeration());}

    // public Enumeration<InputStream> getEnumerator() {
    //    return new PayloadEnumeration();
    //}

    /** Adds the payload at the end of the array. Increases the array if needed */
    public CompositePayload add(Payload pl) {
        Objects.requireNonNull(pl);
        ensureCapacity(index);
        payloads[index++]=pl;
        return this;
    }

    /** Adds the payload at the head of the array. Increases the array if needed and shifts
     * payloads behind the new payload by one */
    public CompositePayload addAtHead(Payload pl) {
        Objects.requireNonNull(pl);
        ensureCapacity(index);
        System.arraycopy(payloads, 0, payloads, 1, index);
        payloads[0]=pl;
        index++;
        return this;
    }

    /** Removes a payload at the end */
    public Payload remove() {
        Payload retval=null;
        if(index == 0 || payloads == null)
            return null;

        retval=payloads[--index];
        payloads[index]=null;
        return retval;
    }

    /** Removes a payload at the head */
    public Payload removeAtHead() {
        Payload retval=null;
        if(index == 0 || payloads == null)
            return null;
        retval=payloads[0];
        System.arraycopy(payloads, 1, payloads, 0, index-1);
        payloads[--index]=null;
        return retval;
    }

    public Payload get(int index) {
        return payloads[index];
    }

    /** Returns a copy by calling {@link org.jgroups.Payload#copy()} on all elements of this CompositePayload */
    public Payload copy() {
        Payload[] copy=payloads != null? new Payload[payloads.length] : null;
        if(copy != null) {
            for(int i=0; i < payloads.length; i++) {
                if(payloads[i] != null)
                    copy[i]=payloads[i].copy();
            }
        }
        CompositePayload comp=new CompositePayload(2);
        comp.payloads=copy;
        comp.index=index;
        return comp;
    }

    public Iterator<Payload> iterator() {
        return new PayloadIterator(index);
    }

    public Stream<Payload> stream() {
        Spliterator<Payload> sp=Spliterators.spliterator(iterator(), numPayloads(), 0);
        return StreamSupport.stream(sp, false);
    }

    public int size() {
        int total=0;
        for(int i=0; i < index && payloads != null; i++)
            total+=payloads[i].size();
        return total;
    }

    public int serializedSize() {
        int size=Global.SHORT_SIZE;
        for(int i=0; i < index && payloads != null; i++)
            size+=payloads[i].serializedSize() + Global.BYTE_SIZE;
        return size;
    }

    public void writeTo(DataOutput out) throws Exception {
        out.writeShort(index);
        for(int i=0; i < index && payloads != null; i++) {
            Payload pl=payloads[i];
            out.writeByte(pl.getType());
            pl.writeTo(out);
        }
    }

    public void readFrom(DataInput in) throws Exception {
        index=in.readShort();
        payloads=new Payload[Math.max(index,2)];
        for(int i=0; i < index; i++) {
            byte type=in.readByte();
            payloads[i]=Payload.create(type, factory);
            payloads[i].readFrom(in);
        }
    }

    public String toString() {
        StringBuilder sb=new StringBuilder(String.format("%s [size=%d, cap=%d",
                                                         getClass().getSimpleName(), index,
                                                         payloads != null? payloads.length : 0));
        if(index > 0 && payloads != null)
            sb.append(String.format(": %s", Util.printListWithDelimiter(payloads, ", ", Util.MAX_LIST_PRINT_SIZE)));
        return sb.append("]").toString();
    }


    protected void ensureCapacity(int size) {
        if(payloads == null)
            payloads=new Payload[size+1];
        else if(size >= payloads.length)
            payloads=Arrays.copyOf(payloads, size+1);
    }



    protected class PayloadEnumeration implements Enumeration<InputStream> {
        protected int curr_idx;

        public boolean hasMoreElements() {
            return payloads != null && curr_idx < index;
        }

        public InputStream nextElement() {
            Payload payload=payloads == null? null : payloads[curr_idx++];
            return payload != null? payload.getInput() : null;
        }
    }

    protected class PayloadIterator implements Iterator<Payload> {
        protected int   curr_idx;
        protected final int saved_index; // index at creation time of the iterator

        public PayloadIterator(int saved_index) {
            this.saved_index=saved_index;
        }

        public boolean hasNext() {
            return payloads != null && curr_idx < index;
        }

        public Payload next() {
            if(curr_idx >= payloads.length)
                throw new NoSuchElementException();
            return payloads == null? null : payloads[curr_idx++];
        }

    }
}
