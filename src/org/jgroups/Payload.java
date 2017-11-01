package org.jgroups;

import org.jgroups.util.Buffer;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.SizeStreamable;

import java.io.InputStream;

/**
 * Interface for the payload in {@link org.jgroups.Message}. A payload carries data which should not be changed
 * (immutability) after setting it in a message.
 * @author Bela Ban
 * @since  5.0.0
 */
public interface Payload extends SizeStreamable {

    // The type of the payload. Cannot be an enum, as users can register additional types
    byte BYTE_ARRAY=1, COMPOSITE=2, PARTIAL=3, NIO_DIRECT=4, NIO_HEAP=5, OBJECT=6, INT=7, INPUT_STREAM=8;

    static Payload create(byte type, PayloadFactory factory) {
        if(factory != null)
            return factory.create(type);
        switch(type) {
            case COMPOSITE:  return new CompositePayload();
            case BYTE_ARRAY: return new ByteArrayPayload();
            default:         return null;
        }
    }

    /** Returns the type of the payload */
    byte getType();

    /** Returns the number of bytes in this payload (if feasible). If the payload has no byte[] array,
     * this can be an estimate or {@link #serializedSize()} can be used instead */
    int size();

    /**
     * Returns true if this payload is backed by a byte[] array
     * @return True if this payload is backed by a byte[] array, else false
     */
    boolean hasArray();

    /**
     * Returns the offset at which data starts within the backing byte[] array
     * @return The offset of the data within the byte[] array. Use {@link #hasArray()} first to check if this payload
     *         is backed by a byte[] array
     * @throws  UnsupportedOperationException
     *     If this payload is not backed by a byte[] array
     */
    int arrayOffset();

    /**
     * Returns the underlying byte[] array of this payload. If the payload is not backed by a byte[] array, an exception
     * will be thrown. Use {@link #hasArray()} to check if the payload has a backing byte[] array.
     * @return The underlying byte[] array
     * @throws  UnsupportedOperationException
     *     If this payload is not backed by a byte[] array
     */
    byte[] array();

    /** Returns a data input stream for reading from this payload */
    InputStream getInput();

    /** Copies the payload. If the payload is a reference to another payload, then it is implementation-dependent
     *  whether the copy is deep or shallow, but the javadoc needs to describe how the copy is done
     */
    Payload copy();

    default Buffer serialize() throws Exception {
        int size=serializedSize()+Global.BYTE_SIZE;
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(Math.max(size, 256));
        out.write(getType());
        writeTo(out);
        return new Buffer(out.buffer(), 0, out.position());
    }


    /*public int acquire() {return 0;}

    public int release() {return 0;}

    public boolean available() {return true;}*/
}
