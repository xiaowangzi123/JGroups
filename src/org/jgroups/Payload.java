package org.jgroups;

import org.jgroups.util.SizeStreamable;

import java.io.InputStream;

/**
 * Interface for the payload in {@link org.jgroups.Message}. A payload carries data which should not be changed
 * (immutability) after setting it in a message.
 * @author Bela Ban
 * @since  5.0.0
 */
public interface Payload extends SizeStreamable {

    byte COMPOSITE=0, PARTIAL=1, BYTE_ARRAY=2, NIO_DIRECT=3, NIO_HEAP=4, OBJECT=5, INT=6, INPUT_STREAM=7;

    static Payload create(byte type, PayloadFactory factory) {
        if(factory != null)
            return factory.create(type);
        switch(type) {
            case COMPOSITE:  return new CompositePayload();
            case BYTE_ARRAY: return new ByteArrayPayload();
            default:         return null;
        }
    }

    byte getType();

    /** Returns a data input stream for reading from this payload */
    InputStream getInput();

    /** Copies the payload. If the payload is a reference to another payload, then it is implementation-dependent
     *  whether the copy is deep or shallow, but the javadoc needs to describe how the copy is done
     */
    Payload copy();


    /*public int acquire() {return 0;}

    public int release() {return 0;}

    public boolean available() {return true;}*/
}
