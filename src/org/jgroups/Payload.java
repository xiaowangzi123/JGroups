package org.jgroups;

import org.jgroups.util.SizeStreamable;

import java.io.DataInput;

/**
 * Interface for the payload in {@link org.jgroups.Message}
 * @author Bela Ban
 * @since  5.0.0
 */
public abstract class Payload implements SizeStreamable {

    /** Returns a data input stream for reading from this payload */
    public abstract DataInput getInput();

    /** Copies the payload. If the payload is a reference to another payload, then it is implementation-dependent
     *  whether the copy is deep or shallow, but the javadoc needs to describe how the copy is done
     */
    public abstract Payload copy();


    /*public int acquire() {return 0;}

    public int release() {return 0;}

    public boolean available() {return true;}*/
}
