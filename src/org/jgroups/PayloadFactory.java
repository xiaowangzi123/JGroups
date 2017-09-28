package org.jgroups;

import java.util.function.Supplier;

/**
 * Creates instances of {@link Payload} and allows for registration of custom payload generators
 * @author Bela Ban
 * @since  5.0.0
 */
public interface PayloadFactory {


    /**
     * Creates a payload based on the given ID
     * @param id The ID
     * @param <T> The type of the payload
     * @return A payload
     */
    <T extends Payload> T create(byte id);

    /**
     * Registers a new creator of payloads
     * @param type The type associated with the new payload. Needs to be the same in all nodes of the same cluster, and
     *             needs to be available (ie., not taken by JGroups or other applications).
     * @param generator The creator of the payload associated with the given type
     */
    void register(byte type, Supplier<? extends Payload> generator);
}
