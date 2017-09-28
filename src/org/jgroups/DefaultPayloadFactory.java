package org.jgroups;

import java.util.Objects;
import java.util.function.Supplier;


/**
 * @author Bela Ban
 * @since  5.0.0
 */
public class DefaultPayloadFactory implements PayloadFactory {
    protected final Supplier<? extends Payload>[] creators=new Supplier[Byte.MAX_VALUE];
    protected static final byte                   MIN_TYPE=32;

    public DefaultPayloadFactory() {
        creators[Payload.COMPOSITE]=CompositePayload::new;
        creators[Payload.BYTE_ARRAY]=ByteArrayPayload::new;

    }

    public <T extends Payload> T create(byte type) {
        Supplier<? extends Payload> creator=creators[type];
        if(creator == null)
            throw new IllegalArgumentException("no creator found for type " + type);
        return (T)creator.get();
    }

    public void register(byte type, Supplier<? extends Payload> generator) {
        Objects.requireNonNull(generator, "the creator must be non-null");
        if(type <= MIN_TYPE)
            throw new IllegalArgumentException(String.format("type (%d) must be > 32", type));
        if(creators[type] != null)
            throw new IllegalArgumentException(String.format("type %d is already taken", type));
        creators[type]=generator;
    }
}
