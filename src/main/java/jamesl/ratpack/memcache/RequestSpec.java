package jamesl.ratpack.memcache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.function.Function;

/**
 * @author jamesl
 * @since 1.0
 */
interface RequestSpec {
    /**
     * @param extrasFactory a factory that provides the "extras" for a binary memcache request.
     * @return
     */
    RequestSpec extras(Function<ByteBufAllocator, ByteBuf> extrasFactory);

    /**
     * @param key the memcache request key.
     * @return
     */
    RequestSpec key(String key);

    /**
     * @param op the binary memcache request "op code".
     * @return
     */
    RequestSpec op(byte op);

    /**
     * @param valueFactory a factory that provides the "value" for a binary memcache request.
     * @return
     */
    RequestSpec value(Function<ByteBufAllocator, ByteBuf> valueFactory);
}
