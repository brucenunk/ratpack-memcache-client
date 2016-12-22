package jamesl.ratpack.memcache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author jamesl
 * @since 1.0
 */
class MemcacheRequest implements RequestSpec {
    private Function<ByteBufAllocator, ByteBuf> extrasFactory;
    private Function<ByteBufAllocator, ByteBuf> keyFactory;
    private byte op;
    private Function<ByteBufAllocator, ByteBuf> valueFactory;

    /**
     * @param spec the specification of this request.
     * @return
     */
    static MemcacheRequest of(Consumer<RequestSpec> spec) {
        MemcacheRequest request = new MemcacheRequest();
        spec.accept(request);
        return request;
    }

    @Override
    public RequestSpec extras(Function<ByteBufAllocator, ByteBuf> extrasFactory) {
        this.extrasFactory = extrasFactory;
        return this;
    }

    @Override
    public RequestSpec key(String key) {
        this.keyFactory = allocator -> allocator.buffer(key.length()).writeBytes(key.getBytes(StandardCharsets.UTF_8));
        return this;
    }

    @Override
    public RequestSpec op(byte op) {
        this.op = op;
        return this;
    }

    @Override
    public RequestSpec value(Function<ByteBufAllocator, ByteBuf> valueFactory) {
        this.valueFactory = valueFactory;
        return this;
    }

    /**
     * Converts this instance to a {@link FullBinaryMemcacheRequest}.
     *
     * @param allocator
     * @return
     */
    FullBinaryMemcacheRequest toBinaryMemcacheRequest(ByteBufAllocator allocator) {
        ByteBuf k = from(keyFactory, allocator);
        ByteBuf v = from(valueFactory, allocator);
        ByteBuf x = from(extrasFactory, allocator);

        DefaultFullBinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(k, x, v);
        request.setOpcode(op);
        return request;
    }

    /**
     * @param factory
     * @param allocator
     * @return
     */
    private ByteBuf from(Function<ByteBufAllocator, ByteBuf> factory, ByteBufAllocator allocator) {
        if (factory == null) {
            return Unpooled.EMPTY_BUFFER;
        } else {
            return factory.apply(allocator);
        }
    }
}
