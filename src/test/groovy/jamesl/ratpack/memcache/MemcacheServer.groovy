package jamesl.ratpack.memcache

import groovy.util.logging.Slf4j
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.*
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.memcache.binary.*
import ratpack.util.internal.ChannelImplDetector

import java.nio.charset.StandardCharsets

import static io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes.*
import static io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus.*

/**
 * @author jamesl
 */
@Slf4j
class MemcacheServer {
    EventLoopGroup eventLoopGroup
    Map<Byte, MemcacheRequestHandler> handlers
    Items items
    Channel serverChannel

    MemcacheServer(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup
        this.handlers = [:].withDefault { k, x, v, items -> Optional.empty() }
        this.items = new Items()
    }

    /**
     *
     * @return
     */
    InetSocketAddress start() {
        init()
        bind()
        serverChannel.localAddress() as InetSocketAddress
    }

    private bind() {
        serverChannel = new ServerBootstrap()
                .channel(ChannelImplDetector.serverSocketChannelImpl)
                .group(eventLoopGroup)
                .childHandler(channelInitializer())
                .bind("localhost", 0)
                .sync()
                .channel()
    }

    private channelInitializer() {
        new ChannelInitializer<SocketChannel>() {
            void initChannel(SocketChannel channel) throws Exception {
                channel.pipeline().addLast("codec", new BinaryMemcacheServerCodec())
                channel.pipeline().addLast("aggregator", new BinaryMemcacheObjectAggregator(Integer.MAX_VALUE))
                channel.pipeline().addLast("handler", new SimpleChannelInboundHandler<FullBinaryMemcacheRequest>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext context, FullBinaryMemcacheRequest message) throws Exception {
                        def key = message.key().toString(StandardCharsets.UTF_8)
                        def handler = handlers[message.opcode()]
                        log.trace("op={},key={},handler={}.", message.opcode(), key, handler)

                        handler.accept(key, message.extras(), message.content(), items).ifPresent { response ->
                            log.trace("response={}.", response)
                            context.writeAndFlush(response)
                        }
                    }
                })
            }
        }
    }

    static Optional<DefaultFullBinaryMemcacheResponse> empty(short status) {
        def response = new DefaultFullBinaryMemcacheResponse(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER)
        response.setStatus(status)
        Optional.of(response)
    }

    ByteBuf get(String key) {
        items.get(key)
    }

    private init() {
        on(ADD) { k, x, v, items ->
            short status
            if (items.contains(k)) {
                status = KEY_EEXISTS
            } else {
                items.update(k, v)
                status = SUCCESS
            }

            return empty(status)
        }

        on(DECREMENT) { k, x, v, items ->
            return items.adjust(k, -1, x).map { response(SUCCESS, it) }.orElse(empty(KEY_ENOENT))
        }

        on(DELETE) { k, x, v, items ->
            short status = items.update(k, null) ? SUCCESS : KEY_ENOENT
            return empty(status)
        }

        on(GET) { k, x, v, items ->
            v = items.get(k)

            if (v != null) {
                return response(SUCCESS, Unpooled.copiedBuffer(v))
            } else {
                return empty(KEY_ENOENT)
            }
        }

        on(INCREMENT) { k, x, v, items ->
            return items.adjust(k, 1, x).map { response(SUCCESS, it) }.orElse(empty(KEY_ENOENT))
        }

        on(SET) { k, x, v, items ->
            items.update(k, v)
            return empty(SUCCESS)
        }
    }

    def on(byte op, MemcacheRequestHandler handler) {
        handlers[op] = handler
    }

    static Optional<DefaultFullBinaryMemcacheResponse> response(short status, ByteBuf value) {
        def response = new DefaultFullBinaryMemcacheResponse(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, value)
        response.setStatus(status)
        Optional.of(response)
    }

    /**
     *
     */
    void stop() {
        items.release()
        serverChannel.close().sync()
    }

    /**
     *
     * @param key
     * @param value
     * @return
     */
    def set(String key, String value) {
        items.update(key, Unpooled.buffer(value.length()).writeBytes(value.bytes))
    }

    /**
     *
     */
    interface MemcacheRequestHandler {
        Optional<FullBinaryMemcacheResponse> accept(String key, ByteBuf extras, ByteBuf value, Items items)
    }

    /**
     *
     */
    static class Items {
        Map<String, ByteBuf> map

        Items() {
            map = [:]
        }

        Optional<ByteBuf> adjust(String key, int adjustment, ByteBuf x) {
            def delta = x.readLong() * adjustment
            def initial = x.readLong()
            def ttl = x.readInt()

            if (contains(key)) {
                def v = map.get(key).toString(StandardCharsets.UTF_8)
                return Optional.of(Unpooled.buffer(8).writeLong(Long.parseLong(v) + delta))
            } else if (ttl == -1) {
                return Optional.empty()
            } else {
                return Optional.of(Unpooled.buffer(8).writeLong(initial))
            }
        }

        boolean contains(String key) {
            map.containsKey(key)
        }

        ByteBuf get(String key) {
            map[key]
        }

        def release() {
            map.each { k, v ->
                v.release()
            }

        }

        def update(String key, ByteBuf value) {
            def existing = value ? map.put(key, Unpooled.copiedBuffer(value)) : map.remove(key)

            if (existing) {
                existing.release()
            }
        }
    }
}