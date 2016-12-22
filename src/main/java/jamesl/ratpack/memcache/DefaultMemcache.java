package jamesl.ratpack.memcache;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.*;
import io.netty.handler.codec.memcache.MemcacheMessage;
import io.netty.handler.codec.memcache.binary.*;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Operation;
import ratpack.exec.Promise;
import ratpack.util.internal.ChannelImplDetector;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * @author jamesl
 * @since 1.0
 */
class DefaultMemcache implements Memcache {
    private static final Logger logger = LoggerFactory.getLogger(DefaultMemcache.class);
    private final ChannelPoolMap<SocketAddress, ChannelPool> channelPools;
    private final Duration readTimeout;
    private final Function<String, SocketAddress> routing;

    /**
     * @param spec the memcache client "spec".
     * @return
     */
    static DefaultMemcache of(Consumer<Spec> spec) {
        DefaultMemcacheSpec x = new DefaultMemcacheSpec();
        spec.accept(x);

        logger.info("creating new DefaultMemcache - spec={}", x);
        return new DefaultMemcache(x);
    }

    /**
     * @param x
     */
    private DefaultMemcache(DefaultMemcacheSpec x) {
        this.readTimeout = x.readTimeout;
        this.routing = x.routing;

        this.channelPools = new AbstractChannelPoolMap<SocketAddress, ChannelPool>() {
            @Override
            protected ChannelPool newPool(SocketAddress remoteHost) {
                return new FixedChannelPool(x.bootstrap.remoteAddress(remoteHost), channelPoolHandler(), x.maxConnections);
            }
        };
    }

    @Override
    public Promise<Boolean> add(String key, Duration ttl, Function<ByteBufAllocator, ByteBuf> valueFactory) {
        return execute(remoteHost(key), spec -> {
            spec.extras(allocator -> allocator.buffer(8).writeZero(4).writeInt(asTtl(ttl)));
            spec.key(key);
            spec.op(BinaryMemcacheOpcodes.ADD);
            spec.value(valueFactory);
        }, mapTrueIfOK().mapIfExists(false));
    }

    @Override
    public Promise<Long> decrement(String key, Duration ttl, long initial) {
        return execute(remoteHost(key), spec -> {
            long delta = 1L;
            spec.extras(allocator -> allocator.buffer(20).writeLong(delta).writeLong(initial).writeInt(asTtl(ttl)));
            spec.key(key);
            spec.op(BinaryMemcacheOpcodes.DECREMENT);
        }, mapIfOK(ByteBuf::readLong));
    }

    @Override
    public Promise<Boolean> exists(String key) {
        return execute(remoteHost(key), spec -> {
            spec.key(key);
            spec.op(BinaryMemcacheOpcodes.GET);
        }, mapTrueIfOK().mapIfNotExists(false));
    }

    @Override
    public <T> Promise<T> get(String key, Function<ByteBuf, T> mapper) {
        return execute(remoteHost(key), spec -> {
            spec.key(key);
            spec.op(BinaryMemcacheOpcodes.GET);
        }, mapIfOK(mapper).mapIfNotExists(null));
    }

    @Override
    public Promise<Long> increment(String key, Duration ttl, long initial) {
        return execute(remoteHost(key), spec -> {
            long delta = 1L;
            spec.extras(allocator -> allocator.buffer(20).writeLong(delta).writeLong(initial).writeInt(asTtl(ttl)));
            spec.key(key);
            spec.op(BinaryMemcacheOpcodes.INCREMENT);
        }, mapIfOK(ByteBuf::readLong));
    }

    @Override
    public Operation set(String key, Duration ttl, Function<ByteBufAllocator, ByteBuf> valueFactory) {
        return execute(remoteHost(key), spec -> {
            spec.extras(allocator -> allocator.buffer(8).writeZero(4).writeInt(asTtl(ttl)));
            spec.key(key);
            spec.op(BinaryMemcacheOpcodes.SET);
            spec.value(valueFactory);
        }, mapTrueIfOK()).operation();
    }

    /**
     * Returns {@code ttl} as an {@code int} so that it can be used as a "ttl" for memcache values.
     *
     * @param ttl
     * @return
     */
    private int asTtl(Duration ttl) {
        return (int) (ttl.toMillis() / 1000);
    }

    /**
     * Returns the {@link ChannelPool} for the specfied {@code remoteHost}.
     *
     * @param remoteHost the remote host associated with a {@link ChannelPool}.
     * @return
     */
    private ChannelPool channelPool(SocketAddress remoteHost) {
        return channelPools.get(remoteHost);
    }

    /**
     * Returns a new {@link ChannelPoolHandler}.
     *
     * @return
     */
    private ChannelPoolHandler channelPoolHandler() {
        return new AbstractChannelPoolHandler() {
            @Override
            public void channelCreated(Channel ch) throws Exception {
                ch.pipeline().addLast("codec", new BinaryMemcacheClientCodec());
                ch.pipeline().addLast("aggregator", new BinaryMemcacheObjectAggregator(Integer.MAX_VALUE));
                logger.trace("created channel {}.", ch);
            }

            @Override
            public void channelReleased(Channel ch) throws Exception {
                removeIfExists(ReadTimeoutHandler.class, ch);
                removeIfExists(ResponseHandler.class, ch);
                logger.trace("released channel {}.", ch);
            }

            /**
             * Removes the handler of type {@code clazz} from the {@link io.netty.channel.ChannelPipeline} if one exists.
             *
             * @param clazz
             * @param channel
             */
            private void removeIfExists(Class<? extends ChannelHandler> clazz, Channel channel) {
                ChannelHandler channelHandler = channel.pipeline().get(clazz);
                if (channelHandler != null) {
                    logger.trace("removing {} from channel pipeline.", channelHandler);
                    channel.pipeline().remove(channelHandler);
                }
            }
        };
    }

    /**
     * Connects to {@code remoteHost}.
     *
     * @param remoteHost the remote host to connect to.
     * @return
     */
    private Promise<Channel> connectTo(SocketAddress remoteHost) {
        ChannelPool channelPool = channelPool(remoteHost);

        return Promise.async(downstream -> {
            logger.trace("connecting to {}.", remoteHost);
            channelPool.acquire().addListener(f -> {
                if (f.isSuccess()) {
                    downstream.success((Channel) f.getNow());
                } else {
                    downstream.error(f.cause());
                }
            });
        });
    }

    /**
     * Executes a memcache request.
     *
     * @param remoteHost  the remote host to send the "request" to.
     * @param requestSpec the request specification.
     * @param mapper      maps the "response" to an instance of {@code T}.
     * @param <T>
     * @return
     */
    private <T> Promise<T> execute(SocketAddress remoteHost, Consumer<RequestSpec> requestSpec, ResponseMapper<T> mapper) {
        return connectTo(remoteHost)
                .flatMap(channel ->
                        send(requestSpec, channel)
                                .flatMap(response -> mapper.apply(response).close(() -> release(response)))
                                .close(() -> release(channel)));
    }

    /**
     * Returns a {@link ResponseMapper} that will yield {@code true}} if the memcache response was "OK".
     *
     * @return
     */
    private ResponseMapper<Boolean> mapTrueIfOK() {
        return mapIfOK(response -> true);
    }

    /**
     * Returns a {@link ResponseMapper} that will yield the result from {@code mapper} if the memcache response was "OK".
     *
     * @param mapper a mapper that will provide the result if the memcache response was "OK".
     * @param <T>
     * @return
     */
    private <T> ResponseMapper<T> mapIfOK(Function<ByteBuf, T> mapper) {
        return new ResponseMapper<>(mapper);
    }

    /**
     * Creates a {@link BinaryMemcacheRequest} from {@code spec}.
     *
     * @param spec
     * @param allocator
     * @return
     */
    private BinaryMemcacheRequest request(Consumer<RequestSpec> spec, ByteBufAllocator allocator) {
        return MemcacheRequest.of(spec).toBinaryMemcacheRequest(allocator);
    }

    /**
     * Sends a "request" of {@code spec} to {@code channel}.
     *
     * @param spec    the request specification.
     * @param channel the channel to send the request to.
     * @return
     */
    private Promise<FullBinaryMemcacheResponse> send(Consumer<RequestSpec> spec, Channel channel) {
        return Promise.async(downstream -> {
            BinaryMemcacheRequest request = request(spec, channel.alloc());
            logger.trace("sending {} to {}.", request, channel);

            channel.pipeline().addLast("responseHandler", new ResponseHandler(downstream));
            channel.writeAndFlush(request).addListener(f -> {
                if (f.isSuccess()) {
                    channel.pipeline().addBefore("responseHandler", "readTimeoutHandler", new ReadTimeoutHandler(readTimeout.toMillis(), TimeUnit.MILLISECONDS));
                } else {
                    downstream.error(f.cause());
                }
            });
        });
    }

    /**
     * Releases {@code channel} back to it's associated {@link ChannelPool}.
     *
     * @param channel the channel to release.
     */
    private void release(Channel channel) {
        logger.trace("releasing channel {}.", channel);
        channelPool(channel.remoteAddress()).release(channel);
    }

    /**
     * Releases {@code message}.
     *
     * @param message the message to release.
     */
    private void release(MemcacheMessage message) {
        logger.trace("releasing message {}.", message);
        message.release();
    }

    /**
     * Returns the remote host to forward the request to.
     *
     * @param key the memcache request key.
     * @return
     */
    private SocketAddress remoteHost(String key) {
        return routing.apply(key);
    }

    /**
     *
     */
    private static class DefaultMemcacheSpec implements Memcache.Spec {
        private Bootstrap bootstrap;
        private int maxConnections;
        private Duration readTimeout;
        private Function<String, SocketAddress> routing;

        DefaultMemcacheSpec() {
            this.bootstrap = new Bootstrap().channel(ChannelImplDetector.getSocketChannelImpl());
        }

        @Override
        public Spec allocator(ByteBufAllocator allocator) {
            this.bootstrap = bootstrap.option(ChannelOption.ALLOCATOR, allocator);
            return this;
        }

        @Override
        public Spec connectTimeout(Duration connectTimeout) {
            this.bootstrap = bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.toMillis());
            return this;
        }

        @Override
        public Spec eventGroupLoop(EventLoopGroup eventGroupLoop) {
            this.bootstrap = bootstrap.group(eventGroupLoop);
            return this;
        }

        @Override
        public Spec maxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }

        @Override
        public Spec readTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        @Override
        public Spec routing(Function<String, SocketAddress> routing) {
            this.routing = routing;
            return this;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("DefaultMemcacheSpec{");
            sb.append("bootstrap=").append(bootstrap);
            sb.append(", maxConnections=").append(maxConnections);
            sb.append(", readTimeout=").append(readTimeout);
            sb.append(", routing=").append(routing);
            sb.append('}');
            return sb.toString();
        }
    }
}
