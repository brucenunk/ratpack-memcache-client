package jamesl.ratpack.memcache;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
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
import ratpack.exec.Execution;
import ratpack.exec.Operation;
import ratpack.exec.Promise;
import ratpack.util.internal.ChannelImplDetector;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * @author jamesl
 * @since 1.0
 */
class DefaultMemcache implements Memcache {
    private static final Logger _logger = LoggerFactory.getLogger(DefaultMemcache.class);
    private final ChannelPoolMap<SocketAddress, ChannelPool> channelPools;
    private final Duration readTimeout;
    private final Function<String, SocketAddress> routing;

    /**
     * @param configuration the memcache client configuration.
     * @return
     */
    static DefaultMemcache of(Consumer<Spec> configuration) {
        DefaultMemcacheSpec spec = new DefaultMemcacheSpec();
        configuration.accept(spec);

        _logger.info("creating new DefaultMemcache.");
        return new DefaultMemcache(spec);
    }

    /**
     * @param spec
     */
    private DefaultMemcache(DefaultMemcacheSpec spec) {
        this.readTimeout = spec.readTimeout;
        this.routing = spec.routing;

        this.channelPools = new AbstractChannelPoolMap<SocketAddress, ChannelPool>() {
            @Override
            protected ChannelPool newPool(SocketAddress remoteHost) {
                return spec.newChannelPool(remoteHost, channelPoolHandler());
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
    public Promise<Optional<Long>> decrement(String key) {
        return decrement(key, 1L);
    }

    @Override
    public Promise<Optional<Long>> decrement(String key, long delta) {
        return execute(remoteHost(key), spec -> {
            int ttl = 0xffffffff;
            spec.extras(allocator -> allocator.buffer(20).writeLong(delta).writeLong(0L).writeInt(ttl));
            spec.key(key);
            spec.op(BinaryMemcacheOpcodes.DECREMENT);
        }, mapOptionalIfOK(ByteBuf::readLong));
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
    public Promise<Boolean> delete(String key) {
        return execute(remoteHost(key), spec -> {
            spec.key(key);
            spec.op(BinaryMemcacheOpcodes.DELETE);
        }, mapTrueIfOK().mapIfNotExists(false));
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
    public Promise<Optional<Long>> increment(String key) {
        return increment(key, 1L);
    }

    @Override
    public Promise<Optional<Long>> increment(String key, long delta) {
        return execute(remoteHost(key), spec -> {
            int ttl = 0xffffffff;
            spec.extras(allocator -> allocator.buffer(20).writeLong(delta).writeLong(0L).writeInt(ttl));
            spec.key(key);
            spec.op(BinaryMemcacheOpcodes.INCREMENT);
        }, mapOptionalIfOK(ByteBuf::readLong));
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
    public <T> Promise<Optional<T>> maybeGet(String key, Function<ByteBuf, T> mapper) {
        return execute(remoteHost(key), spec -> {
            spec.key(key);
            spec.op(BinaryMemcacheOpcodes.GET);
        }, mapOptionalIfOK(mapper));
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
     * Returns the {@link ChannelPool} for the specified {@code remoteHost}.
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
        return new DefaultMemcacheChannelPoolHandler();
    }

    /**
     * Connects to {@code remoteHost}.
     *
     * @param remoteHost the remote host to connect to.
     * @return
     */
    private Promise<Channel> connectTo(SocketAddress remoteHost) {
        return Promise.<Channel>async(downstream -> {
            _logger.trace("connecting to {}.", remoteHost);
            channelPool(remoteHost).acquire()
                    .addListener(f -> {
                        if (f.isSuccess()) {
                            _logger.debug("connected to {}.", remoteHost);
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
        AtomicBoolean channelReleased = new AtomicBoolean(false);
        return connectTo(remoteHost)
                .next(channel -> Execution.current().onComplete(() -> release(channel, channelReleased, true)))
                .flatMap(channel ->
                        send(requestSpec, channel)
                                .flatMap(response -> map(response, mapper).close(() -> release(response)))
                                .close(() -> release(channel, channelReleased, false)));
    }

    /**
     * Dumps {@code request} to logger.
     *
     * @param request
     */
    private void log(FullBinaryMemcacheRequest request) {
        if (_logger.isDebugEnabled()) {
            _logger.debug("sending memcache request - op={},key={},value={},extra={}.", request.opcode(), request.key().toString(StandardCharsets.UTF_8),
                    ByteBufUtil.hexDump(request.content()), ByteBufUtil.hexDump(request.extras()));
        }
    }

    /**
     * Maps {@code response} using {@code mapper}.
     *
     * @param response
     * @param mapper
     * @param <T>
     * @return
     */
    private <T> Promise<T> map(FullBinaryMemcacheResponse response, ResponseMapper<T> mapper) {
        try {
            return mapper.apply(response);
        } catch (Exception e) {
            return Promise.error(e);
        }
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
     * Returns a {@link ResponseMapper} that will yield the result of {@code mapper} within the context
     * of {@link Optional} if the response was "OK". If the response was "not found", {@link Optional#empty()} will be returned.
     *
     * @param mapper a mapper that will provide the result if the memcache response was "OK".
     * @param <T>
     * @return
     */
    private <T> ResponseMapper<Optional<T>> mapOptionalIfOK(Function<ByteBuf, T> mapper) {
        return mapIfOK(response -> Optional.of(mapper.apply(response))).mapIfNotExists(Optional.empty());
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
     * Releases {@code channel} back to it's associated {@link ChannelPool}.
     *
     * @param channel         the channel to release.
     * @param channelReleased
     * @param onComplete
     */
    private void release(Channel channel, AtomicBoolean channelReleased, boolean onComplete) {
        if (channelReleased.compareAndSet(false, true)) {
            _logger.trace("releasing channel {}, onComplete={}.", channel, onComplete);
            removeIfExists(ReadTimeoutHandler.class, channel);
            removeIfExists(ResponseHandler.class, channel);

            SocketAddress remoteHost = channel.remoteAddress();
            channelPool(remoteHost).release(channel)
                    .addListener(f -> {
                        _logger.trace("released channel {}.", channel);
                        if (!f.isSuccess()) {
                            _logger.trace("failed to release " + channel + " back to pool.", f.cause());
                        }
                    });
        }
    }

    /**
     * Releases {@code message}.
     *
     * @param message the message to release.
     */
    private void release(MemcacheMessage message) {
        _logger.trace("releasing message {}.", message);
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
     * Removes the handler of type {@code clazz} from the {@link io.netty.channel.ChannelPipeline} if one exists.
     *
     * @param clazz
     * @param channel
     */
    private void removeIfExists(Class<? extends ChannelHandler> clazz, Channel channel) {
        ChannelHandler channelHandler = channel.pipeline().get(clazz);
        if (channelHandler != null) {
            _logger.trace("removing {} from channel pipeline.", channelHandler);
            channel.pipeline().remove(channelHandler);
        }
    }

    /**
     * Creates a {@link BinaryMemcacheRequest} from {@code spec}.
     *
     * @param spec
     * @param allocator
     * @return
     */
    private FullBinaryMemcacheRequest request(Consumer<RequestSpec> spec, ByteBufAllocator allocator) {
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
            FullBinaryMemcacheRequest request = request(spec, channel.alloc());
            log(request);

            AtomicBoolean complete = new AtomicBoolean(false);
            channel.pipeline().addLast("readTimeoutHandler", new ReadTimeoutHandler(readTimeout.toMillis(), TimeUnit.MILLISECONDS));
            channel.pipeline().addLast("responseHandler", new ResponseHandler(complete, downstream));

            channel.writeAndFlush(request)
                    .addListener(f -> {
                        if (f.isSuccess()) {
                            _logger.trace("request send to {} - waiting for response.", channel);
                        } else {
                            if (!complete.compareAndSet(false, true)) {
                                downstream.error(f.cause());
                            }
                            _logger.warn("failed to send request", f.cause());
                        }
                    });
        });
    }

    /**
     *
     */
    private static class DefaultMemcacheChannelPoolHandler implements ChannelPoolHandler {
        private static final Logger _logger = LoggerFactory.getLogger(DefaultMemcacheChannelPoolHandler.class);
        private AtomicInteger numberOfChannels = new AtomicInteger(0);

        @Override
        public void channelAcquired(Channel ch) throws Exception {
            int n = numberOfChannels.incrementAndGet();
            _logger.trace("channel acquired - numberOfChannels={}, channel={}.", n, ch);
        }

        @Override
        public void channelCreated(Channel ch) throws Exception {
            ch.pipeline().addLast("codec", new BinaryMemcacheClientCodec());
            ch.pipeline().addLast("aggregator", new BinaryMemcacheObjectAggregator(Integer.MAX_VALUE));

            int n = numberOfChannels.incrementAndGet();
            _logger.trace("channel created - numberOfChannels={}, channel={}.", n, ch);
        }

        @Override
        public void channelReleased(Channel ch) throws Exception {
            int n = numberOfChannels.decrementAndGet();
            _logger.trace("channel released - numberOfChannels={}, channel={}.", n, ch);
        }
    }

    /**
     *
     */
    private static class DefaultMemcacheSpec implements Memcache.Spec {
        private Bootstrap bootstrap;
        private BiFunction<SocketAddress, ChannelPoolHandler, ChannelPool> channelPoolFactory;
        private Duration readTimeout;
        private Function<String, SocketAddress> routing;

        DefaultMemcacheSpec() {
            this.bootstrap = new Bootstrap().channel(ChannelImplDetector.getSocketChannelImpl());
            this.channelPoolFactory = (remoteHost, channelPoolHandler) -> new SimpleChannelPool(bootstrap.remoteAddress(remoteHost), channelPoolHandler);
            this.readTimeout = Duration.ofMillis(400);
        }

        @Override
        public Spec allocator(ByteBufAllocator allocator) {
            Objects.requireNonNull(allocator, "allocator");
            this.bootstrap = bootstrap.option(ChannelOption.ALLOCATOR, allocator);
            return this;
        }

        @Override
        public Spec channelPool(int maxConnections, int maxPendingAcquires, Duration acquireTimeout) {
            Objects.requireNonNull(acquireTimeout, "acquireTimeout");
            channelPoolFactory = (remoteHost, channelPoolHandler) -> new FixedChannelPool(bootstrap.remoteAddress(remoteHost), channelPoolHandler, ChannelHealthChecker.ACTIVE,
                    FixedChannelPool.AcquireTimeoutAction.FAIL, acquireTimeout.toMillis(), maxConnections, maxPendingAcquires);

            return this;
        }

        @Override
        public Spec connectTimeout(Duration connectTimeout) {
            Objects.requireNonNull(connectTimeout, "connectTimeout");
            this.bootstrap = bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.toMillis());
            return this;
        }

        @Override
        public Spec eventGroupLoop(EventLoopGroup eventGroupLoop) {
            Objects.requireNonNull(eventGroupLoop, "eventGroupLoop");
            this.bootstrap = bootstrap.group(eventGroupLoop);
            return this;
        }

        @Override
        public Spec readTimeout(Duration readTimeout) {
            Objects.requireNonNull(readTimeout, "readTimeout");
            this.readTimeout = readTimeout;
            return this;
        }

        @Override
        public Spec routing(Function<String, SocketAddress> routing) {
            Objects.requireNonNull(routing, "routing");
            this.routing = routing;
            return this;
        }

        /**
         * @param remoteHost
         * @param channelPoolHandler
         * @return
         */
        ChannelPool newChannelPool(SocketAddress remoteHost, ChannelPoolHandler channelPoolHandler) {
            return channelPoolFactory.apply(remoteHost, channelPoolHandler);
        }
    }
}
