package jamesl.ratpack.memcache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import ratpack.exec.Operation;
import ratpack.exec.Promise;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author jamesl
 * @since 1.0
 */
public interface Memcache {
    /**
     * Returns a new instance of {@link Memcache} according to {@code spec}.
     *
     * @param spec
     * @return
     */
    static Memcache of(Consumer<Spec> spec) {
        return DefaultMemcache.of(spec);
    }

    /**
     * Performs an "add" operation.
     *
     * @param key          the key.
     * @param ttl          the time-to-live for the value.
     * @param valueFactory a "factory" that will yield the value to associate with {@code key}.
     * @return {@code true} if the value was added or {@code false} if a value already exists under the specified {@code key}.
     */
    Promise<Boolean> add(String key, Duration ttl, Function<ByteBufAllocator, ByteBuf> valueFactory);

    /**
     * Performs a "decrement" operation.
     *
     * @param key the key.
     * @return the current value of {@code key}.
     * @since 1.2
     */
    Promise<Optional<Long>> decrement(String key);

    /**
     * Performs a "decrement" operation.
     *
     * @param key   the key.
     * @param delta the value to decrement.
     * @return the current value of {@code key}.
     * @since 1.2
     */
    Promise<Optional<Long>> decrement(String key, long delta);

    /**
     * Performs a "decrement" operation.
     *
     * @param key     the key.
     * @param ttl     the time-to-live for the value.
     * @param initial the initial value to seed if {@code key} is not found.
     * @return the current value of {@code key}.
     */
    Promise<Long> decrement(String key, Duration ttl, long initial);

    /**
     * Performs a "delete" operation.
     *
     * @param key the key.
     * @return
     * @since 1.3
     */
    Promise<Boolean> delete(String key);

    /**
     * Performs an "exists". This is not a native memcache operation but is implemented as a wrapper around "get".
     *
     * @param key
     * @return
     */
    Promise<Boolean> exists(String key);

    /**
     * Performs a "get" operation.
     *
     * @param key    the key.
     * @param mapper a mapper that will convert the {@link ByteBuf} containing the value to an instance of {@code T}.
     * @param <T>
     * @return the current value or {@code null}.
     */
    <T> Promise<T> get(String key, Function<ByteBuf, T> mapper);

    /**
     * Performs a "increment" operation.
     *
     * @param key the key.
     * @return the current value of {@code key}.
     * @since 1.2
     */
    Promise<Optional<Long>> increment(String key);

    /**
     * Performs a "increment" operation.
     *
     * @param key   the key.
     * @param delta the value to decrement.
     * @return the current value of {@code key}.
     * @since 1.2
     */
    Promise<Optional<Long>> increment(String key, long delta);

    /**
     * Performs a "increment" operation.
     *
     * @param key     the key.
     * @param ttl     the time-to-live for the value.
     * @param initial the initial value to seed if {@code key} is not found.
     * @return the current value of {@code key}.
     */
    Promise<Long> increment(String key, Duration ttl, long initial);

    /**
     * Performs a "get" operation but returns {@link Optional#empty()} instead of {@code null} if a value does not exist.
     *
     * @param key    the key.
     * @param mapper a mapper that will convert the {@link ByteBuf} containing the value to an instance of {@code T}.
     * @param <T>
     * @return the current value wrapped in {@link Optional#of(Object)} or {@link Optional#empty()}.
     * @since 1.1
     */
    <T> Promise<Optional<T>> maybeGet(String key, Function<ByteBuf, T> mapper);

    /**
     * Performs a "set" operation.
     *
     * @param key   the key.
     * @param ttl   the time-to-live for the value.
     * @param value a value factory.
     * @return
     */
    Operation set(String key, Duration ttl, Function<ByteBufAllocator, ByteBuf> value);

    /**
     *
     */
    interface Spec {
        /**
         * Specifies the {@code allocator}.
         *
         * @param allocator the allocator to use when allocating {@link ByteBuf}s.
         * @return
         */
        Spec allocator(ByteBufAllocator allocator);

        /**
         * Configures the "channel pool".
         *
         * @param maxConnections     the maximum number of connections/channels.
         * @param maxPendingAcquires the maximum number of "pending" channel acquire requests.
         * @param acquireTimeout     the maximum period of time to wait for a channel.
         * @return
         */
        Spec channelPool(int maxConnections, int maxPendingAcquires, Duration acquireTimeout);

        /**
         * Specifies the max period of time to wait when trying to connect a channel.
         *
         * @param connectTimeout the connect timeout.
         * @return
         */
        Spec connectTimeout(Duration connectTimeout);

        /**
         * Specifies the {@code eventGroupLoop}.
         *
         * @param eventGroupLoop the event group loop
         * @return
         */
        Spec eventGroupLoop(EventLoopGroup eventGroupLoop);

        /**
         * Specifies the {@code readTimeout}.
         *
         * @param readTimeout the read timeout.
         * @return
         */
        Spec readTimeout(Duration readTimeout);

        /**
         * Specifies the request {@code routing}.
         *
         * @param routing
         * @return
         */
        Spec routing(Function<String, SocketAddress> routing);
    }
}
