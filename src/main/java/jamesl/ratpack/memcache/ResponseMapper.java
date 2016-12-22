package jamesl.ratpack.memcache;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import ratpack.exec.Promise;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @param <T>
 * @author jamesl
 * @since 1.0
 */
final class ResponseMapper<T> {
    private final List<MappingElement<T>> mappings;

    /**
     * @param mapOK the mapping element responsible for handling "OK" memcache responses.
     */
    ResponseMapper(Function<ByteBuf, T> mapOK) {
        this.mappings = new ArrayList<>();
        append(BinaryMemcacheResponseStatus.SUCCESS, response -> Promise.value(mapOK.apply(response)));
    }

    /**
     * Returns {@code value} if the memcache response was "exists".
     *
     * @param value
     * @return
     */
    ResponseMapper<T> mapIfExists(T value) {
        return append(BinaryMemcacheResponseStatus.KEY_EEXISTS, response -> Promise.value(value));
    }

    /**
     * Returns {@code value} if the memcache response was "not exists".
     *
     * @param value
     * @return
     */
    ResponseMapper<T> mapIfNotExists(T value) {
        return append(BinaryMemcacheResponseStatus.KEY_ENOENT, response -> Promise.value(value));
    }

    /**
     * Applies this mapper to the {@code response}.
     *
     * @param response the binary memcache response.
     * @return
     */
    Promise<T> apply(FullBinaryMemcacheResponse response) {
        for (MappingElement<T> mappingElement : mappings) {
            if (response.status() == mappingElement.status) {
                Function<ByteBuf, Promise<T>> mapper = mappingElement.mapper;
                return mapper.apply(response.content());
            }
        }

        return Promise.error(new MemcacheException(response));
    }

    /**
     * @param status
     * @param mapper
     * @return
     */
    private ResponseMapper<T> append(short status, Function<ByteBuf, Promise<T>> mapper) {
        mappings.add(new MappingElement<>(status, mapper));
        return this;
    }

    /**
     * @param <T>
     */
    static class MappingElement<T> {
        private final short status;
        private final Function<ByteBuf, Promise<T>> mapper;

        MappingElement(short status, Function<ByteBuf, Promise<T>> mapper) {
            this.status = status;
            this.mapper = mapper;
        }
    }
}
