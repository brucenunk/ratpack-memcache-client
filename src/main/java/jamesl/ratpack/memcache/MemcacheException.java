package jamesl.ratpack.memcache;

import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;

import java.nio.charset.StandardCharsets;

/**
 * @author jamesl
 * @since 1.0
 */
public class MemcacheException extends RuntimeException {
    MemcacheException(FullBinaryMemcacheResponse response) {
        this(response.content().toString(StandardCharsets.UTF_8));
    }

    MemcacheException(String message) {
        super(message);
    }
}
