package sample

import com.google.inject.AbstractModule
import com.google.inject.Provides
import com.google.inject.Scopes
import com.google.inject.Singleton
import io.netty.buffer.ByteBufAllocator
import jamesl.ratpack.memcache.Memcache
import ratpack.exec.ExecController

import java.time.Duration

/**
 * @author jamesl
 */
class AppModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(Random).in(Scopes.SINGLETON)
    }

    @Provides
    @Singleton
    Memcache memcache(ByteBufAllocator allocator, ExecController execController) {
        def ms = Duration.ofMillis(200)
        def remoteHost = new InetSocketAddress("memcache", 11211)

        Memcache.of { spec ->
            spec.allocator allocator
            spec.eventGroupLoop execController.eventLoopGroup
            spec.connectTimeout ms
            spec.readTimeout ms
            spec.routing { key -> remoteHost }
        }
    }
}
