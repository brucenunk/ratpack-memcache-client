package jamesl.ratpack.memcache

import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.Unpooled
import ratpack.test.exec.ExecHarness
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.time.Duration

/**
 * @author jamesl
 * @since 1.0
 */
class MemcacheSpec extends Specification {
    ExecHarness exec
    Memcache memcache
    MemcacheServer server
    Duration ttl

    def setup() {
        def allocator = ByteBufAllocator.DEFAULT
        exec = ExecHarness.harness()
        ttl = Duration.ofMillis(200)

        def eventLoopGroup = exec.controller.eventLoopGroup
        server = new MemcacheServer(eventLoopGroup)

        def remoteHost = server.start()

        memcache = Memcache.of { spec ->
            spec.allocator allocator
            spec.connectTimeout Duration.ofMillis(200)
            spec.eventGroupLoop eventLoopGroup
            spec.maxConnections 20
            spec.readTimeout Duration.ofSeconds(2)
            spec.routing { key -> remoteHost }
        }
    }

    def cleanup() {
        server.stop()
        exec.close()
    }

    def "add when key not exists"() {
        when:
        def result = exec.yield { e ->
            memcache.add("sample", ttl) { allocator ->
                allocator.buffer().writeBytes("james".bytes)
            }
        }

        then:
        result.success
        result.value
    }

    def "add when key exists"() {
        server.items.sample = Unpooled.wrappedBuffer("jamesl".bytes)

        when:
        def result = exec.yield { e ->
            memcache.add("sample", ttl) { allocator ->
                allocator.buffer().writeBytes("james".bytes)
            }
        }

        then:
        result.success
        !result.value
    }

    def "decrement when key not exists"() {
        def initial = 20L

        when:
        def result = exec.yield { e ->
            memcache.decrement("sample", ttl, initial)
        }

        then:
        result.success
        result.value == initial
    }

    def "decrement when exists"() {
        def existing = 30L
        def initial = 20L

        server.items.sample = Unpooled.wrappedBuffer(String.valueOf(existing).bytes)

        when:
        def result = exec.yield { e ->
            memcache.decrement("sample", ttl, initial)
        }

        then:
        result.success
        result.value == (existing - 1)
    }

    def "exists when not exists"() {
        when:
        def result = exec.yield { e ->
            memcache.exists("sample")
        }

        then:
        result.success
        !result.value
    }

    def "exists when exists"() {
        server.items.sample = Unpooled.wrappedBuffer("jamesl".bytes)

        when:
        def result = exec.yield { e ->
            memcache.exists("sample")
        }

        then:
        result.success
        result.value
    }

    def "get when not exists"() {
        when:
        def result = exec.yield { e ->
            memcache.get("sample") { buffer ->
                Optional.of(buffer.toString(StandardCharsets.UTF_8))
            }
        }

        then:
        result.success
        result.value == null
    }

    def "get when exists"() {
        server.items.sample = Unpooled.wrappedBuffer("jamesl".bytes)

        when:
        def result = exec.yield { e ->
            memcache.get("sample") { buffer ->
                buffer.toString(StandardCharsets.UTF_8)
            }
        }

        then:
        result.success
        result.value == "jamesl"
    }

    def "increment when key not exists"() {
        def initial = 20L

        when:
        def result = exec.yield { e ->
            memcache.increment("sample", ttl, initial)
        }

        then:
        result.success
        result.value == initial
    }

    def "increment when exists"() {
        def existing = 30L
        def initial = 20L

        server.items.sample = Unpooled.wrappedBuffer(String.valueOf(existing).bytes)

        when:
        def result = exec.yield { e ->
            memcache.increment("sample", ttl, initial)
        }

        then:
        result.success
        result.value == (existing + 1)
    }

    def "set"() {
        when:
        exec.execute { e ->
            memcache.set("sample", ttl) { allocator ->
                allocator.buffer().writeBytes("jamesl".bytes)
            }
        }

        def saved = server.items.sample

        then:
        saved
        saved.toString(StandardCharsets.UTF_8) == "jamesl"
    }
}
