package jamesl.ratpack.memcache

import groovy.util.logging.Slf4j
import io.netty.buffer.ByteBufAllocator
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes
import io.netty.handler.timeout.ReadTimeoutException
import ratpack.exec.Promise
import ratpack.exec.util.ParallelBatch
import ratpack.test.exec.ExecHarness
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.TimeoutException

/**
 * @author jamesl
 * @since 1.0
 */
@Slf4j
class MemcacheSpec extends Specification {
    Duration acquireTimeout
    ExecHarness exec
    int maxConnections
    int maxPendingAcquires
    Memcache memcache
    MemcacheServer server
    Duration ttl

    def setup() {
        exec = ExecHarness.harness()
        acquireTimeout = Duration.ofMillis(200)
        maxConnections = 2
        maxPendingAcquires = 3
        ttl = Duration.ofMillis(200)

        def eventLoopGroup = exec.controller.eventLoopGroup
        server = new MemcacheServer(eventLoopGroup)

        def remoteHost = server.start()

        memcache = Memcache.of { spec ->
            spec.allocator ByteBufAllocator.DEFAULT
            spec.channelPool(maxConnections, maxPendingAcquires, acquireTimeout)
            spec.connectTimeout Duration.ofMillis(80)
            spec.eventGroupLoop eventLoopGroup
            spec.readTimeout Duration.ofMillis(400)
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
        server.set("sample", "jamesl")

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

    def "decrement when key not exists and no seed"() {
        when:
        def result = exec.yield { e ->
            memcache.decrement("sample")
        }

        then:
        result.success
        result.value == Optional.empty()
    }

    def "decrement when key not exists and no seed with delta"() {
        when:
        def result = exec.yield { e ->
            memcache.decrement("sample", 2L)
        }

        then:
        result.success
        result.value == Optional.empty()
    }

    def "decrement when exists"() {
        def existing = 30L
        def initial = 20L
        server.set("sample", String.valueOf(existing))

        when:
        def result = exec.yield { e ->
            memcache.decrement("sample", ttl, initial)
        }

        then:
        result.success
        result.value == (existing - 1)
    }

    def "decrement when key exists and no seed"() {
        def existing = 30L
        server.set("sample", String.valueOf(existing))

        when:
        def result = exec.yield { e ->
            memcache.decrement("sample")
        }

        then:
        result.success
        result.value == Optional.of(existing - 1)
    }

    def "decrement when key exists and no seed with delta"() {
        def delta = 2L
        def existing = 30L
        server.set("sample", String.valueOf(existing))

        when:
        def result = exec.yield { e ->
            memcache.decrement("sample", delta)
        }

        then:
        result.success
        result.value == Optional.of(existing - delta)
    }

    def "delete when not exists"() {
        when:
        def result = exec.yield { e ->
            memcache.delete("sample")
        }

        then:
        result.success
        !result.value
    }

    def "delete when exists"() {
        server.set("sample", "jamesl")

        when:
        def result = exec.yield { e ->
            memcache.delete("sample")
        }

        then:
        result.success
        result.value
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
        server.set("sample", "jamesl")

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
                buffer.toString(StandardCharsets.UTF_8)
            }
        }

        then:
        result.success
        result.value == null
    }

    def "get when exists"() {
        server.set("sample", "jamesl")

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

    def "get when mapper raises exception"() {
        server.set("sample", "jamesl")

        when:
        def result = exec.yield { e ->
            memcache.get("sample") { buffer ->
                throw new RuntimeException("mapping error")
            }
        }

        then:
        result.error
        result.throwable.message == "mapping error"
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

    def "increment when key not exists and no seed"() {
        when:
        def result = exec.yield { e ->
            memcache.increment("sample")
        }

        then:
        result.success
        result.value == Optional.empty()
    }

    def "increment when key not exists and no seed with delta"() {
        when:
        def result = exec.yield { e ->
            memcache.increment("sample", 2L)
        }

        then:
        result.success
        result.value == Optional.empty()
    }

    def "increment when exists"() {
        def existing = 30L
        def initial = 20L
        server.set("sample", String.valueOf(existing))

        when:
        def result = exec.yield { e ->
            memcache.increment("sample", ttl, initial)
        }

        then:
        result.success
        result.value == (existing + 1)
    }

    def "increment when exists and no seed"() {
        def existing = 30L
        server.set("sample", String.valueOf(existing))

        when:
        def result = exec.yield { e ->
            memcache.increment("sample")
        }

        then:
        result.success
        result.value == Optional.of(existing + 1)
    }

    def "increment when key exists and no seed with delta"() {
        def delta = 2L
        def existing = 30L
        server.set("sample", String.valueOf(existing))

        when:
        def result = exec.yield { e ->
            memcache.increment("sample", delta)
        }

        then:
        result.success
        result.value == Optional.of(existing + delta)
    }

    def "maybeget when not exists"() {
        when:
        def result = exec.yield { e ->
            memcache.maybeGet("sample") { buffer ->
                buffer.toString(StandardCharsets.UTF_8)
            }
        }

        then:
        result.success
        result.value == Optional.empty()
    }

    def "maybeget when exists"() {
        server.set("sample", "jamesl")

        when:
        def result = exec.yield { e ->
            memcache.maybeGet("sample") { buffer ->
                buffer.toString(StandardCharsets.UTF_8)
            }
        }

        then:
        result.success
        result.value == Optional.of("jamesl")
    }

    def "set"() {
        when:
        exec.execute { e ->
            memcache.set("sample", ttl) { allocator ->
                allocator.buffer().writeBytes("jamesl".bytes)
            }
        }

        def saved = server.get("sample")

        then:
        saved
        saved.toString(StandardCharsets.UTF_8) == "jamesl"
    }

    def "allow pending acquires if channel is available within specified timeout"() {
        server.set("sample", "jamesl")
        setup: "a number of requests that should complete quickly"

        def max = maxConnections + maxPendingAcquires
        def requests = (0..<max).collect {
            memcache.get("sample") { it.toString(StandardCharsets.UTF_8) } as Promise<String>
        }

        when: "execute requests in parallel"
        def result = exec.yield { e ->
            ParallelBatch.of(requests).yieldAll()
        }

        then: "every result should return the correct value"
        result.success
        result.value.every { it.value == "jamesl" }
    }

    def "fail pending acquires if channel is not available within specified timeout"() {
        server.set("sample", "jamesl")
        setup: "a number of requests that sleep for the acquire timeout before returning"

        def max = maxConnections + maxPendingAcquires
        def requests = (0..<max).collect {
            memcache.get("sample") { buffer ->
                log.trace("sleeping for {}ms", acquireTimeout.toMillis())
                Thread.sleep(acquireTimeout.toMillis())

                log.trace("completing mapper")
                buffer.toString(StandardCharsets.UTF_8)
            } as Promise<String>
        }

        when: "execute requests in parallel"
        def result = exec.yield { e ->
            ParallelBatch.of(requests).yieldAll()
        }

        then:
        result.success
        def (successful, failures) = result.value.split { it.success }

        and: "all successful results should return the correct value, all failures should be from acquire timeout"
        successful.every { it.value == "jamesl" }
        failures.every { it.error && it.throwable instanceof TimeoutException }
    }

    def "fails with read timeout if no response"() {
        server.on(BinaryMemcacheOpcodes.GET) { k, x, v, items ->
            // JL don't send a response.
            Optional.empty()
        }
        server.set("sample", "jamesl")

        when:
        def result = exec.yield { e ->
            memcache.get("sample") { buffer ->
                buffer.toString(StandardCharsets.UTF_8)
            }
        }

        then:
        result.error
        result.throwable instanceof ReadTimeoutException
    }
}
