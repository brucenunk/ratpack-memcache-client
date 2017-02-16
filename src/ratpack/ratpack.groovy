import jamesl.ratpack.memcache.Memcache
import sample.AppModule

import java.nio.charset.StandardCharsets
import java.time.Duration

import static ratpack.groovy.Groovy.ratpack

ratpack {
    bindings {
        module(AppModule)
    }

    handlers {
        get { Memcache memcache, Random random ->
            def key = "sample"

            memcache.get(key) { buffer -> buffer.toString(StandardCharsets.UTF_8) }
            .flatMapIf({ it == null }) {
                def ttl = Duration.ofSeconds(random.nextInt(8))
                def s2 = "ms=${ttl.toMillis()}"

                memcache.add(key, ttl) { it.buffer(s2.length()).writeBytes(s2.getBytes(StandardCharsets.UTF_8)) }
                .map { x -> s2 }
            }
            .then { s ->
                render "OK"
            }
        }
    }
}