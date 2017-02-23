import jamesl.ratpack.memcache.LocalBatch
import jamesl.ratpack.memcache.Memcache
import ratpack.exec.util.SerialBatch
import sample.AppModule

import java.nio.charset.StandardCharsets

import static ratpack.groovy.Groovy.ratpack

ratpack {
    bindings {
        module(AppModule)
    }

    handlers {
        get("simple-get") { Memcache memcache, Random random ->
            memcache.get("xx") { buffer -> buffer.toString(StandardCharsets.UTF_8) }
            .then { x ->
                render "OK"
            }
        }

        get("multi-get") { Memcache memcache ->
            def promise = memcache.get("xx") { buffer -> buffer.toString(StandardCharsets.UTF_8) }
            def promise2 = memcache.get("xx2") { buffer -> buffer.toString(StandardCharsets.UTF_8) }

            promise.flatRight { left -> promise2 }
            .then { both ->
                render "OK"
            }
        }

        get("local-batch-get") { Memcache memcache ->
            def promise = memcache.get("xx") { buffer -> buffer.toString(StandardCharsets.UTF_8) }
            def promise2 = memcache.get("xx2") { buffer -> buffer.toString(StandardCharsets.UTF_8) }

            LocalBatch.of(promise, promise2).yield()
            .then { x ->
                render "OK"
            }
        }

        get("local-batch-get2") { Memcache memcache ->
            def promise = memcache.get("xx") { buffer -> buffer.toString(StandardCharsets.UTF_8) }
            def promise2 = memcache.get("xx2") { buffer -> buffer.toString(StandardCharsets.UTF_8) }

            LocalBatch.of(promise, promise2).yield2()
            .then { x ->
                render "OK"
            }
        }

        get("serial-batch-get") { Memcache memcache ->
            def promise = memcache.get("xx") { buffer -> buffer.toString(StandardCharsets.UTF_8) }
            def promise2 = memcache.get("xx2") { buffer -> buffer.toString(StandardCharsets.UTF_8) }

            SerialBatch.of(promise, promise2).yield()
            .then { x ->
                render "OK"
            }
        }
    }
}