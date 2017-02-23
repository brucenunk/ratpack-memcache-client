package jamesl.ratpack.memcache;

import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.ExecResult;
import ratpack.exec.Operation;
import ratpack.exec.Promise;
import ratpack.func.BiAction;
import ratpack.func.BiFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @param <T>
 * @author jamesl
 */
public class LocalBatch<T> {
    private static final Logger _logger = LoggerFactory.getLogger(LocalBatch.class);
    private static final ConcurrentSet<Object> _set = new ConcurrentSet<>();
    private final Iterable<? extends Promise<T>> promises;

    private LocalBatch(Iterable<? extends Promise<T>> promises) {
        this.promises = promises;
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> LocalBatch<T> of(Promise<T>... promises) {
        return new LocalBatch<>(Arrays.asList(promises));
    }

    public Promise<List<? extends T>> yield() {
        List<T> results = new ArrayList<>();
        return Promise.async(d ->
                forEach((i, r) -> results.add(r))
                        .onError(d::error)
                        .then(() -> d.success(results))
        );
    }

    public Promise<List<? extends T>> yield2() {
        List<T> results = new ArrayList<>();
        return forEach((i, r) -> results.add(r)).map(() -> results);
    }

    public Operation forEach(BiAction<? super Integer, ? super T> consumer) {
        return Promise.<Void>async(d ->
                yieldPromise(promises.iterator(), 0, (i, r) -> consumer.execute(i, r.getValue()), (i, r) -> {
                    d.error(r.getThrowable());
                    return false;
                }, () -> d.success(null))
        ).operation();
    }

    private static <T> void yieldPromise(Iterator<? extends Promise<T>> promises, int i, BiAction<Integer, ExecResult<T>> withItem, BiFunction<Integer, ExecResult<T>, Boolean> onError, Runnable onComplete) {
        if (promises.hasNext()) {
            Promise<T> promise = promises.next();

            promise.onYield(() -> _set.add(promise))
                    .result(r -> {
                        _set.remove(promise);
                        if (r.isError()) {
                            _logger.info("isError,_set={}.", _set);
                            if (!onError.apply(i, r)) {
                                return;
                            }
                        } else {
                            withItem.execute(i, r);
                        }
                        yieldPromise(promises, i + 1, withItem, onError, onComplete);
                    });
        } else {
            _logger.info("onComplete,_set={}.", _set);
            onComplete.run();
        }
    }
}
