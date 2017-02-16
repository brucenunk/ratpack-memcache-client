package sample

import groovy.util.logging.Slf4j
import ratpack.error.ServerErrorHandler
import ratpack.handling.Context

/**
 * @author jamesl
 */
@Slf4j
class AppServerErrorHandler implements ServerErrorHandler {
    @Override
    void error(Context context, Throwable e) throws Exception {
        log.error("x=${e.class.simpleName},e=${e.message}", e)
        context.render "x=${e.class.simpleName},e=${e.message}"
    }
}
