package jamesl.ratpack.memcache;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Downstream;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.netty.buffer.ByteBufUtil.hexDump;

/**
 * @author jamesl
 * @since 1.0
 */
class ResponseHandler extends SimpleChannelInboundHandler<FullBinaryMemcacheResponse> {
    private static final Logger _logger = LoggerFactory.getLogger(ResponseHandler.class);
    private final AtomicBoolean complete;
    private final Downstream<? super FullBinaryMemcacheResponse> downstream;

    ResponseHandler(AtomicBoolean complete, Downstream<? super FullBinaryMemcacheResponse> downstream) {
        super(false);
        this.complete = complete;
        this.downstream = downstream;
    }

    private boolean canComplete() {
        return complete.compareAndSet(false, true);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        _logger.trace("channel is now inactive - channel={},complete={}.", ctx.channel(), complete);
        if (canComplete()) {
            downstream.error(new ChannelInactiveException(ctx.channel()));
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullBinaryMemcacheResponse response) throws Exception {
        if (canComplete()) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("received memcache response - status={},content={}.", response.status(), hexDump(response.content()));
            }

            downstream.success(response);
        } else {
            _logger.trace("ignoring memcache response, promise complete - status={},content={}.", response.status(), hexDump(response.content()));
            response.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        _logger.trace("closing channel={}", ctx.channel());
        ctx.close();

        if (canComplete()) {
            downstream.error(cause);
        } else {
            _logger.trace("ignoring exception, promise complete", cause);
        }
    }

    private static class ChannelInactiveException extends RuntimeException {
        ChannelInactiveException(Channel channel) {
            super("channel=" + channel + " is now inactive");
        }
    }
}
