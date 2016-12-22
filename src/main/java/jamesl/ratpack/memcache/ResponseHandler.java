package jamesl.ratpack.memcache;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Downstream;

/**
 * @author jamesl
 * @since 1.0
 */
class ResponseHandler extends SimpleChannelInboundHandler<FullBinaryMemcacheResponse> {
    private static final Logger logger = LoggerFactory.getLogger(ResponseHandler.class);
    private final Downstream<? super FullBinaryMemcacheResponse> downstream;

    ResponseHandler(Downstream<? super FullBinaryMemcacheResponse> downstream) {
        super(false);
        this.downstream = downstream;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullBinaryMemcacheResponse response) throws Exception {
        logger.trace("sending {} downstream.", response);
        downstream.success(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        downstream.error(cause);
    }
}
