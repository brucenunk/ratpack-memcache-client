package jamesl.ratpack.memcache;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Downstream;

/**
 * @author jamesl
 * @since 1.0
 */
class ResponseHandler extends SimpleChannelInboundHandler<FullBinaryMemcacheResponse> {
    static final AttributeKey<Downstream<? super FullBinaryMemcacheResponse>> DOWNSTREAM = AttributeKey.valueOf(ResponseHandler.class, "downstream");
    private static final Logger logger = LoggerFactory.getLogger(ResponseHandler.class);

    ResponseHandler() {
        super(false);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullBinaryMemcacheResponse response) throws Exception {
        Downstream<? super FullBinaryMemcacheResponse> downstream = ctx.channel().attr(DOWNSTREAM).get();
        logger.trace("sending {} downstream.", response);
        downstream.success(response);
    }
}
