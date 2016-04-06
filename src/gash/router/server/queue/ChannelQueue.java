package gash.router.server.queue;

import com.google.protobuf.GeneratedMessage;
import gash.router.container.RoutingConf;
import gash.router.server.ServerState;
import io.netty.channel.Channel;

/**
 * The channel queue is the interaction we expect between the resources and
 * clients. How this is implemented is encapsulated in the implementation
 *
 * @author gash
 *
 */
public interface ChannelQueue {

    /**
     * The server is shutting down, terminate all work.
     *
     * @param hard
     */
    public abstract void shutdown(boolean hard);

    /**
     * add a request to a Command queue
     *
     * @param req
     *            The request to process
     * @param channel
     *            The channel that the request was made on. For some queue
     *            implementations, this is redundant.
     */
    public abstract void enqueueRequest(GeneratedMessage req, Channel channel);

    /**
     * add a reply to a request to the outboundWork queue
     *
     * @param reply
     *            The reply to a request
     * @param channel
     *            The channel that the request was made on. For some queue
     *            implementations, this is redundant.
     */
    public abstract void enqueueResponse(GeneratedMessage reply, Channel channel);

    void setState(ServerState state);
}
