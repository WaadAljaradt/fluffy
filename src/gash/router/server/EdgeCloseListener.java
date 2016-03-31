package gash.router.server;

import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeMonitor;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * Created by patel on 3/31/2016.
 */
public class EdgeCloseListener implements ChannelFutureListener {

    private EdgeInfo edgeInfo;

    public EdgeCloseListener(EdgeInfo edgeInfo)
    {
        this.edgeInfo = edgeInfo;
    }

    @Override
    public void operationComplete(ChannelFuture channelFuture) throws Exception
    {
        // remove active connection and check if removed node was leader
        EdgeMonitor.activeConnections.remove(edgeInfo.getRef());
        ElectionHandler.getInstance().checkCurrentState();
    }
}
