package gash.router.server.queue;

import com.google.protobuf.GeneratedMessage;
import gash.router.server.ServerState;
import gash.router.server.threads.OutboundWorkerThread;
import io.netty.channel.Channel;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by patel on 4/6/2016.
 */
public class OutboundWorkerQueue implements ChannelQueue {

    private LinkedBlockingDeque<GeneratedMessage> outboundQueue;
    private Channel channel;
    private ServerState state;

    private OutboundWorkerThread outboundWorkerThread;

    public static ChannelQueue getInstance(Channel channel, ServerState state)
    {
        ChannelQueue queue = null;

        queue = new WorkerQueue(channel,state);

        // on close remove from queue
//        channel.closeFuture().addListener(new WorkHandler.ConnectionCloseListener(queue));

        return queue;
    }

    public OutboundWorkerQueue(Channel channel, ServerState state) {
        this.channel = channel;
        this.state = state;
        init();
    }

    protected void init() {
        outboundQueue = new LinkedBlockingDeque<>();

//        System.out.println("Starting to listen to Work worker");

        outboundWorkerThread = new OutboundWorkerThread(this);
        outboundWorkerThread.start();
    }

    @Override
    public void shutdown(boolean hard) {

        if (outboundWorkerThread!= null) {
            outboundWorkerThread.forever = false;
            if (outboundWorkerThread.getState() == Thread.State.BLOCKED || outboundWorkerThread.getState() == Thread.State.WAITING)
                outboundWorkerThread.interrupt();
        }
    }

    @Override
    public void enqueueResponse(GeneratedMessage reply, Channel channel) {
        if (reply == null)
            return;

        try {
            outboundQueue.put(reply);
        } catch (InterruptedException e) {
            System.out.println("message not enqueued for reply");
            e.printStackTrace();
        }
    }

    @Override
    public void enqueueRequest(GeneratedMessage req, Channel channel) {

    }

    @Override
    public void setState(ServerState state) {
        this.state = state;
    }

    public ServerState getState()
    {
        return state;
    }

    public LinkedBlockingDeque<GeneratedMessage> getOutboundQueue()
    {
        return outboundQueue;
    }

    public Channel getChannel()
    {
        return channel;
    }
}
