package gash.router.server.queue;

import com.google.protobuf.GeneratedMessage;
import gash.router.server.ServerState;
import gash.router.server.threads.InboundWorkerThread;
import io.netty.channel.Channel;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by patel on 4/6/2016.
 */
public class InboundWorkerQueue implements ChannelQueue {

    private LinkedBlockingDeque<GeneratedMessage> inboundQueue;
    private Channel channel;
    private ServerState state;

    private InboundWorkerThread inboundWorkerThread;

    public static ChannelQueue getInstance(Channel channel, ServerState state)
    {
        ChannelQueue queue = null;

        queue = new WorkerQueue(channel,state);

        // on close remove from queue
//        channel.closeFuture().addListener(new WorkHandler.ConnectionCloseListener(queue));

        return queue;
    }

    public InboundWorkerQueue(Channel channel, ServerState state) {
        this.channel = channel;
        this.state = state;
        init();
    }

    protected void init() {
        inboundQueue = new LinkedBlockingDeque<>();

//        System.out.println("Starting to listen to Work worker");

        inboundWorkerThread = new InboundWorkerThread(this);
        inboundWorkerThread.start();
    }

    @Override
    public void shutdown(boolean hard) {

        if (inboundWorkerThread!= null) {
            inboundWorkerThread.forever = false;
            if (inboundWorkerThread.getState() == Thread.State.BLOCKED || inboundWorkerThread.getState() == Thread.State.WAITING)
                inboundWorkerThread.interrupt();
        }
    }

    @Override
    public void enqueueRequest(GeneratedMessage req, Channel channel) {
        try {
            inboundQueue.put(req);
        } catch (InterruptedException e) {
            System.out.println("message not enqueued for processing");
            e.printStackTrace();
        }
    }

    @Override
    public void enqueueResponse(GeneratedMessage reply, Channel channel) {
    }

    @Override
    public void setState(ServerState state) {
        this.state = state;
    }

    public ServerState getState()
    {
        return state;
    }

    public LinkedBlockingDeque<GeneratedMessage> getInboundQueue()
    {
        return inboundQueue;
    }

    public Channel getChannel()
    {
        return channel;
    }
}
