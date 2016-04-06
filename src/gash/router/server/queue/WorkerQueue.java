package gash.router.server.queue;

import com.google.protobuf.GeneratedMessage;
import gash.router.server.ServerState;
import gash.router.server.WorkHandler;
import gash.router.server.threads.InboundWorkerThread;
import gash.router.server.threads.OutboundWorkerThread;
import io.netty.channel.Channel;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by patel on 4/5/2016.
 */
public class WorkerQueue implements ChannelQueue {

    private LinkedBlockingDeque<GeneratedMessage> inboundQueue;
    private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outboundQueue;
    private Channel channel;
    private ServerState state;

    private OutboundWorkerThread outboundWorkerThread;
    private InboundWorkerThread inboundWorkerThread;

    public static ChannelQueue getInstance(Channel channel,ServerState state)
    {
        ChannelQueue queue = null;

        queue = new WorkerQueue(channel,state);

        // on close remove from queue
//        channel.closeFuture().addListener(new WorkHandler.ConnectionCloseListener(queue));

        return queue;
    }

    public WorkerQueue(Channel channel,ServerState state) {
        this.channel = channel;
        this.state = state;
        init();
    }

    protected void init() {
        inboundQueue = new LinkedBlockingDeque<>();
        outboundQueue = new LinkedBlockingDeque<>();

//        System.out.println("Starting to listen to Work worker");

//        outboundWorkerThread = new OutboundWorkerThread(this);
//        outboundWorkerThread.start();
//
//        inboundWorkerThread = new InboundWorkerThread(this);
//        inboundWorkerThread.start();
    }

    @Override
    public void shutdown(boolean hard) {

        if (inboundWorkerThread!= null) {
            inboundWorkerThread.forever = false;
            if (inboundWorkerThread.getState() == Thread.State.BLOCKED || inboundWorkerThread.getState() == Thread.State.WAITING)
                inboundWorkerThread.interrupt();
        }

        if (outboundWorkerThread!= null) {
            outboundWorkerThread.forever = false;
            if (outboundWorkerThread.getState() == Thread.State.BLOCKED || inboundWorkerThread.getState() == Thread.State.WAITING)
                outboundWorkerThread.interrupt();
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

    public LinkedBlockingDeque<GeneratedMessage> getOutboundQueue()
    {
        return outboundQueue;
    }

    public Channel getChannel()
    {
        return channel;
    }
}
