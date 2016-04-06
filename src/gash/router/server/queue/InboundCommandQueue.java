package gash.router.server.queue;

import com.google.protobuf.GeneratedMessage;
import gash.router.server.ServerState;
import gash.router.server.threads.InboundCommandThread;
import io.netty.channel.Channel;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by patel on 4/6/2016.
 */
public class InboundCommandQueue implements ChannelQueue {

    private LinkedBlockingDeque<GeneratedMessage> inboundQueue;
    private Channel channel;
    private ServerState state;

    private InboundCommandThread inboundCommandThread;

    public static ChannelQueue getInstance(Channel channel, ServerState state)
    {
        ChannelQueue queue = null;

        queue = new WorkerQueue(channel,state);

        // on close remove from queue
//        channel.closeFuture().addListener(new WorkHandler.ConnectionCloseListener(queue));

        return queue;
    }

    public InboundCommandQueue(Channel channel, ServerState state) {
        this.channel = channel;
        this.state = state;
        init();
    }

    protected void init() {
        inboundQueue = new LinkedBlockingDeque<>();

//        System.out.println("Starting to listen to Work worker");

        inboundCommandThread = new InboundCommandThread(this);
        inboundCommandThread.start();
    }

    @Override
    public void shutdown(boolean hard) {

        if (inboundCommandThread != null) {
            inboundCommandThread.forever = false;
            if (inboundCommandThread.getState() == Thread.State.BLOCKED || inboundCommandThread.getState() == Thread.State.WAITING)
                inboundCommandThread.interrupt();
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
