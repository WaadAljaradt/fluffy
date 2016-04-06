package gash.router.server.threads;

import com.google.protobuf.GeneratedMessage;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.queue.OutboundWorkerQueue;
import gash.router.server.queue.WorkerQueue;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import pipe.work.Work;

/**
 * Created by patel on 4/5/2016.
 */
public class OutboundWorkerThread extends Thread {

    OutboundWorkerQueue outboundWorkQueue;
    public boolean forever = true;

    public OutboundWorkerThread(OutboundWorkerQueue outboundWorkQueue)
    {
        super(new ThreadGroup("PerChannelQ-" + System.nanoTime()),""+0);
        this.outboundWorkQueue = outboundWorkQueue;
    }

    @Override
    public void run() {
        Channel conn = outboundWorkQueue.getChannel();
        if (conn == null || !conn.isOpen()) {
//            System.out.println("connection missing, no outboundWork communication");
            return;
        }

        while (true) {
            if (!forever && outboundWorkQueue.getOutboundQueue().size() == 0)
                break;

            try {
                // block until a message is enqueued
                GeneratedMessage msg = outboundWorkQueue.getOutboundQueue().take();
                if (conn.isWritable()) {
                    boolean rtn = false;
                    if (outboundWorkQueue.getChannel() != null && outboundWorkQueue.getChannel().isOpen() && outboundWorkQueue.getChannel().isWritable()) {

                        ChannelFuture cf = outboundWorkQueue.getChannel().writeAndFlush(msg);

//                        System.out.println("Server--sending -- work-- response to "+((Work.WorkMessage)msg).getHeader().getDestination());
                        // blocks on write - use listener to be async
                        cf.awaitUninterruptibly();
//                        System.out.println("Written to channel");
                        rtn = cf.isSuccess();
                        if (!rtn) {
                            System.out.println("Sending failed " + rtn
                                    + "{Reason:" + cf.cause() + "}");
                            outboundWorkQueue.getOutboundQueue().putFirst(msg);
                        }
//                        else
//                            System.out.println("Message Sent");
                    }

                } else
                    outboundWorkQueue.getOutboundQueue().putFirst(msg);
            } catch (InterruptedException ie) {
                break;
            } catch (Exception e) {
                System.out.println("Unexpected communcation failure");
                e.printStackTrace();
                break;
            }
        }

        if (!forever) {
            System.out.println("connection queue closing");
        }
    }
}
