package gash.router.server.threads;

import com.datastax.driver.core.ResultSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import gash.router.server.CassandraDAO;
import gash.router.server.ElectionHandler;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.queue.InboundCommandQueue;
import io.netty.channel.Channel;
import pipe.common.Common;
import pipe.work.Work;
import routing.Pipe;

import java.nio.ByteBuffer;

/**
 * Created by patel on 4/6/2016.
 */
public class InboundCommandThread extends Thread {

    InboundCommandQueue inboundCommandQueue;
    public boolean forever = true;
    CassandraDAO dao;

    public InboundCommandThread(InboundCommandQueue inboundCommandQueue)
    {
        super(new ThreadGroup("PerChannelQ-" + System.nanoTime()),""+0);
        this.inboundCommandQueue = inboundCommandQueue;

        dao = new CassandraDAO();
        if (inboundCommandQueue.getInboundQueue()== null)
            throw new RuntimeException("connection worker detected null inboundWork queue");
    }

    @Override
    public void run() {

        Channel conn = inboundCommandQueue.getChannel();
        if (conn == null || !conn.isOpen()) {
//            System.out.println("connection missing, no inboundWork communication");
            return;
        }

        while (true) {
            if (!forever && inboundCommandQueue.getInboundQueue().size() == 0)
                break;

            try {
                // block until a message is enqueued
                GeneratedMessage msg = inboundCommandQueue.getInboundQueue().take();

                // process request and enqueue response

                if (msg instanceof Pipe.CommandMessage) {
                    Pipe.CommandMessage commandMessage = ((Pipe.CommandMessage) msg);

                    if (commandMessage.hasRetrieve()) {

                        boolean hasSavedData = false;

                        if(hasSavedData)
                        {
                            //is saved in local database
                        }
                        else
                        {
                            Work.WorkState.Builder sb = Work.WorkState.newBuilder();
                            sb.setEnqueued(-1);
                            sb.setProcessed(-1);

                            pipe.election.Election.LeaderStatus.Builder leaderStatusBuilder = pipe.election.Election.LeaderStatus.newBuilder();
                            leaderStatusBuilder.setAction(pipe.election.Election.LeaderStatus.LeaderQuery.WHOISTHELEADER);

                            Common.Header.Builder hb = Common.Header.newBuilder();
                            hb.setNodeId(inboundCommandQueue.getState().getConf().getNodeId());
                            hb.setDestination(-1);
                            hb.setTime(System.currentTimeMillis());

                            Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
                            wb.setHeader(hb);
                            wb.setLeader(leaderStatusBuilder);

                            wb.setSecret(1000l);

                            EdgeMonitor.broadcastMessage(wb.build());
                        }
                    }
                    else if (commandMessage.hasData()) {

                        if(ElectionHandler.getInstance().getLeaderNodeId() == ElectionHandler.conf.getNodeId())
                        {
                            // you are the leader save it and send it to all nodes
                        }
                        else
                        {
                            Work.Task.Builder taskBuilder = Work.Task.newBuilder();
                            taskBuilder.setTaskType(Work.Task.TaskType.SAVEDATATOLEADER);
                            taskBuilder.setFilename(commandMessage.getData().getFilename());
                            taskBuilder.setData(commandMessage.getData().getData());
                            taskBuilder.setSeqId(0);
                            taskBuilder.setSeriesId(System.currentTimeMillis());

                            Common.Header.Builder hb = Common.Header.newBuilder();
                            hb.setNodeId(inboundCommandQueue.getState().getConf().getNodeId());
                            hb.setDestination(-1);
                            hb.setTime(System.currentTimeMillis());

                            Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
                            wb.setHeader(hb);
                            wb.setTask(taskBuilder);

                            wb.setSecret(1000l);

                            EdgeMonitor.sendMessage(ElectionHandler.getInstance().getLeaderNodeId(), wb.build());
                        }
//                    if (msg.getData().hasFilename()) {
//                        File file = new File(msg.getData().getFilename());
//                        if (msg.hasData()) {
//                            FileOutputStream fos = new FileOutputStream(file);
//                            byte[] filedata = msg.getData().getData().toByteArray();
//                            fos.write(filedata, 0, filedata.length);
//                            fos.close();
//                        }
//                    }
                    }
                }
            } catch (InterruptedException ie) {
                break;
            } catch (Exception e) {
                System.out.println("Unexpected processing failure");
                e.printStackTrace();
                break;
            }
        }

        if (!forever) {
            System.out.println("connection queue closing");
        }
    }

}
