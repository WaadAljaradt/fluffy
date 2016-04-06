package gash.router.server.threads;

import com.datastax.driver.core.ResultSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import gash.router.server.CassandraDAO;
import gash.router.server.ElectionHandler;
import gash.router.server.MessageServer;
import gash.router.server.PrintUtil;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.queue.InboundWorkerQueue;
import gash.router.server.queue.WorkerQueue;
import io.netty.channel.Channel;
import pipe.common.Common;
import pipe.work.Work;

import java.nio.ByteBuffer;

/**
 * Created by patel on 4/5/2016.
 */
public class InboundWorkerThread extends Thread {

    InboundWorkerQueue inboundWorkQueue;
    public boolean forever = true;
    CassandraDAO dao;

    public InboundWorkerThread(InboundWorkerQueue inboundWorkQueue)
    {
        super(new ThreadGroup("PerChannelQ-" + System.nanoTime()),""+0);
        this.inboundWorkQueue = inboundWorkQueue;

        dao = new CassandraDAO();
        if (inboundWorkQueue.getInboundQueue()== null)
            throw new RuntimeException("connection worker detected null inboundWork queue");
    }

    @Override
    public void run() {

        Channel conn = inboundWorkQueue.getChannel();
        if (conn == null || !conn.isOpen()) {
//            System.out.println("connection missing, no inboundWork communication");
            return;
        }

        while (true) {
            if (!forever && inboundWorkQueue.getInboundQueue().size() == 0)
                break;

            try {
                // block until a message is enqueued
                GeneratedMessage msg = inboundWorkQueue.getInboundQueue().take();

                // process request and enqueue response

                if (msg instanceof Work.WorkMessage) {
                    Work.WorkMessage workMessage = ((Work.WorkMessage) msg);

                    boolean msgDropFlag;

                    if (workMessage.hasBeat()) {

                        System.out.println("Heartbeat from "+workMessage.getHeader().getNodeId());

                        ElectionHandler.getInstance().checkCurrentState();

                    } else if (workMessage.hasPing()) {

                        System.out.println("ping from " + workMessage.getHeader().getNodeId());
                        boolean p = workMessage.getPing();
                        Work.WorkMessage.Builder rb = Work.WorkMessage.newBuilder();
                        rb.setPing(true);
                        inboundWorkQueue.enqueueResponse(rb.build(), inboundWorkQueue.getChannel());

                    } else if (workMessage.hasErr()) {
                        Common.Failure err = workMessage.getErr();
                        System.out.println("failure from " + workMessage.getHeader().getNodeId());

                    } else if (workMessage.hasTask()) {

                        Work.Task t = workMessage.getTask();
                        if (t.getTaskType() == Work.Task.TaskType.SAVEDATATOLEADER) {

                            System.out.println("Saving to myself and broadcasting it");

                            ByteString data = t.getData();
                            if(data!= null){
                                byte [] savebytes = t.getData().toByteArray();

                                ByteBuffer fileByteBuffer = ByteBuffer.wrap( savebytes);
                                ResultSet insertq = dao.insert(t.getFilename(), fileByteBuffer);
                                if(insertq.wasApplied()){
                                    // duplicate to other nodes
                                    Work.Task.Builder taskBuilder = Work.Task.newBuilder();
                                    taskBuilder.setTaskType(Work.Task.TaskType.SAVEDATATONODE);
                                    taskBuilder.setFilename(workMessage.getTask().getFilename());
                                    taskBuilder.setData(workMessage.getTask().getData());
                                    taskBuilder.setSeriesId(t.getSeriesId());
                                    taskBuilder.setSeqId(t.getSeqId());

                                    Common.Header.Builder hb = Common.Header.newBuilder();
                                    hb.setNodeId(inboundWorkQueue.getState().getConf().getNodeId());
                                    hb.setDestination(-1);
                                    hb.setTime(System.currentTimeMillis());

                                    Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
                                    wb.setHeader(hb);
                                    wb.setTask(taskBuilder);

                                    wb.setSecret(1000l);

                                    //EdgeMonitor.sendMessage(ElectionHandler.getInstance().getLeaderNodeId(), wb.build());
                                    EdgeMonitor.broadcastMessage(wb.build());

                                    taskBuilder = Work.Task.newBuilder();
                                    taskBuilder.setTaskType(Work.Task.TaskType.DATASAVEDBYEVERYONE);
                                    taskBuilder.setSeriesId(t.getSeriesId());
                                    taskBuilder.setSeqId(t.getSeqId());
                                    hb = Common.Header.newBuilder();
                                    hb.setNodeId(inboundWorkQueue.getState().getConf().getNodeId());
                                    hb.setDestination(workMessage.getHeader().getNodeId());
                                    hb.setTime(System.currentTimeMillis());
                                    

                                    wb = Work.WorkMessage.newBuilder();
                                    wb.setHeader(hb);
                                    wb.setTask(taskBuilder);

                                    wb.setSecret(1000l);

                                    EdgeMonitor.sendMessage(workMessage.getHeader().getNodeId(), wb.build());
                                }
                            }
                        }
                        else if (t.getTaskType() == Work.Task.TaskType.SAVEDATATONODE) {
                        	System.out.println("i got message from leader and i will save it");
                            ByteString data = t.getData();
                            if (data != null) {
                                byte [] savebytes = t.getData().toByteArray();

                                ByteBuffer fileByteBuffer = ByteBuffer.wrap( savebytes);
                                ResultSet insertq = dao.insert(t.getFilename(), fileByteBuffer);
                                if(insertq.wasApplied()) {

                                    Work.Task.Builder taskBuilder = Work.Task.newBuilder();
                                    taskBuilder.setTaskType(Work.Task.TaskType.DATASAVEDBYNODE);
                                    taskBuilder.setSeriesId(t.getSeriesId());
                                    taskBuilder.setSeqId(t.getSeqId());
                                    Common.Header.Builder hb = Common.Header.newBuilder();
                                    hb.setNodeId(inboundWorkQueue.getState().getConf().getNodeId());
                                    hb.setDestination(workMessage.getHeader().getNodeId());
                                    hb.setTime(System.currentTimeMillis());

                                    Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
                                    wb.setHeader(hb);
                                    wb.setTask(taskBuilder);

                                    wb.setSecret(1000l);

                                    EdgeMonitor.sendMessage(workMessage.getHeader().getNodeId(), wb.build());
                                }
                            }
                        }

                    } else if (workMessage.hasState()) {
                        Work.WorkState s = workMessage.getState();
                    }
                    else if (workMessage.hasLeader()) {

                        ElectionHandler.getInstance().handleLeader(workMessage);

                    } else if (workMessage.hasElection()) {

                        ElectionHandler.getInstance().handleElection(workMessage);

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
