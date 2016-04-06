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

//                        logger.info("heartbeat from " + req.getHeader().getNodeId());
//                        EdgeMonitor emon = MessageServer.getEmon();
//                        EdgeInfo ei = new EdgeInfo(req.getHeader().getNodeId(),"",req.getHeader().getSource());
//                        ei.setChannel(sq.getChannel());
//                        emon.addToInbound(ei);
//                        RaftManager.getInstance().assessCurrentState();
                        ElectionHandler.getInstance().checkCurrentState();

                    } else if (workMessage.hasPing()) {

                        System.out.println("ping from " + workMessage.getHeader().getNodeId());
                        boolean p = workMessage.getPing();
                        Work.WorkMessage.Builder rb = Work.WorkMessage.newBuilder();
                        rb.setPing(true);
                        inboundWorkQueue.enqueueResponse(rb.build(), inboundWorkQueue.getChannel());

//                        logger.info("ping from <node,host> : <" + req.getHeader().getNodeId() + ", " + req.getHeader().getSourceHost()+">");
//                        PrintUtil.printWork(req);
//                        if(req.getHeader().getDestination() == sq.state.getConf().getNodeId()){
//                            //handle message by self
//                            logger.info("Ping for me: " + " from "+ req.getHeader().getSourceHost());
//
//                            Work.WorkRequest.Builder rb = Work.WorkRequest.newBuilder();
//
//                            Common.Header.Builder hb = Common.Header.newBuilder();
//                            hb.setNodeId(sq.state.getConf().getNodeId());
//                            hb.setTime(System.currentTimeMillis());
//                            hb.setDestination(Integer.parseInt(req.getHeader().getSourceHost().substring(req.getHeader().getSourceHost().lastIndexOf('_')+1)));
//                            hb.setSourceHost(req.getHeader().getSourceHost().substring(req.getHeader().getSourceHost().indexOf('_')+1));
//                            hb.setDestinationHost(req.getHeader().getSourceHost());
//                            hb.setMaxHops(5);
//
//                            rb.setHeader(hb);
//                            rb.setSecret(1234567809);
//                            rb.setPayload(Work.Payload.newBuilder().setPing(true));
//                            //channel.writeAndFlush(rb.build());
//                            sq.enqueueResponse(rb.build(),sq.getChannel());
//                        }
//                        else { //message doesn't belong to current node. Forward on other edges
//                            msgDropFlag = true;
//                            PrintUtil.printWork(req);
//                            if (req.getHeader().getMaxHops() > 0 && MessageServer.getEmon() != null) {// forward if Comm-worker port is active
//                                for (EdgeInfo ei : MessageServer.getEmon().getOutboundEdgeInfoList()) {
//                                    if (ei.isActive() && ei.getChannel() != null) {// check if channel of outbound edge is active
//                                        logger.debug("Workmessage being queued");
//                                        Work.WorkRequest.Builder wb = Work.WorkRequest.newBuilder();
//
//                                        Common.Header.Builder hb = Common.Header.newBuilder();
//                                        hb.setNodeId(sq.state.getConf().getNodeId());
//                                        hb.setTime(req.getHeader().getTime());
//                                        hb.setDestination(req.getHeader().getDestination());
//                                        hb.setSourceHost(sq.state.getConf().getNodeId()+"_"+req.getHeader().getSourceHost());
//                                        hb.setDestinationHost(req.getHeader().getDestinationHost());
//                                        hb.setMaxHops(req.getHeader().getMaxHops() -1);
//
//                                        wb.setHeader(hb);
//                                        wb.setSecret(1234567809);
//                                        wb.setPayload(Work.Payload.newBuilder().setPing(true));
//                                        //ei.getChannel().writeAndFlush(wb.build());
//                                        PerChannelWorkQueue edgeQueue = (PerChannelWorkQueue) ei.getQueue();
//                                        edgeQueue.enqueueResponse(wb.build(),ei.getChannel());
//                                        msgDropFlag = false;
//                                        logger.debug("Workmessage queued");
//                                    }
//                                }
//                                if (msgDropFlag)
//                                    logger.info("Message dropped <node,ping,destination>: <" + req.getHeader().getNodeId() + "," + payload.getPing() + "," + req.getHeader().getDestination() + ">");
//                            } else {// drop the message or queue it for limited time to send to connected node
//                                //todo
//                                logger.info("No outbound edges to forward. To be handled");
//                            }
//                        }

                    } else if (workMessage.hasErr()) {
//                        Common.Failure err = payload.getErr();
//                        logger.error("failure from " + req.getHeader().getNodeId());

                        Common.Failure err = workMessage.getErr();
                        System.out.println("failure from " + workMessage.getHeader().getNodeId());

                    } else if (workMessage.hasTask()) {
//                        Work.Task t = payload.getTask();
//                        sq.gerServerState().getTasks().addTask(t);

                        Work.Task t = workMessage.getTask();
                        if (t.getTaskType() == Work.Task.TaskType.SAVEDATATOLEADER) {
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

                            ByteString data = t.getData();
                            if (data != null) {
                                byte [] savebytes = t.getData().toByteArray();

                                ByteBuffer fileByteBuffer = ByteBuffer.wrap( savebytes);
                                ResultSet insertq = dao.insert(t.getFilename(), fileByteBuffer);
                                if(insertq.wasApplied()) {

                                    Work.Task.Builder taskBuilder = Work.Task.newBuilder();
                                    taskBuilder.setTaskType(Work.Task.TaskType.DATASAVEDBYNODE);

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
//                    else if(workMessage.hasRaftmsg())
//                    {
//                        RaftManager.getInstance().processRequest((Work.WorkRequest)msg);
//                        //ElectionManager.getInstance().assessCurrentState();
//                    }
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
