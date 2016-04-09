package gash.router.server.threads;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import gash.router.server.CassandraDAO;
import gash.router.server.ElectionHandler;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.queue.InboundCommandQueue;
import io.netty.channel.Channel;
import pipe.common.Common;
import pipe.common.Common.Failure;
import pipe.common.Common.Header;
import pipe.filedata.Filedata.FileDataInfo;
import pipe.work.Work;
import routing.Pipe;
import routing.Pipe.CommandMessage;
import routing.Pipe.CommandMessageOrBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Created by patel on 4/6/2016.
 */
public class InboundCommandThread extends Thread {

    InboundCommandQueue inboundCommandQueue;
    public boolean forever = true;
    private HashMap<String, Integer> chunkCountMapping = new HashMap<String, Integer>();
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
                    
        			if(commandMessage.hasSave()){
                    	
        				if (commandMessage.hasData()) {
        					
                            if(ElectionHandler.getInstance().getLeaderNodeId() == ElectionHandler.conf.getNodeId())
                            {
                                // you are the leader save it and send it to all nodes
                                ByteString data = commandMessage.getData().getData();
                                if(data!= null){
                                	
                                    byte [] savebytes = commandMessage.getData().getData().toByteArray();

                                    long timeStamp = System.currentTimeMillis();

                                    ByteBuffer fileByteBuffer = ByteBuffer.wrap( savebytes);
                                    ResultSet insertq = dao.insert(commandMessage.getData().getFilename(), fileByteBuffer, (int)commandMessage.getData().getChunkblockid(), timeStamp);
                                    if(insertq.wasApplied()){
                                        // duplicate to other nodes
                                        Work.Task.Builder taskBuilder = Work.Task.newBuilder();
                                        taskBuilder.setTaskType(Work.Task.TaskType.SAVEDATATONODE);
                                        taskBuilder.setFilename(commandMessage.getData().getFilename());
                                        taskBuilder.setData(commandMessage.getData().getData());
                                        taskBuilder.setSeqId((int)commandMessage.getData().getChunkblockid());
                                        taskBuilder.setSeriesId(timeStamp);

                                        Common.Header.Builder hb = Common.Header.newBuilder();
                                        hb.setNodeId(inboundCommandQueue.getState().getConf().getNodeId());
                                        hb.setDestination(-1);
                                        hb.setTime(System.currentTimeMillis());

                                        Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
                                        wb.setHeader(hb);
                                        wb.setTask(taskBuilder);

                                        wb.setSecret(1000l);

                                        EdgeMonitor.broadcastMessage(wb.build());

                                        //TODO acknowledge the client
                                        
                                        CommandMessage.Builder rb = CommandMessage.newBuilder();
                                        rb.setHeader(hb);
                                        
                                        if(!chunkCountMapping.containsKey(commandMessage.getUsername())){
                							chunkCountMapping.put(commandMessage.getUsername(), 1);
                							if(commandMessage.getData().getTotalchunks() == 1){
                								rb.setMessage("File Saved Successfully");
                							}else{
                								rb.setMessage("Chunk 1 has been stored." );
                							}
                						}else{
                							int counts = (chunkCountMapping.get(commandMessage.getUsername()));
                							counts++;
                							if(chunkCountMapping.get(commandMessage.getUsername()) == commandMessage.getData().getTotalchunks()){
                								chunkCountMapping.remove(commandMessage.getUsername()); //removing client from hashmap for chunkCalculation.
                								rb.setMessage("Chunk " + (counts) + " Stored successfully.  \n Whole File Stored Successfully");
                							}else{
                								chunkCountMapping.put(commandMessage.getUsername(), counts);
                    							rb.setMessage("Chunk " + counts + " Stored successfully.");
                							}
                						}
                						conn.writeAndFlush(rb.build());
                                    }
                                }
                            }
                            else
                            {
                                Work.Task.Builder taskBuilder = Work.Task.newBuilder();
                                taskBuilder.setTaskType(Work.Task.TaskType.SAVEDATATOLEADER);
                                taskBuilder.setFilename(commandMessage.getData().getFilename());
                                taskBuilder.setData(commandMessage.getData().getData());
                                taskBuilder.setSeqId((int)commandMessage.getData().getChunkblockid());
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
                        
        				}
                    }
        			else if (commandMessage.hasRetrieve()) {
        				boolean hasSavedData = false;
        				if(commandMessage.getData().hasFilename()) {
        	                
        					Row r = dao.get(commandMessage.getData().getFilename());
        					if(r!=null) {
        						hasSavedData = true;
        					}
        				}
        				
                        if(hasSavedData)
                        {
            				Header.Builder hb = Header.newBuilder();
            				hb.setNodeId(990);
            				hb.setTime(System.currentTimeMillis());
            				hb.setDestination(-1);

            				CommandMessage.Builder rb = CommandMessage.newBuilder();
            				rb.setHeader(hb);
            				rb.setPing(true);
            				rb.setRetrieve(true);

            				FileDataInfo.Builder fd = FileDataInfo.newBuilder();
        					com.datastax.driver.core.ResultSet rs = dao.getMatchingFiles(commandMessage.getData().getFilename());
        					Long fileCount = dao.getFileCount(commandMessage.getData().getFilename());
        					System.out.println("File Count = " +  fileCount);
        					System.out.println("Before the row retriver");
            				for(Row row: rs) {
                				fd.setFilename(row.getString("filename"));
            					System.out.println("Filename " + row.getString("filename"));
                				byte[] data = Bytes.getArray(row.getBytes("file"));
                				ByteString bs = ByteString.copyFrom(data);
                				fd.setData(bs);
                				fd.setChunkblockid(row.getInt("seq_id"));
                				fd.setTotalchunks(fileCount); //adding How many chunks are required.
                				rb.setData(fd);
                				conn.writeAndFlush(rb.build());
                        	}
                        }
                        else
                        {
                            Work.WorkState.Builder sb = Work.WorkState.newBuilder();
                            sb.setEnqueued(-1);
                            sb.setProcessed(-1);

                            pipe.election.Election.LeaderStatus.Builder leaderStatusBuilder = pipe.election.Election.LeaderStatus.newBuilder();
                            leaderStatusBuilder.setAction(pipe.election.Election.LeaderStatus.LeaderQuery.WHOISTHELEADER);

                            Common.Header.Builder hb = Common.Header.newBuilder();
                            hb.setNodeId(990);
                            hb.setDestination(-1);
                            hb.setTime(System.currentTimeMillis());

                            Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
                            wb.setHeader(hb);
                            wb.setLeader(leaderStatusBuilder);

                            wb.setSecret(1000l);

                            EdgeMonitor.broadcastMessage(wb.build());
                            
                            CommandMessage.Builder rb = CommandMessage.newBuilder();
                            Failure.Builder flr = Failure.newBuilder();
                            flr.setMessage("FIle Not Found!");
            				rb.setHeader(hb);
            				rb.setPing(true);
            				rb.setRetrieve(true);
            				rb.setErr(flr);
            				conn.writeAndFlush(rb.build());
                        }
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