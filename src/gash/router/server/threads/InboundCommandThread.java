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
import pipe.common.Common.Header;
import pipe.work.Work;
import routing.Pipe;
import routing.Pipe.CommandMessage;

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
                    	
                    	System.out.println("Has save - True");
        				if (commandMessage.hasData()) {
        					System.out.println("Has data - True");
        					if (commandMessage.getData().hasFilename()) {
        						System.out.println("Has filename - True");
        						String filename = commandMessage.getData().getFilename();
        						String username = commandMessage.getUsername();
        						File file = new File(filename);
        						FileOutputStream fos = null;

        						System.out.println("-------------START CASSANDRA-----------");
    							System.out.println("Saving in Cassandra");
    							com.datastax.driver.core.ResultSet rs =
    							dao.insert(commandMessage.getData().getFilename(),ByteBuffer.wrap(commandMessage.getData().getData().toByteArray()), (int)commandMessage.getData().getChunkblockid(),0);
    							System.out.println(rs.wasApplied());
    							System.out.println();
    							System.out.println("-------------END CASSANDRA-----------");
        						//Logic to check whether full chunk has been added to DB or not - start
        						
    							Header.Builder hb = Header.newBuilder();
    							hb.setNodeId(990);
    							hb.setTime(System.currentTimeMillis());
    							hb.setDestination(-1);		
    							CommandMessage.Builder rb = CommandMessage.newBuilder();
    							rb.setHeader(hb);
    							
        						if(!chunkCountMapping.containsKey(commandMessage.getUsername())){
        							chunkCountMapping.put(commandMessage.getUsername(), 1);
        							if(commandMessage.getData().getTotalchunks() == 1){
        								rb.setMessage("File Saved Successfully");
        							}else{
        								rb.setMessage("Chunk 1 has been stored." );
        							}
        							System.out.println("Chunk 1 has been stored.");
        						}else{
        							int counts = (chunkCountMapping.get(commandMessage.getUsername()));
        							counts++;
        							if(chunkCountMapping.get(commandMessage.getUsername()) == commandMessage.getData().getTotalchunks()){
        								chunkCountMapping.remove(commandMessage.getUsername()); //removing client from hashmap for chunkCalculation.
        								rb.setMessage("Chunk " + (counts) + " Stored successfully.  \n Whole File Stored Successfully");
        								System.out.println("Chunk " + (counts) + " Stored successfully.  \n Whole File Stored Successfully");
        							}else{
        								chunkCountMapping.put(commandMessage.getUsername(), counts);
        								System.out.println("Chunk " + counts + " Stored successfully.");
            							rb.setMessage("Chunk " + counts + " Stored successfully.");
        							}
        						}
        						conn.writeAndFlush(rb.build());
        						//end
        						
        						

        						/*//get mapping of open file uploads for a particular users
        						if(userFileMaping.containsKey(username)) {
        							if(userFileMaping.get(username).containsKey(filename)) {
        								System.out.println("File stream already exits");
        								FileChunkInfo fc = userFileMaping.get(msg.getUsername()).get(filename);
        								fos = fc.fOutStream;
        								
        								byte[] filedata = msg.getData().getData().toByteArray();
        								int offset = (int) fos.getChannel().size();
        								fos.write(filedata, offset, filedata.length);
        							}
        						}
        						else {
        							System.out.println("Creating new File stream");
        							System.out.println(msg.getData().getFilesize());
        							fos = new FileOutputStream(file);
        							byte[] filedata = msg.getData().getData().toByteArray();
        							fos.write(filedata, 0, filedata.length);
        							FileChunkInfo fci = new FileChunkInfo();
        							fci.chunkBlockId = msg.getData().getChunkblockid();
        							fci.currentBytesLength = fos.getChannel().size();
        							fci.fileLength = msg.getData().getFilesize();
        							fci.fOutStream = fos;
        							HashMap<String, FileChunkInfo> fchunk = new  HashMap<String, FileChunkInfo>();
        							fchunk.put(username, fci);
        							userFileMaping.put(username, fchunk);
        							System.out.println(userFileMaping.get(username).size());
        							System.out.println(fos.getChannel().size());
        						}*/
        						
        						/*if(fos.getChannel().size() == msg.getData().getFilesize()) {
        							fos.close();
        							userFileMaping.get(username).remove(filename);
        							
        							Header.Builder hb = Header.newBuilder();
        							hb.setNodeId(990);
        							hb.setTime(System.currentTimeMillis());
        							hb.setDestination(-1);							
        							CommandMessage.Builder rb = CommandMessage.newBuilder();
        							rb.setHeader(hb);
        							rb.setMessage("File Store was Successful");
        							channel.writeAndFlush(rb.build());
        						}*/
        					
        					}
        				}
        			
                    	
                    }
                    
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
                    
                    if (commandMessage.hasData()) {

                        if(ElectionHandler.getInstance().getLeaderNodeId() == ElectionHandler.conf.getNodeId())
                        {
                            // you are the leader save it and send it to all nodes

                            ByteString data = commandMessage.getData().getData();
                            if(data!= null){
                                byte [] savebytes = commandMessage.getData().getData().toByteArray();

                                ByteBuffer fileByteBuffer = ByteBuffer.wrap( savebytes);
                                long timeStamp = System.currentTimeMillis();
                                
                                ResultSet insertq = dao.insert(commandMessage.getData().getFilename(), fileByteBuffer,(int) commandMessage.getData().getChunkblockid(),timeStamp);
                                if(insertq.wasApplied()){
                                    // duplicate to other nodes
                                    Work.Task.Builder taskBuilder = Work.Task.newBuilder();
                                    taskBuilder.setTaskType(Work.Task.TaskType.SAVEDATATONODE);
                                    taskBuilder.setFilename(commandMessage.getData().getFilename());
                                    taskBuilder.setData(commandMessage.getData().getData());
                                    taskBuilder.setSeqId((int) commandMessage.getData().getChunkblockid());
                                    taskBuilder.setSeriesId(timeStamp);

                                    Common.Header.Builder hb = Common.Header.newBuilder();
                                    hb.setNodeId(inboundCommandQueue.getState().getConf().getNodeId());
                                    hb.setDestination(-1);
                                    hb.setTime(System.currentTimeMillis());

                                    Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
                                    wb.setHeader(hb);
                                    wb.setTask(taskBuilder);

                                    wb.setSecret(1000l);

                                    //EdgeMonitor.sendMessage(ElectionHandler.getInstance().getLeaderNodeId(), wb.build());
                                    EdgeMonitor.broadcastMessage(wb.build());

                                    //TODO acknowledge the client
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