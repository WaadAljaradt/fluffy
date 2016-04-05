/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.server.edges;

import gash.router.server.EdgeCloseListener;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf.RoutingEntry;
import gash.router.server.ServerState;
import gash.router.server.WorkInit;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import pipe.common.Common.Header;
import pipe.work.Work;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;

import java.net.ConnectException;
import java.util.HashMap;

public class EdgeMonitor implements EdgeListener, Runnable {
	protected static Logger logger = LoggerFactory.getLogger("edge monitor");

	private EdgeList outboundEdges;
	private EdgeList inboundEdges;
	private long dt = 3000;
	private ServerState state;
	private boolean forever = true;

    public static HashMap<Integer, EdgeInfo> activeConnections = new HashMap<>();

	public EdgeMonitor(ServerState state) {
		if (state == null)
			throw new RuntimeException("state is null");

		this.outboundEdges = new EdgeList();
		this.inboundEdges = new EdgeList();
		this.state = state;
		this.state.setEmon(this);

		if (state.getConf().getRouting() != null) {
			for (RoutingEntry e : state.getConf().getRouting()) {
				outboundEdges.addNode(e.getId(), e.getHost(), e.getPort());
			}
		}

		// cannot go below 2 sec
		if (state.getConf().getHeartbeatDt() > this.dt)
			this.dt = state.getConf().getHeartbeatDt();
	}

	public void createInboundIfNew(int ref, String host, int port) {
		inboundEdges.createIfNew(ref, host, port);
	}

	private WorkMessage createHB(EdgeInfo ei) {
		WorkState.Builder sb = WorkState.newBuilder();
		sb.setEnqueued(-1);
		sb.setProcessed(-1);

		Heartbeat.Builder bb = Heartbeat.newBuilder();
		bb.setState(sb);

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(state.getConf().getNodeId());
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setBeat(bb);
		
		wb.setSecret(1000l);

		return wb.build();
	}

	public void shutdown() {
		forever = false;
	}

	@Override
	public void run() {
		while (forever) {
			try {
				for (EdgeInfo ei : this.outboundEdges.map.values()) {
					if (ei.isActive() && ei.getChannel() != null && ei.getChannel().isOpen()) {
//						System.out.print("open "+ei.getChannel().isOpen()+" writable "+ei.getChannel().isWritable());
                            if(ei.getChannel().isOpen()) {
                                WorkMessage wm = createHB(ei);
                                ei.getChannel().writeAndFlush(wm);
                            }
                            else
                            {
                                activeConnections.remove(ei.getRef());
                                ei.setActive(false);
                            }
					} else {
						// TODO create a client to the node
						Channel channel = connectToChannel(ei);

						ei.setChannel(channel);

						ei.setActive(true);
						if (channel == null) {
//							logger.info("trying to connect to node " + ei.getRef());
						}
                        else
                        {
                            activeConnections.put(ei.getRef(), ei);
                            System.out.println("Connected to Channel with host " + ei.getHost());
                        }
						//logger.info("trying to connect to node " + ei.getRef());
					}
				}

				Thread.sleep(dt);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private Channel connectToChannel(EdgeInfo ei) {
		Bootstrap b = new Bootstrap();
		NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup();
		WorkInit workInit = new WorkInit(this.state, false);

		try {
			b.group(nioEventLoopGroup).channel(NioSocketChannel.class).handler(workInit);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			// Make the connection attempt.
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//Returning a channel
		Channel retChannel = null;
		
		try{
			retChannel = b.connect(ei.getHost(), ei.getPort()).syncUninterruptibly().channel();

            EdgeCloseListener edgeCloseListener = new EdgeCloseListener(ei);
            retChannel.closeFuture().addListener(edgeCloseListener);
		}catch(Exception e){

			e.printStackTrace();
//            if(e instanceof ConnectException)
//                System.out.println("Unable to connect to "+ei.getRef());
//            else
//			    e.printStackTrace();
		}
		
		return retChannel;

	}

    public static void broadcastMessage(Work.WorkMessage message)
    {
        if(message == null)
            return;

        for(EdgeInfo ei : activeConnections.values())
        {
            if(ei.isActive() && ei.getChannel() != null && ei.getChannel().isOpen())
                ei.getChannel().writeAndFlush(message);
        }
    }

    public static void sendMessage(int nodeId, WorkMessage workMessage)
    {
        try {

            if(workMessage == null)
                return;

            Channel ch = activeConnections.get(nodeId).getChannel();
            if(ch.isOpen())
                ch.writeAndFlush(workMessage);

        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }

	@Override
	public synchronized void onAdd(EdgeInfo ei) {
		// TODO check connection
	}

	@Override
	public synchronized void onRemove(EdgeInfo ei) {
		// TODO ?
	}

    public synchronized EdgeList getOutboundEdges()
    {
        return outboundEdges;
    }
}
