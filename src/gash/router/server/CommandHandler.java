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
package gash.router.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.util.HashMap;

import gash.router.server.edges.EdgeMonitor;
import gash.router.server.queue.ChannelQueue;
import gash.router.server.queue.InboundCommandQueue;
import gash.router.server.queue.InboundWorkerQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.google.protobuf.ByteString;

import gash.router.container.RoutingConf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common;
import pipe.common.Common.Failure;
import pipe.common.Common.Header;
import pipe.election.Election;
import pipe.filedata.Filedata.FileDataInfo;
import pipe.work.Work;
import routing.Pipe.CommandMessage;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class CommandHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("cmd");
	protected RoutingConf conf;
	private ServerState state;
	private HashMap<String, Integer> chunkCountMapping = new HashMap<String, Integer>();
	
	CassandraDAO dao = new CassandraDAO();
	
	private InboundCommandQueue queue;

	public CommandHandler(RoutingConf conf) {
		if (conf != null) {
			this.conf = conf;

			state = new ServerState();
			state.setConf(conf);
		}
	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, CommandMessage msg) throws Exception {
		//Enqueue the incomming command message
		getQueueInstance(ctx, state).enqueueRequest(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

	private ChannelQueue getQueueInstance(ChannelHandlerContext ctx, ServerState state)
	{
		if (queue != null)
			return queue;
		else {
			queue = new InboundCommandQueue(ctx.channel(), state);
		}

		return queue;
	}
}