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
package gash.router.client;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.protobuf.ByteString;

import pipe.common.Common.Header;
import pipe.filedata.Filedata.FileDataInfo;
import routing.Pipe.CommandMessage;

/**
 * front-end (proxy) to our service - functional-based
 * 
 * @author gash
 * 
 */
public class MessageClient {
	// track requests
	private long curID = 0;

	public MessageClient(String host, int port) {
		init(host, port);
	}

	private void init(String host, int port) {
		CommConnection.initConnection(host, port);
	}

	public void addListener(CommListener listener) {
		CommConnection.getInstance().addListener(listener);
	}

	public void downloadFile(String filename) {
		// construct the message to send
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);
		
		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setPing(true);
		rb.setRetrieve(true);
		FileDataInfo.Builder fd = FileDataInfo.newBuilder();
		fd.setFilename(filename);
		rb.setData(fd);

		try {
			// direct no queue
			// CommConnection.getInstance().write(rb.build());

			// using queue
			CommConnection.getInstance().enqueue(rb.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void uploadFile(String filepath, String username) {
		// construct the message to send

		File file = new File(filepath);
		long fileLength = file.length(); // File Length
		long chunkLength = 1024 * 1024; // sets chunkLength Size

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setPing(true);
		rb.setSave(true);
		rb.setUsername(username);
		try {
			FileInputStream fis = new FileInputStream(file);
			/*Check if the file length size is less than 1 MB
			 * Send only one chunk in this case
			 * */
			if(fileLength < chunkLength) {
				byte[] dataBuffer = new byte[(int)fileLength];
				fis.read(dataBuffer);

				ByteString bs = ByteString.copyFrom(dataBuffer);
				FileDataInfo.Builder fd = FileDataInfo.newBuilder();
				Path p = Paths.get(filepath);
				String fname = p.getFileName().toString();

				fd.setFilename(fname);
				fd.setData(bs);
				fd.setChunkblockid(0);
				fd.setTotalchunks(1); //only one chunk is required.
				rb.setData(fd);
				CommConnection.getInstance().enqueue(rb.build());
			}
			else {
				int count = 0;
				byte[] dataBuffer = new byte[(int)chunkLength];
				int bytesread = 0;
				long totalChunks = (long)(fileLength/chunkLength);
				int extraChunk = 0;
				if(fileLength%chunkLength > 0) {
					extraChunk = 1;
				}
				totalChunks = totalChunks + extraChunk;
				BufferedInputStream in = new BufferedInputStream(fis);
				
				/*To make sure that we don't send extra bytes while sending the chunk
				 * First send full chunks and then send one of the remaining bytes chunk
				 * */
				
				while(count < (totalChunks-1)) {
					bytesread = in.read(dataBuffer);
					ByteString bs = ByteString.copyFrom(dataBuffer);						
					FileDataInfo.Builder fd = FileDataInfo.newBuilder();
					Path p = Paths.get(filepath);
					String fname = p.getFileName().toString();
					System.out.println(fname);
					count++;
					fd.setFilename(fname);
					fd.setData(bs);
					fd.setChunkblockid(count);
					fd.setTotalchunks(totalChunks); //adding how many chunks are required.
					rb.setData(fd);
					CommConnection.getInstance().enqueue(rb.build());
				}
				int extraChunkLength = (int)(fileLength%chunkLength);
				dataBuffer = new byte[extraChunkLength];
				bytesread = in.read(dataBuffer);
				ByteString bs = ByteString.copyFrom(dataBuffer);						
				FileDataInfo.Builder fd = FileDataInfo.newBuilder();
				Path p = Paths.get(filepath);
				String fname = p.getFileName().toString();
				System.out.println(fname);
				count++;
				fd.setFilename(fname);
				fd.setData(bs);
				fd.setChunkblockid(count);
				fd.setTotalchunks(totalChunks); //adding how many chunks are required.
				rb.setData(fd);
				CommConnection.getInstance().enqueue(rb.build());				
			}
			fis.close();			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void release() {
		CommConnection.getInstance().release();
	}

	/**
	 * Since the service/server is asychronous we need a unique ID to associate
	 * our requests with the server's reply
	 * 
	 * @return
	 */
	private synchronized long nextId() {
		return ++curID;
	}
}
