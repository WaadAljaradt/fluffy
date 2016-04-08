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
		
		System.out.println("Sending a file");
		
		File file = new File(filepath);
		long fileLength = file.length(); // File Length
		long chunkLength = 1048576; // sets chunkLength Size
		try {
			int count = 0;
			byte[] dataBuffer = new byte[(int)chunkLength];
			FileInputStream fis = new FileInputStream(file);
			int bytesread = 0;
			int extraChunk = 0;
			BufferedInputStream in = new BufferedInputStream(fis);
			while((bytesread = in.read(dataBuffer)) !=-1) {
				ByteString bs = ByteString.copyFrom(dataBuffer);
				Header.Builder hb = Header.newBuilder();
				hb.setNodeId(999);
				hb.setTime(System.currentTimeMillis());
				hb.setDestination(-1);
				
				CommandMessage.Builder rb = CommandMessage.newBuilder();
				rb.setHeader(hb);
				rb.setPing(true);
				rb.setSave(true);
				rb.setUsername(username);
					
				FileDataInfo.Builder fd = FileDataInfo.newBuilder();
				Path p = Paths.get(filepath);
				String fname = p.getFileName().toString();
				System.out.println(fname);
				count++;
				fd.setFilename(fname);
				fd.setData(bs);
				fd.setChunkblockid(count);
				fd.setFilesize(fileLength);
				if(fileLength%chunkLength > 0) {
					extraChunk = 1;
				}
				fd.setTotalchunks((long)(fileLength/chunkLength)+extraChunk); //adding How many chunks are required.
				rb.setData(fd);
				CommConnection.getInstance().enqueue(rb.build());
				System.out.println("Chunk Counts " + count);
			}
			fis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			// direct no queue
			// CommConnection.getInstance().write(rb.build());

			// using queue
//			CommConnection.getInstance().enqueue(rb.build());
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
