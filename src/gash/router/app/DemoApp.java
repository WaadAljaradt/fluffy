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
package gash.router.app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import com.google.protobuf.ByteString;

import gash.router.client.CommConnection;
import gash.router.client.CommListener;
import gash.router.client.MessageClient;
import pipe.common.Common.Header;
import routing.Pipe.CommandMessage;

public class DemoApp implements CommListener {
	private MessageClient mc;
	private TreeMap<Integer, ByteString> tm = new TreeMap<Integer, ByteString>();
	public static String fileDownloadPath;
	
	public DemoApp(MessageClient mc) {
		init(mc);
	}

	private void init(MessageClient mc) {
		this.mc = mc;
		this.mc.addListener(this);
	}

	private void ping(String filepath, String username) {
		// test round-trip overhead (note overhead for initial connection)
		mc.uploadFile(filepath, username);
	}
	
	private void download(String filename) {
		mc.downloadFile(filename);
	}

	@Override
	public String getListenerID() {
		return "demo";
	}

	@Override
	public void onMessage(CommandMessage msg) {
		
//		if(msg.hasMessage()) {
//			System.out.println("---> " + msg.getMessage());
//		}
		/* Logic to handle file retrieval on client side
		 * Assemble all the chunks on client side
		 * And then pass it on to FileOutputStream to create a new file
		 * */
		if(msg.hasRetrieve()) {
			if (msg.hasData()) {
				if (msg.getData().hasFilename()) {
					
					tm.put((int)msg.getData().getChunkblockid(), msg.getData().getData());
					if(tm.size() == msg.getData().getTotalchunks()) {
						File file = new File(fileDownloadPath + msg.getData().getFilename());
						try {
							FileOutputStream fout = new FileOutputStream(file, true);
							for(Map.Entry<Integer, ByteString> entry: tm.entrySet()) {
								byte[] data = entry.getValue().toByteArray();
								fout.write(data);
							}
							fout.close();
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						finally {
							tm.clear();							
						}						
					}
				}
			}			
		}
	}

	/**
	 * sample application (client) use of our messaging service
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String host = "127.0.0.1";
		int port = 4568;

		try {
			MessageClient mc = new MessageClient(host, port);
			DemoApp da = new DemoApp(mc);

			// do stuff w/ the connection
			if(args[1].equals("upload")) {
				da.ping(args[2], args[0]);				
			}

			if(args[1].equals("download")) {
				fileDownloadPath = args[3];
				da.download(args[2]);	
			}
			System.out.println("\n** exiting in 10 seconds. **");
			System.out.flush();
//			Thread.sleep(10 * 1000);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
//			CommConnection.getInstance().release();
		}
	}
}
