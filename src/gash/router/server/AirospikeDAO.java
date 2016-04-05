package gash.router.server;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class AirospikeDAO {
	private AerospikeClient client;
	private String seedHost ="127.0.0.1";
	private Integer port =3000;
	ClientPolicy cPolicy;
	WritePolicy wPolicy;
	Key key;
	 public AirospikeDAO()
	    {
	        connect();
	    }
	 
	
	 private void connect()
	    {
	 this.cPolicy = new ClientPolicy();
	cPolicy.timeout = 500;
	try {
		this.client = new AerospikeClient(cPolicy, this.seedHost, this.port);
		//this.client =  new AerospikeClient("192.168.1.150", 3000);
		
	} catch (AerospikeException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	    } 
	 
	 
	 public void insert (String fileName , byte[] file){
		 this.wPolicy  = new WritePolicy();
			this.wPolicy.recordExistsAction = RecordExistsAction.UPDATE;
		 try {
			
			 Key key = new Key("test", "cmpe275", fileName);
	            Bin bin1 = new Bin("fileName", fileName);
	            Bin bin2 = new Bin("file", file);
	            this.client.put(wPolicy, key, bin1,bin2);
		} catch (AerospikeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	 }
	 public Object get (String fileName ){
		
		 Key key;
		try {
			key = new Key("test", "cmpe275", fileName);
			Record record = client.get(null, key);
			return record.getValue("file");
		} catch (AerospikeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 client.close();
		 return null;
		 
	 }
	 
	 public static void main(String[] args) throws IOException { 
	 //AerospikeClient client = new AerospikeClient("192.168.1.150", 3000);
		AirospikeDAO dao = new AirospikeDAO();
		File file = new File("/Users/waadjaradat/Documents/workspaceNetty/fluffy/runtime/route-1.conf");
		byte[] bytes = FileUtils.readFileToByteArray( file);
		dao.insert("testFileName", bytes);
		Object obj = dao.get("testFileName");
		System.out.println(obj.toString());
	 
		
	 }
}
