package gash.router.server;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.commons.io.FileUtils;
public class CassandraDAO
{
    private Cluster cluster;
    private Session session;
   /* private String NODE = ABCServiceTester.Properties.Settings.Default.CASSANDRA_NODE;
    private String USER = ABCServiceTester.Properties.Settings.Default.USERNAME;
    private String PASS = ABCServiceTester.Properties.Settings.Default.PASSWORD;
    */

    public CassandraDAO()
    {
        connect();
    }
    /*CREATE KEYSPACE files WITH replication = {
      'class': 'SimpleStrategy',
      'replication_factor': '1'
    };

<<<<<<< HEAD
CREATE TABLE files ( filename text, file blob,  seq_id int , timeStamp double ,  PRIMARY KEY (filename,timeStamp));

you have to drop your table to add the new primary key and create it again
 * 
 */
    private void connect()
    {
       cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("files");
    }

    protected Session getSession()
    {
        if (session == null)
        {
            connect();
        }
        return session;
    }
    
    public Long getLatestTimeStamp(){
    	Statement statement = new SimpleStatement("select MAX(timestamp) as a from files");
    	Row resutls = session.execute( statement ).one();
    	Long latest = (long) resutls.getDouble("a");
        return latest;	
    }
    
    public ResultSet getLatestRecords(long timestamp){
    	Statement statement = new SimpleStatement("select fileName, file, seq_id , timestamp from files where timestamp > "+timestamp+" ALLOW FILTERING");
    	ResultSet resutls = session.execute( statement );
        return resutls;	
    }
    
    public ResultSet insert(String filename, ByteBuffer byteBuffer, int seq_id, long timeStamp)
    {
        Statement insertFile = QueryBuilder.insertInto( "files" ).value( "filename", filename ).value( "file", byteBuffer ).value("seq_id", seq_id).value("timeStamp", timeStamp);
        ResultSet resutls = session.execute( insertFile );
        return resutls;
        //	Similar to session.execute("INSERT INTO users (key,value) VALUES ('"+key+"', '"+value+"')");
    }

    public Row get(String filename)
    {
        Statement readFile = QueryBuilder.select( "file" ).from( "files" ).where( QueryBuilder.eq( "filename", filename ) );
        Row fileRow = session.execute( readFile ).one();
        return fileRow;
    }

    // Return matching filename chunks from database
    public ResultSet getMatchingFiles(String filename) {
    	Statement readFile = new SimpleStatement("select filename, file, seq_id from files where filename = '" + filename +"'");
    	ResultSet results= session.execute(readFile);
    	
    	return results;
    }
    
    //Return counts of file chunk of a particular file
    public long getFileCount(String filename) {
    	Statement readFile = new SimpleStatement("select count(*) as c from files where filename = '" + filename +"'");
    	Row results= session.execute(readFile).one();
    	
    	return results.getLong("c");
    }

        public static void main(String[] args) {
        	CassandraDAO dao = new CassandraDAO();
        	File file = new File("/Users/waadjaradat/Documents/workspaceNetty/fluffy/runtime/route-1.conf");
        	ByteBuffer fileByteBuffer;
			try {
				fileByteBuffer = ByteBuffer.wrap( FileUtils.readFileToByteArray( file) );
			
				dao.insert("test", fileByteBuffer,1, System.currentTimeMillis());
				long latest = dao.getLatestTimeStamp();
				
        	
        	ResultSet results = dao.getLatestRecords(latest);
				for (Row r : results){
					System.out.println(r.getInt("seq_id"));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
 	
/*	Cluster cluster;
	Session session;
	
	cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
	session = cluster.connect("demo_1459794486_840f3099bc5bd51b5f41126590bb9636");
	session.execute("INSERT INTO users (lastname, age, city, email, firstname) VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob')");
	ResultSet results = session.execute("SELECT * FROM users WHERE lastname='Jones'");
	for (Row row : results) {
	System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
	}*/

    
}