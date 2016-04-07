package gash.router.server;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
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

    CREATE TABLE files ( filename text, file blob,  seq_id int , timeStamp double ,  PRIMARY KEY (filename,seq_id));
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
    public ResultSet insert(String filename, ByteBuffer byteBuffer, int seq_id, long timeStamp)
    {
        //connect();
        //	ByteBuffer fileByteBuffer = ByteBuffer.wrap( readFileToByteArray( filename ) );
        Statement insertFile = QueryBuilder.insertInto( "files" ).value( "filename", filename ).value( "file", byteBuffer ).value("seq_id", seq_id).value("timeStamp", timeStamp);
        ResultSet resutls = session.execute( insertFile );
        return resutls;

        //	session.execute("INSERT INTO users (key,value) VALUES ('"+key+"', '"+value+"')");
    }

    public Row get(String filename)
    {
        Statement readFile = QueryBuilder.select( "file" ).from( "files" ).where( QueryBuilder.eq( "filename", filename ) );
        Row fileRow = session.execute( readFile ).one();
 /*   if ( fileRow != null ) {
        ByteBuffer fileBytes = fileRow.getBytes( "file" );
        File f = convertToFile( fileBytes );
    }
    	connect();
  ResultSet results = session.execute("SELECT * FROM users WHERE key='"+key+"'");
  */
        return fileRow;
    }



    public static void main(String[] args) {
        CassandraDAO dao = new CassandraDAO();
        File file = new File("/Users/waadjaradat/Documents/workspaceNetty/fluffy/runtime/route-1.conf");
        ByteBuffer fileByteBuffer;
        try {
            fileByteBuffer = ByteBuffer.wrap( FileUtils.readFileToByteArray( file) );
            dao.insert("test", fileByteBuffer,1, System.currentTimeMillis());
            Row fileRow = dao.get("test");
            if ( fileRow != null ) {
                //  ByteBuffer fileBytes = fileRow.getDouble("timeStamp");
                double time =  fileRow.getDouble("timestamp");
                System.out.println(time);
                // File f = convertToFile( fileBytes );
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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
}