# fluffy
Team project for CMPE275.

#Setting up Cassandra Database
Create a new database named Files by running following query in command line tool of cassandra.

CREATE KEYSPACE files WITH replication = {
      'class': 'SimpleStrategy',
      'replication_factor': '1'
    };
    
Run the following query after entering in to the data base through cassandra command line tool.

CREATE TABLE files ( filename text, file blob,  seq_id int , timeStamp double ,  PRIMARY KEY (filename,timeStamp));



#To build proto files
Change proto home PROTOC_HOME=/usr/local/bin/protoc in the "build_pb.sh"
Run "build_pb.sh"

#Starting Server
To run "startServer.sh" and pass conf file path. Make sure that server's commandPort is running on 4568 port.
Change the appropriate commandPort to be 4568 

#Starting Client
To run Client, you have to run "runPing.sh". Before running shell file, kindly make following changes in "runPing.sh"
#Uncomment either of JAVA_ARGS in "runPing.sh" to run client in upload or download mode. Only one operation can be performed at a time. 
#And then close your client.

#Arguments for download. It has four arguments: username download filenametobedownloaded directorypath. 
#Uncomment and change username by your username and filepath with your arguments.
#JAVA_ARGS="vishv download mbuntu-0.jpg /home/vishv/Desktop/"

#Arguments for upload.It has three arguments: username upload filepath. 
#Uncomment and change  username, directorypath and filenametobedownloaded by your arguments
#JAVA_ARGS="vishv upload /home/vishv/Pictures/mbuntu-0.jpg"

