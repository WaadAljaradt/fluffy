# fluffy
Introduction:

The aim of the project is to design and build the File Edge architecture. that include a scalability strategy to account for a massive registration processes, distributed image storage, and search capabilities that include real-time monitoring of work and data as they enter the network of servers. 
Why are we investigating this kind of systems ? because storing data in the cloud or on a network of servers share a common challenges such as how to securely share, store, and survive failures and corruption of data many of these system are popular systems in the Industry market such as Drop Box, Sync, Google Drive, and server installations like HDFS and databases like Mongo that use multiple servers and redundant storage approaches.
The project aims to provide a flexible network supporting the Intra and Inter cluster interactions. Communication between nodes is being handled that arise many issues to be addressed in this project such as the discovery of other nodes and communication amongst themselves and sharing data between themselves. Each cluster follows a specific network topology on the basis of implementation for example ring, mesh etc., Along with this each cluster needs to take care of issues like node/leader failure, leader election, failed node recovery.
In this report, a detailed explanation is provided for the cluster’s topology used, leader election strategy, cluster functionalities, failure recovery and many other issues.

2. Technologies Used:
a) Languages : Java
b) Packages : Netty
c) Storage : Cassandra 
d) Other supporting technologies : i. Apache Ant : For building the source files and making the code executable 
ii. Google Protobuf : For defining a common interface for interaction between servers and also between client and server



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

