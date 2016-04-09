#!/bin/bash
#
# This script is used to start the server from a supplied config file
#

export SVR_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "** starting from ${SVR_HOME} **"

echo server home = $SVR_HOME
#exit

#cd ${SVR_HOME}

JAVA_MAIN='gash.router.app.DemoApp'
#Uncomment either of JAVA_ARGS in "runPing.sh" to run client in upload or download mode. Only one operation can be performed at a time. 
#And then close your client.

#Arguments for download. It has four arguments: username download filenametobedownloaded directorypathWhereFileIsDownloaded. 
#Uncomment and change  username, directorypath and filenametobedownloaded by your arguments
#JAVA_ARGS="vishv download mbuntu-0.jpg /home/vishv/Desktop/"

#Arguments for upload.It has three arguments: username upload filepath. 
#Uncomment and change username by your username and filepath with your arguments.
#JAVA_ARGS="vishv upload /home/vishv/Pictures/mbuntu-0.jpg"

#echo -e "\n** config: ${JAVA_ARGS} **\n"

# superceded by http://www.oracle.com/technetwork/java/tuning-139912.html
JAVA_TUNE='-client -Djava.net.preferIPv4Stack=true'


java ${JAVA_TUNE} -cp .:${SVR_HOME}/lib/'*':${SVR_HOME}/classes ${JAVA_MAIN} ${JAVA_ARGS} 
