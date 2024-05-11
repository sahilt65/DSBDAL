**How To Run Mapreduce in cloudera**

New Java Project
Add External Jars -> File Sysystems -> usr -> lib -> hadoop (add all .jar files)
Add External Jars -> File Sysystems -> usr -> lib -> hadoop->client (add all .jar files)
Finish
Right click on src -> new -> class -> FileName(same as Project Name) -> Finish
Right click on project -> Export -> JAVA -> JAR File-> Browse -> Cloudera ->FileName.jar -> ok -> Finish
Terminal : cat > /home/cloudera/Processfile1.txt (Enter inputs, ctr+z to exit )

hdfs dfs -mkdir /inputfolder
hdfs dfs -put /home/cloudera/Processfile1.txt /inputfolder1
hdfs dfs -cat /inputfolder1/Processfile1.txt
hadoop jar /home/cloudera/WordCount.jar WordCount /inputfolder1/Processfile1.txt /out1
hadoop fs -ls /out1
hadoop fs -cat /out1/part-r-00000


Shared Folder Commands

->su root
->mount -t vboxsf WindowsFname ClouderaFpath
