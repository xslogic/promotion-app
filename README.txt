This archive contains:

/crapp			the demo application together with the script files that are executed by the container started by the application master
demo-setup.sh		the script file to compile and copy the necessary application files to HDFS
demo-start.sh		the script to launch the demo application
YARN-4876.003.patch	the YARN patch containing the container restart capability



*** PREREQUISITES ***

before DEPLOYING the application:
1) apply the YARN-4876.003.patch to trunk
2) run “mvn install” to ensure that the 3.0.0-SNAPSHOT version of YARN is installed in the local maven repository and updated with the patch
3) start YARN + HDFS



*** DEPLOYING DEMO APPLICATION ***

To compile the application AND copy the necessary application demo files to HDFS, run the following command (this assumes that the “hdfs” and “mvn” commands are in your PATH):

./demo-setup.sh

If you have issues in compiling the application, you can copy the application JAR from: https://dl.dropboxusercontent.com/u/46901765/YARN-4876/crapp-1.0.0.jar and paste it into the crapp/target folder. After that, in order to copy the application demo files to HDFS run the following command:
 
./demo-setup.sh no-compile



*** RUN DEMO APPLICATION ***

the execute the demo application simply run the following command (this assumes that the “yarn” command is in your PATH):

./demo-start.sh

The application master starts a container that runs crapp/script/script0.sh. After a given time, the container resources are relocalized and the container is restarted using crapp/script/script1.sh.
While the application is running, check the logs of container 2 to verify that the container resources are relocalized and the container is restarted.


