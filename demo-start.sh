HDFS_DIR=hdfs:///user/YARN-4876-demo

yarn jar crapp/target/crapp-1.0.0.jar com.polimi.crapp.Client $HDFS_DIR/script0.sh $HDFS_DIR/script1.sh $HDFS_DIR/crapp-1.0.0.jar 

