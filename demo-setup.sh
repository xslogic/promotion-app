compile=1

if [ "$1" == "no-compile" ]; then
	compile=0
fi

if [[ $compile == 1 ]]; then

	echo "****************************"
	echo "COMPILING DEMO APPLICATION"
	echo "****************************"

	mvn -f crapp/pom.xml install

	rc=$?
	if [[ $rc != 0 ]]; then
		echo "Compilation failed, check maven output or either skip compilation with option \"no-compile\"."
		exit $rc
	fi
fi

echo "****************************"
echo "COPYING DEMO FILES TO HDFS"
echo "****************************"

HDFS_DIR="/user/YARN-4876-demo"

hdfs dfs -mkdir -p "$HDFS_DIR"

rc=$?
if [[ $rc != 0 ]]; then
	echo "Unable to create directory: \"$HDFS_DIR\" on HDFS"
	exit $rc
fi


hdfs dfs -copyFromLocal -f crapp/target/crapp-1.0.0.jar $HDFS_DIR/crapp-1.0.0.jar

rc=$?
if [[ $rc != 0 ]]; then
	echo "Unable to copy application JAR to HDFS."
	exit $rc
fi


hdfs dfs -copyFromLocal -f crapp/scripts/script0.sh $HDFS_DIR/script0.sh

rc=$?
if [[ $rc != 0 ]]; then
	echo "Unable to copy script0 to HDFS."
	exit $rc
fi


hdfs dfs -copyFromLocal -f crapp/scripts/script1.sh $HDFS_DIR/script1.sh

rc=$?
if [[ $rc != 0 ]]; then
	echo "Unable to copy script1 to HDFS."
	exit $rc
fi


