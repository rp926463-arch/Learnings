#!/bin/ksh


rm -rf /var/tmp/hdfs

if [ $Env = "dev" ]
then
	export HADOOP_HOME=/opt/cloudera/parcels/cdh/lib/hadoop-mapreduce
	export HADOOP_CONF_DIR=/path where files like hdfs-site.xml, yarn-site.xml, mapred-site.xml, core-site.xml reside
elif [ $Env = "qa" ]
then
	export HADOOP_HOME=/opt/cloudera/parcels/cdh/lib/hadoop-mapreduce
	export HADOOP_CONF_DIR=/path where files like hdfs-site.xml, yarn-site.xml, mapred-site.xml, core-site.xml reside
else
	echo "Environment varibale $Env mentioned is not present in the choice, skipping process of token creation"
	exit 0
fi

token_path="/var/tmp/hdfs"
mkdir -p $token_path
toen_fqn=$token_path/token
cd $token_path
unset HADOOP_TOKEN_FILE_LOCATION
/opt/cloudera/parcels/cdh/bin/hdfs fetchdt --renewer hdfs token

if [-e /var/tmp/hdfs/token ]
then
	echo "Token where successfully created for ${USER} in ${Env} environment at path ${token_path}"
else
	echo "Token creation failed for ${USER} in ${Env} environment, terminating the process"
	exit 1
fi

if [ $Env = "dev" ]
then
	cp /var/tmp/hdfs/token /some/permenent_path_to_token
	if [ -e /some/permenent_path_to_token ]
	then
		echo "Initiating token clean up on /var/tmp/"
		rm -rf /var/tmp/hdfs
		echo "Token cleanup completed successfully for {$USER} in {$Env} environment"
		exit 0
	else
		echo "Tokens are not created for ${USER} in ${Env} environment, please rech out"
		exit 1
fi