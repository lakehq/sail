#!/usr/bin/env bash

# hadoop needs to ssh even in pseudo-distributed mode
# this is for the container to ssh into itself
ssh-keygen -A
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# start the ssh server
/usr/sbin/sshd

# setup the hadoop environment
cat >> /opt/hadoop/etc/hadoop/hadoop-env.sh <<EOF
export JAVA_HOME=$JAVA_HOME
export HDFS_NAMENODE_USER="$HADOOP_USER_NAME"
export HDFS_DATANODE_USER="$HADOOP_USER_NAME"
export HDFS_SECONDARYNAMENODE_USER="$HADOOP_USER_NAME"
export YARN_RESOURCEMANAGER_USER="$HADOOP_USER_NAME"
export YARN_NODEMANAGER_USER="$HADOOP_USER_NAME"
EOF

# start hadoop in pseudo-distributed mode
/opt/hadoop/bin/hdfs namenode -format
/opt/hadoop/sbin/start-dfs.sh

# create the necessary directories and files for testing
/opt/hadoop/bin/hdfs dfs -mkdir /user
/opt/hadoop/bin/hdfs dfs -mkdir /user/$HADOOP_USER_NAME
[ "${HADOOP_USER_NAME}" != "root" ] && /opt/hadoop/bin/hdfs dfs -chown $HADOOP_USER_NAME /user/$HADOOP_USER_NAME
/opt/hadoop/bin/hdfs dfs -chmod +w /user/$HADOOP_USER_NAME

echo '{ "name": "Bob" }' > /tmp/test.json
/opt/hadoop/bin/hdfs dfs -put /tmp/test.json /user/$HADOOP_USER_NAME/test.json

# keep the container running
tail -f /dev/null
