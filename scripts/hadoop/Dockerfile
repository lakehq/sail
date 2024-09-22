FROM --platform=linux/amd64 fedora:40

# Install Java
RUN dnf -y update && \
    dnf -y install java-17-openjdk-devel hostname openssh-server openssh-clients rsync ncurses

# Install Hadoop
ENV HDP_VER 3.4.0
ADD https://downloads.apache.org/hadoop/common/hadoop-"$HDP_VER"/hadoop-"$HDP_VER".tar.gz /opt/
RUN tar -xzvf /opt/hadoop-"$HDP_VER".tar.gz -C /opt/ && \
    rm /opt/hadoop-"$HDP_VER".tar.gz && \
    ln -s /opt/hadoop-"$HDP_VER" /opt/hadoop

# Set environment variables
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk

ENV HADOOP_HOME /opt/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
RUN cat >> /opt/hadoop/etc/hadoop/hadoop-env.sh <<EOF
export JAVA_HOME=$JAVA_HOME
export HDFS_NAMENODE_USER="$HADDOP_USER_NAME"
export HDFS_DATANODE_USER="$HADOOP_USER_NAME"
export HDFS_SECONDARYNAMENODE_USER="$HADOOP_USER_NAME"
export YARN_RESOURCEMANAGER_USER="$HADOOP_USER_NAME"
export YARN_NODEMANAGER_USER="$HADOOP_USER_NAME"
EOF
RUN cat > /opt/hadoop/etc/hadoop/core-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://0.0.0.0:9000</value>
    </property>
</configuration>
EOF
RUN cat > /opt/hadoop/etc/hadoop/hdfs-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
EOF
RUN cat > /hdfs-init.sh <<EOF
#!/bin/bash
# hadoop needs to ssh even in pseudo-distributed mode
# this is for the container to ssh into itself
ssh-keygen -A
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# start the ssh server
/usr/sbin/sshd

/opt/hadoop/bin/hdfs namenode -format
/opt/hadoop/sbin/start-dfs.sh
echo "initializing dfs content"
/opt/hadoop/bin/hdfs dfs -mkdir /user
/opt/hadoop/bin/hdfs dfs -mkdir /user/root


echo '{ "name": "Bob" }' > /tmp/test.json
/opt/hadoop/bin/hdfs dfs -put /tmp/test.json /user/root/test.json
EOF
#COPY hdfs-init.sh /hdfs-init.sh
RUN chmod +x /hdfs-init.sh
CMD ["/hdfs-init.sh"]
