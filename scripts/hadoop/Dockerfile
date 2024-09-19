FROM fedora:40

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
export HDFS_NAMENODE_USER="root"
export HDFS_DATANODE_USER="root"
export HDFS_SECONDARYNAMENODE_USER="root"
export YARN_RESOURCEMANAGER_USER="root"
export YARN_NODEMANAGER_USER="root"
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
COPY hdfs-init.sh /hdfs-init.sh
RUN chmod +x /hdfs-init.sh
CMD ["/hdfs-init.sh"]
