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
ARG HADOOP_USER_NAME=root
ENV HADOOP_USER_NAME $HADOOP_USER_NAME
ENV HADOOP_HOME /opt/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
COPY hdfs-init.sh /usr/bin/hdfs-init.sh
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
RUN chmod +x /usr/bin/hdfs-init.sh
CMD ["hdfs-init.sh"]
