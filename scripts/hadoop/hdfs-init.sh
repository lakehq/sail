
#!/usr/bin/bash

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
/opt/hadoop/bin/hdfs dfs -mkdir /user
/opt/hadoop/bin/hdfs dfs -mkdir /user/root


echo '{ "name": "Bob" }' > /tmp/test.json
/opt/hadoop/bin/hdfs dfs -put /tmp/test.json /user/root/test.json
