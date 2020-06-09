package server;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.SSHConnection;

public class CassandraServer {
	
	private static Logger logger = LoggerFactory.getLogger(CassandraServer.class);
	
	public void initAWS(SSHConnection sshConnection, String cassandraCommit, String privateIP, String... nodes) {
		ArrayList<String> commands = new ArrayList<String>();
		
		//stop running cassandra processes
		commands.add("ps ax | grep java | grep -v 'grep' | cut -d '?' -f1 | xargs kill -9");
		
		//remove old stuff
		commands.add("sudo rm -rf /tmp");
		commands.add("sudo rm -f /home/ec2-user/byteman*");
		commands.add("sudo rm -rf /home/ec2-user/.ant");			
		commands.add("sudo mkdir /tmp");
		commands.add("sudo chmod 777 /tmp");
		
		
		//make updates
		commands.add("sudo yum update -y");
		
		//upgrade JAVA
		commands.add("sudo yum install java-1.8.0 -y");
		commands.add("sudo yum remove java-1.7.0-openjdk -y");
		commands.add("sudo yum install java-devel -y");
		
		//download ant
		commands.add("wget https://www-eu.apache.org/dist//ant/binaries/apache-ant-1.10.5-bin.tar.gz -P /tmp/");
		commands.add("tar -zxvf /tmp/apache-ant-1.10.5-bin.tar.gz -C /tmp/");
		commands.add("rm /tmp/apache-ant-1.10.5-bin.tar.gz");
		commands.add("mv /tmp/apache-ant-1.10.5 /tmp/ant");
		commands.add("/tmp/ant/bin/ant -f /tmp/ant/fetch.xml -Ddest=system");
		
		//install git
		commands.add("sudo yum install git-all -y");
				
		//Download Cassandra
		commands.add("git clone -b trunk https://github.com/apache/cassandra.git /tmp/cassandra &> /dev/null");
		
		for (String cmd : commands) {
			String r =sshConnection.runCommand(cmd);
			if (!cmd.contains("tar ")) {
				logger.debug(r);
			}
		}
		commands.clear();
		
		//go to commit xy
		if(cassandraCommit != null){
			commands.add("cd /tmp/cassandra/ && git reset --hard " + cassandraCommit);
		}
		//add libaries to ant
		commands.add("cp /tmp/cassandra/lib/* /tmp/ant/lib");
		
		//configure Cassandra
		String thisNode = nodes[0];
		commands.add("sed -i '/^rpc_address: /d' /tmp/cassandra/conf/cassandra.yaml");
		commands.add("sed -i '/^listen_address: /d' /tmp/cassandra/conf/cassandra.yaml");
		
		commands.add("echo -e \"listen_address: " + privateIP + "\n"
				+ "broadcast_rpc_address: " + thisNode + "\n"
				+ "rpc_address: 0.0.0.0\" >> /tmp/cassandra/conf/cassandra.yaml");
		//commands.add("echo -e \"listen_address: localhost\n"
		//		+ "broadcast_rpc_address: localhost\n"
		//		+ "rpc_address: 0.0.0.0\" >> /tmp/cassandra/conf/cassandra.yaml");
		String old = "127.0.0.1";
		String replacement = "";
		for (String other : nodes) {
			replacement += "," + other; 
		}
		replacement = replacement.substring(1);
		commands.add("sed -i 's/" + old + "/" + replacement + "/g' /tmp/cassandra/conf/cassandra.yaml");
		commands.add("sed -i 's/endpoint_snitch: SimpleSnitch/endpoint_snitch: GossipingPropertyFileSnitch/g' /tmp/cassandra/conf/cassandra.yaml");
		commands.add("echo -e \"auto_bootstrap: false\" >> /tmp/cassandra/conf/cassandra.yaml");
		
		for (String cmd : commands) {
			String r =sshConnection.runCommand(cmd);
			logger.debug(r);
		}
		commands.clear();
		
		//Build Cassandra
		String result = sshConnection.runCommand("cd /tmp/cassandra/ && /tmp/ant/bin/ant _main-jar");

		logger.debug(result);
		if(!result.contains("BUILD SUCCESSFUL")){
			logger.warn("Cassandra build failed, try again with default target");			
			result = sshConnection.runCommand("cd /tmp/cassandra/ && /tmp/ant/bin/ant");
			logger.debug(result);
			//if the build fails replace the byteman-install (only works after building once).
			if(!result.contains("BUILD SUCCESSFUL")){
				logger.warn("Cassandra build failed again, try again with other byteman");
				commands.add("wget http://downloads.jboss.org/byteman/3.0.3/byteman-download-3.0.3-bin.zip");
				commands.add("unzip byteman-download-3.0.3-bin.zip -d /tmp");
				commands.add("rm -f /tmp/cassandra/build/lib/jars/byteman-install-3.0.3.jar");
				commands.add("cp /tmp/byteman-download-3.0.3/lib/byteman-install.jar /tmp/cassandra/build/lib/jars/byteman-install-3.0.3.jar");
				
				for (String cmd : commands) {
					String r =sshConnection.runCommand(cmd);
					logger.debug(r);
				}
				commands.clear();
				result = sshConnection.runCommand("cd /tmp/cassandra/ && /tmp/ant/bin/ant");
				logger.debug(result);
				if(!result.contains("BUILD SUCCESSFUL")){
					logger.error("Unable to build cassandra :(, commit:" + cassandraCommit);
				}
			}
		}
		
		//close possible running java processes
		commands.add("ps ax | grep java | grep -v 'grep' | cut -d '?' -f1 | xargs kill -9");
		
		//Start Cassandra
		commands.add("/tmp/cassandra/bin/cassandra");
		
		for (String cmd : commands) {
			String r =sshConnection.runCommand(cmd);
			logger.debug(r);
		}
		logger.info("Cassandra initalized, commit: " + cassandraCommit);
	}

}
