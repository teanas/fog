package server;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import manager.Benchmark;
import manager.Pair;
import manager.S3Upload;
import util.SSHConnection;

public class YCSBClient {
	
	private static Logger logger = LoggerFactory.getLogger(YCSBClient.class);
	private Pair cassandraCommit;
	private String[] cassandraHosts;
	private int fieldCount;
	
	public YCSBClient(Pair commit, int fieldCount, String... hosts) {
		this.cassandraCommit = commit;
		this.cassandraHosts = hosts;
		this.fieldCount = fieldCount;
	}
	
	public void init(SSHConnection sshConnection) {
		ArrayList<String> commands = new ArrayList<String>();
		
		//remove old stuff
		commands.add("sudo rm -rf /tmp");
		commands.add("sudo rm -rf /home/ec2-user/.ant");
		commands.add("sudo mkdir /tmp");
		commands.add("sudo chmod 777 /tmp");
		
		//make updates
		commands.add("sudo yum update -y");
		
		//upgrade JAVA
		commands.add("sudo yum install java-1.8.0 -y");
		commands.add("sudo yum remove java-1.7.0-openjdk -y");
		commands.add("sudo yum install java-devel -y");
		
		//download ycsb
		commands.add("wget https://github.com/brianfrankcooper/YCSB/releases/download/0.15.0/ycsb-0.15.0.tar.gz -P /tmp/");
		commands.add("tar xfvz /tmp/ycsb-0.15.0.tar.gz -C /tmp/");
		commands.add("rm /tmp/ycsb-0.15.0.tar.gz");
		commands.add("mv /tmp/ycsb-0.15.0 /tmp/ycsb");
		
		//install git
		commands.add("sudo yum install git-all -y");
		//download Cassandra
		commands.add("git clone -b trunk https://github.com/apache/cassandra.git /tmp/cassandra &> /dev/null");
		
		for (String cmd : commands) {
			String r =sshConnection.runCommand(cmd);
			if (!cmd.contains("tar ")) {
				logger.debug(r);
			}
		}
		logger.info("YCSB initialized, commit is " + cassandraCommit.getCommit());
	}
	
	public long[] runTest(SSHConnection sshConnection, long recordCount, long operationCount, int threads, String path, String bucketName) {
		
		long[] times = new long[2];		
		
		//create output directory
		File theDir = new File(path.substring(0, path.lastIndexOf(File.separatorChar)));
		if(!theDir.exists()){
			theDir.mkdirs();
		}
		
		//Init CassandraDB
		String fields = IntStream.range(0, this.fieldCount).mapToObj(x -> "field" + x + " varchar").collect(Collectors.joining(", "));
		String r = sshConnection.runCommandToConsole("/tmp/cassandra/bin/cqlsh " + this.cassandraHosts[0] + " -e \""
				+ "CREATE SCHEMA ycsb WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };"
				+ "USE ycsb;"
				+ "CREATE TABLE usertable (y_id varchar primary key, " + fields + ")\"");
		logger.debug(r);
		ArrayList<String> props = new ArrayList<String>();
		props.add("recordcount=" + recordCount);
		props.add("threads=" + threads);
		props.add("fieldcount=" + fieldCount);
		props.add("fieldlength=100");//100 default
		String properties = props.stream().map(x-> "-p " + x).collect(Collectors.joining(" "));
		
		times[0] = System.currentTimeMillis();
		
		//init CassandraDB
		String hosts = "";
		for (String h : cassandraHosts) {
			hosts += "," + h;
		}
		hosts = hosts.substring(1);
		sshConnection.runCommandDoNothing("/tmp/ycsb/bin/ycsb load cassandra-cql -p hosts=\"" + hosts + "\" -P /tmp/ycsb/workloads/workloada -s " + properties +" > /tmp/result_load.ycsb");
		times[0] = System.currentTimeMillis() - times[0];
				
		props = new ArrayList<String>();
		props.add("recordcount=" + recordCount);
		props.add("threads=" + threads);
		props.add("operationcount="+operationCount);
		props.add("fieldlength=100");//100 default
		properties = props.stream().map(x-> "-p " + x).collect(Collectors.joining(" "));
		
		times[1] = System.currentTimeMillis();
		
		//run benchmark
		sshConnection.runCommandDoNothing("/tmp/ycsb/bin/ycsb run cassandra-cql -p hosts=\"" + hosts + "\" -P /tmp/ycsb/workloads/workloada -s " + properties +" > /tmp/result_run.ycsb");
		
		times[1] = System.currentTimeMillis() - times[1];
		
		String loadFile = path + "result_load_" + cassandraCommit.getCommit() + ".ycsb";
		sshConnection.copyFile("/tmp/result_load.ycsb", loadFile);
		checkValidFiles(loadFile);
		String runFile = path + "result_run_" + cassandraCommit.getCommit() + ".ycsb";
		sshConnection.copyFile("/tmp/result_run.ycsb", runFile);
		checkValidFiles(runFile);
		
		//Upload to S3 bucket
		String key = Benchmark.getKey(cassandraCommit.getDate()) + "result_load_" + cassandraCommit.getCommit() + ".ycsb";
		S3Upload upload = new S3Upload(new File(loadFile), bucketName, key);
		upload.upload();
		
		key = Benchmark.getKey(cassandraCommit.getDate()) + "result_run_" + cassandraCommit.getCommit() + ".ycsb";
		upload = new S3Upload(new File(runFile), bucketName, key);
		upload.upload();
		
		logger.info("YCSB benchmark succeeded");		
		return times;
		
	}
	
	
	public static boolean checkValidFiles(String path) {
		boolean check = true;
		try {
			check = Files.lines(new File(path).toPath()).allMatch(
					x -> {
						boolean succes = true;
						String[] a = null;
						try {
							a = x.split(",");
							String last = a[a.length -1];
							double d = Double.parseDouble(last);
						}catch(Exception e) {
							succes = false;
						}
						return x.startsWith("[") && succes && a != null && a.length == 3;
					}
					);
		} catch (IOException e) {
			System.out.println(e);
			check = false;
		}
		if(!check) {
			logger.error("Error while reading " + path);
		}
		return check;
	}

}
