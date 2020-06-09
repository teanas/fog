package manager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import server.CassandraServer;
import server.YCSBClient;
import util.SSHConnection;

public class Benchmark extends Thread{
	
	private static Logger logger = LoggerFactory.getLogger(Benchmark.class);
	private static String configFileName = "config.json";
	private static JSONObject config;
	private String cassandraCommit = null;
	private Date commitDate = null;
	private boolean done = false;
	
	
	public static Benchmark createBenchmark(Pair cassandraCommitPair) {
		if (!isConfigAvailable())  {
			logger.error("unable to find config, path is " + new File("config.json").getAbsoluteFile());
			return null;
		}
		return new Benchmark(cassandraCommitPair);
	}
	
	public static void initConfig() {
		//Read file with host and login information
		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(new FileReader("config.json"));
	        config = (JSONObject) obj;	        
		} catch (Exception e) {
			logger.error("unable to read config, path is " + new File("config.json").getAbsoluteFile());
		}
	}
	
	private Benchmark(Pair cassandraCommitPair) {
		this.cassandraCommit = cassandraCommitPair.getCommit();
		this.commitDate = cassandraCommitPair.getDate();		
	}
	
	private static boolean isConfigAvailable() {
		return new File(configFileName).exists();
	}
	
	@Override
	public void run() {
		long initTime;
		
		long startTime = System.currentTimeMillis();
				
		if(cassandraCommit != null){
			logger.info("Use cassandra commit: " + cassandraCommit + " (" + commitDate.toString() + ")");
		}
		
		String outputPath = buildPath(commitDate);
		
		if(BenchmarkManager.wasBuildBefore(cassandraCommit, commitDate)) {
			logger.warn(cassandraCommit + " was allready benchmarked.");
			return;
		}
		
		try{			
			long timestamp = System.currentTimeMillis();
			String cassandraHostName1 = (String) config.get("cassandraHost1");
			String privateIPCassandra1 = (String) config.get("cassandraHost1Private");
			String cassandraHostName2 = (String) config.get("cassandraHost2");
			String privateIPCassandra2 = (String) config.get("cassandraHost2Private");
			String ycsbHostName = (String) config.get("ycsbHost");
			String userName = (String) config.get("username");
			String keyFileName = (String) config.get("keyfile");
			
			SSHConnection cassandraServer1 = new SSHConnection(cassandraHostName1, "C1", userName, keyFileName);
			SSHConnection cassandraServer2 = new SSHConnection(cassandraHostName2, "C2", userName, keyFileName);
			SSHConnection ycsbClient= new SSHConnection(ycsbHostName, "Y", userName, keyFileName);
			
			if (cassandraServer1.isConnected() && cassandraServer2.isConnected() && ycsbClient.isConnected()) {
				//Connections established, start initializing benchmark
				
				//Create server objects
				server.CassandraServer c1 = new CassandraServer();
				server.CassandraServer c2 = new CassandraServer();
				YCSBClient ycsb = new YCSBClient(new Pair(cassandraCommit, commitDate), 10, cassandraHostName1, cassandraHostName2);
				
				//initialize cassandra cluster
				Runnable cassandraR1 = () -> c1.initAWS(cassandraServer1, cassandraCommit, privateIPCassandra1, cassandraHostName2, cassandraHostName1);
				Thread cassandra1Thread = new Thread(cassandraR1); 
				cassandra1Thread.start();
				
				Runnable cassandraR2 = () -> c2.initAWS(cassandraServer2, cassandraCommit, privateIPCassandra2, cassandraHostName1, cassandraHostName2);
				Thread cassandra2Thread = new Thread(cassandraR2); 
				cassandra2Thread.start();
				
				//initialize YCSB
				Runnable ycsbR2 = () -> ycsb.init(ycsbClient);
				Thread ycsbThread = new Thread(ycsbR2); 
				ycsbThread.start();
				
				//Wait for initialization
				cassandra1Thread.join();
				cassandra2Thread.join();
				ycsbThread.join();
				
				initTime = System.currentTimeMillis() - startTime;
				
				logger.info("Startup took " + (System.currentTimeMillis()-timestamp)/1000 + "s, commit is " + cassandraCommit);
				timestamp = System.currentTimeMillis();
				
				//execute benchmark
				String bucket = (String) config.get("bucket");
				long[] times = ycsb.runTest(ycsbClient, 20_000, 1_000_000, 100, outputPath, bucket);
				//long[] times = ycsb.runTest(ycsbClient, 300_000, 1_500_000, 100, outputPath, bucket);
				//long[] times = ycsb.runTest(ycsbClient, 20_000, 100_000, 30, outputPath, bucket);
				String fileName = outputPath + "result_time_" + cassandraCommit + ".csv";
				writeTimes(fileName, initTime, times[0], times[1]);
				
				String key = getKey(commitDate) + "result_time_" + cassandraCommit + ".csv";
				S3Upload upload = new S3Upload(new File(fileName), bucket, key);
				upload.upload();
				
				logger.info("Benchmark took " + (System.currentTimeMillis()-timestamp)/1000 + "s, commit is " + cassandraCommit);
			} 
			
			cassandraServer1.disconnect();
			cassandraServer2.disconnect();
			ycsbClient.disconnect();
			
		}catch (Exception e) {
			logger.error("Error during benchmark: ", e);
		}finally{
			done = true;
		}
	}
	
	private void writeTimes(String path, long init, long load, long run){
		PrintWriter out;
		try {
			out = new PrintWriter(path);
			out.println("Metric;Value");
			out.println("Init Time;" + (init / 1000d / 60d));
			out.println("Load Time;" + (load / 1000d / 60d));
			out.println("Run Time;" + (run / 1000d / 60d));
			out.println("Total Time;" + ((run + load + init) / 1000d / 60d));
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static String buildPath(Date commitDate) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-d");  
		String strDate = commitDate != null ? dateFormat.format(commitDate) : "";  
		String outputdir = (String) config.get("results");
		return outputdir + File.separatorChar + strDate + "_";
	}
	
	public static String getKey(Date commitDate) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-d");  
		String strDate = commitDate != null ? dateFormat.format(commitDate) : "";  
		return strDate + "_";
	}
	
	
	public boolean isFinished() {
		return done;
	}
	
	public boolean wasBuildWithSucces() {
		return BenchmarkManager.wasBuildBefore(cassandraCommit, commitDate);
	}

}
