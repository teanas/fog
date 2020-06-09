package manager;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import server.YCSBClient;

public class BenchmarkManager {

	private static Logger logger = LoggerFactory.getLogger(BenchmarkManager.class);
	
	public static void main(String[] args) {
		byte waitMinutes = 45;
		long waitMs = waitMinutes*60*1000;
		boolean unix = File.separatorChar  == '/';
		
		Benchmark.initConfig();
		
		List<Pair> commits = new ArrayList<Pair>();
		String cassandraCommits = unix ? "/tmp/cassandraCommits.csv" : "cassandraCommits.csv";
		try {
		
		Files.lines(new File(cassandraCommits).toPath())
		.sequential()
		.map(x -> x.split(";"))
		.map(x -> new Pair(x[0], new Date(x[1])))
		.filter(x -> x.getDate().after(new Date(2016-1900, 0, 1))) //1.1.2016
		.forEach(x -> {
			if(!wasBuildBefore(x.getCommit(), x.getDate())) {
				commits.add(x);
			}
		});
		} catch (Exception e) {
			logger.error("unable to read file with cassandra commits", e);
			File f = new File(cassandraCommits);
			logger.error("Path is " + f.getAbsoluteFile());
			return;
		}
		
		logger.info(commits.size() + " commits in queue.");
				
		while(!commits.isEmpty()) {
			logger.info(commits.size() + " commits remaining in list");
			Pair pair = commits.remove(0);
			Benchmark b = Benchmark.createBenchmark(pair);
			long startTime = System.currentTimeMillis();
			b.start();
			
			while(!b.isFinished() && System.currentTimeMillis() < startTime + waitMs) {
				try {
					Thread.sleep(10000);
					} catch (InterruptedException e) {
						logger.warn("Interrupted while waiting for benchmark");					
					}
			}
			
			if (!b.wasBuildWithSucces()) {
				logger.warn("Benchmark " + pair.toString() + " failed, requeued");
				commits.add(pair);
			} else {
				logger.info("Benchmark " + pair.toString() + " succeeded");
			}
		}
		
		logger.info("Run complete :)");
	}
				
				
	public static boolean wasBuildBefore(String cassandraCommit, Date commitDate) {
		String outputPath = Benchmark.buildPath(commitDate);
		String r = outputPath + "result_run_" + cassandraCommit + ".ycsb";
		String l = outputPath + "result_load_" + cassandraCommit + ".ycsb";
		String t = outputPath + "result_time_" + cassandraCommit + ".csv";
		File rf = new File(r);
		File lf = new File(l);
		File tf = new File(t);
		boolean result =  (rf.exists() && lf.exists() && tf.exists() && YCSBClient.checkValidFiles(r) && YCSBClient.checkValidFiles(l));
		if(!result) {
			rf.delete();
			lf.delete();
			tf.delete();
		}
		return result;
	}
	
	

}
