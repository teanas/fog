package manager;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;


public class S3Upload {

	private static Logger logger = LoggerFactory.getLogger(S3Upload.class);
	private static AWSCredentials credentials = new BasicAWSCredentials("AKIAJZFYGPNKMOV7DH4A", "qXeEOb3e4zw9f5zAIDc1mEnnsw3p/ASnWk5fQuq/");
	private static AmazonS3 client;
	
	private File file;
	private String bucketName;
	private String key;
	
	public S3Upload(File file, String bucketName, String key) {
		this.file = file;
		this.bucketName = bucketName;
		this.key = key;
		if (client == null) {
			client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.EU_WEST_1).build();
		}
		
		if(!client.doesBucketExistV2(bucketName)) {
			client.createBucket(bucketName);
		} else {
			logger.info("Bucket already exist, skip creation of bucket");			
		}
	}
	
	public boolean upload() {
		// Upload a file as a new object with ContentType and title specified.
		if (!file.exists()) {
			logger.error("File " + file.getAbsolutePath() + " does not exist, unable to upload");
			return false;
		}
		try {
	        PutObjectRequest request = new PutObjectRequest(bucketName, key, file);
	        ObjectMetadata metadata = new ObjectMetadata();
	        metadata.setContentType("plain/text");
	        request.setMetadata(metadata);
	        client.putObject(request);
	        return true;
		} catch (SdkClientException e) {
			logger.error("SDK Client Exception:", e);
			return false;
		}
	}
	
	
	
	
}
