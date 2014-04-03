package org.fcrepo.federation.glacierconnector;


import org.apache.avro.reflect.Nullable;
import org.jgroups.util.UUID;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.UnsubscribeRequest;

public class GlacireArchive {

	private AmazonGlacierClient client;
	private AmazonSNSClient snsClient;
	private String region ;
	
	private String accessKey;
	private String secretKey;
	private String archiveId;

	private String vaultName = "examplevaultfordelete";
	private String snsTopicNamePrefix = "GlacierConnector";
	private String sqsQueueName = "GlacierConnectorQueue";

	private String sqsQueueARN;
	private String sqsQueueURL;
	private String snsTopicARN;
	private String snsSubscriptionARN;

	public enum JobType {
		GETINVENTORY("inventory-retrieval"), GETARCHIVE("archive-retrieval");
		private String typeCode;

		private JobType(String typeCode) {
			this.typeCode = typeCode;
		}

		public String getJobTypeCode() {
			return this.typeCode;
		}
	}

	public GlacireArchive(String accessKey, String secretKey, String region) {
		BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey,
				secretKey);

		this.client = new AmazonGlacierClient(credentials);
		this.client.setEndpoint("https://glacier." + region + ".amazonaws.com");
		this.snsClient = new AmazonSNSClient(credentials);
		this.snsClient.setEndpoint("https://sns." + region + ".amazonaws.com");
		
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.region = region;

	}

	public GlacireArchive withValultName(String vaultName) {
		this.vaultName = vaultName;
		return this;
	}
	
	public GlacireArchive withSNS(String sqsQueueARN) {
		this.sqsQueueARN = sqsQueueARN;
		
		CreateTopicRequest request = new CreateTopicRequest()
				.withName(snsTopicNamePrefix + UUID.randomUUID());
		CreateTopicResult result = snsClient.createTopic(request);
		snsTopicARN = result.getTopicArn();

		SubscribeRequest request2 = new SubscribeRequest()
				.withTopicArn(snsTopicARN).withEndpoint(sqsQueueARN)
				.withProtocol("sqs");
		SubscribeResult result2 = snsClient.subscribe(request2);

		snsSubscriptionARN = result2.getSubscriptionArn();

		return this;
	}

	
	/**
	 * send the download request to Glacier
	 * 
	 * @param type
	 * @param archiveId
	 * @return jobId
	 */

	public String sendDownloadRequest(String type, @Nullable String archiveId) {
		
		this.archiveId = archiveId;

		boolean isDownloadArcive = archiveId != null && !archiveId.equals("") ? true
				: false;


		JobParameters jobParameters = new JobParameters().withType(type)
				.withSNSTopic(snsTopicARN);

		if (isDownloadArcive)
			jobParameters.withArchiveId(archiveId);

		InitiateJobRequest request = new InitiateJobRequest().withVaultName(
				vaultName).withJobParameters(jobParameters);

		InitiateJobResult response = client.initiateJob(request);

		return response.getJobId();
	}
	

	public void shutDown() {

		snsClient.unsubscribe(new UnsubscribeRequest(snsSubscriptionARN));
		snsClient.deleteTopic(new DeleteTopicRequest(snsTopicARN));

	}

}