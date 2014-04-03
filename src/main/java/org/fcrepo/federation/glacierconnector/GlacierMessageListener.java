package org.fcrepo.federation.glacierconnector;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

public class GlacierMessageListener implements Runnable {

	private AmazonSQSClient sqsClient;
	private String sqsQueueARN;
	private String sqsQueueURL;
	private static long sleepTime = 10;
	private static int poolSize = 10;
	private String accessKey;
	private String secretKey;
	private String region;
	private String cacheDirectory;

	private static Pool jobPool;
	private static Map<String, String> waitingList;

	public GlacierMessageListener() {
		jobPool = new Pool(poolSize);
		waitingList = new HashMap<String, String>();
	}

	public GlacierMessageListener withcacheDirectory(String cacheDirectory) {
		this.cacheDirectory = cacheDirectory;
		return this;
	}

	public GlacierMessageListener withSQSClient(String accessKey,
			String secretKey, String region) {

		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.region = region;

		BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey,
				secretKey);

		this.sqsClient = new AmazonSQSClient(credentials);
		this.sqsClient.setEndpoint("https://sqs." + region + ".amazonaws.com");

		return this;
	}

	public GlacierMessageListener withSQS(String SQSQueueName) {

		// Create queue
		CreateQueueRequest request = new CreateQueueRequest()
				.withQueueName(SQSQueueName);
		CreateQueueResult result = sqsClient.createQueue(request);
		sqsQueueURL = result.getQueueUrl();

		GetQueueAttributesRequest qRequest = new GetQueueAttributesRequest()
				.withQueueUrl(sqsQueueURL).withAttributeNames("QueueArn");

		// Get Queue ARN
		GetQueueAttributesResult qResult = sqsClient
				.getQueueAttributes(qRequest);
		sqsQueueARN = qResult.getAttributes().get("QueueArn");

		// Create policy for the Queue
		Policy sqsPolicy = new Policy().withStatements(new Statement(
				Effect.Allow).withPrincipals(Principal.AllUsers)
				.withActions(SQSActions.SendMessage)
				.withResources(new Resource(sqsQueueARN)));
		Map<String, String> queueAttributes = new HashMap<String, String>();
		queueAttributes.put("Policy", sqsPolicy.toJson());
		sqsClient.setQueueAttributes(new SetQueueAttributesRequest(sqsQueueURL,
				queueAttributes));

		return this;
	}

	public String GetSQSQueueARN() {
		return sqsQueueARN;
	}

	private void deleteJobMessage(Message msg) {

		System.out.println("Deleting a message.\n");
		String messageReceiptHandle = msg.getReceiptHandle();
		sqsClient.deleteMessage(new DeleteMessageRequest().withQueueUrl(
				sqsQueueURL).withReceiptHandle(messageReceiptHandle));

	}

	private void receiveJobMessage() throws InterruptedException,
			JsonParseException, IOException {

		try {
			ObjectMapper mapper = new ObjectMapper();
			JsonFactory factory = mapper.getJsonFactory();

			System.out.println("receiveJobMessage job is invoked...");
			while (!Thread.currentThread().isInterrupted()) {

				List<Message> msgs = sqsClient.receiveMessage(
						new ReceiveMessageRequest(sqsQueueURL)
								.withMaxNumberOfMessages(10)).getMessages();

				if (msgs.size() > 0) {
					for (Message m : msgs) {
						JsonParser jpMessage = factory.createJsonParser(m
								.getBody());
						JsonNode jobMessageNode = mapper.readTree(jpMessage);
						String jobMessage = jobMessageNode.get("Message")
								.getTextValue();

						JsonParser jpDesc = factory
								.createJsonParser(jobMessage);
						JsonNode jobDescNode = mapper.readTree(jpDesc);

						final String retrievedJobId = jobDescNode.get("JobId")
								.getTextValue();
						final String statusCode = jobDescNode.get("StatusCode")
								.getTextValue();
						final String archiveId = jobDescNode.get("ArchiveId")
								.getTextValue();
						String vaultNameTmp = jobDescNode.get("VaultARN")
								.getTextValue();

						if (vaultNameTmp != null && !vaultNameTmp.equals("")) {
							int ind = vaultNameTmp.lastIndexOf('/');
							if (ind > 0) {
								vaultNameTmp = vaultNameTmp.substring(ind + 1);
							}
						}
						final String vaultName = vaultNameTmp;

						if (isInWaitingList(archiveId)
								&& statusCode.equals("Succeeded")
								&& !isRegistered(archiveId)) {
							// Download file to local cache

							DownloadArchiveCallback downloadPostProcessor = new DownloadArchiveCallback(
									"sender", "password", "to");

							DownloadArchiveThread downloadArchiveThread = new DownloadArchiveThread(
									null, jobPool, downloadPostProcessor)
									.withArchiveId(archiveId)
									.withcacheDirectory(cacheDirectory)
									.withVaultName(vaultName)
									.withGlacierClientCredential(accessKey,
											secretKey, region)
									.withJobId(retrievedJobId);

							System.out
									.println("before downloadArchiveThread.start() ");

							downloadArchiveThread.start();

							register(archiveId, new DownloadArchiveThreadInfo(
									retrievedJobId, archiveId,
									downloadArchiveThread));

							System.out
									.println("after downloadArchiveThread.start() ");
							
							deleteJobMessage(m);
						}
					}

				} else {
					Thread.sleep(sleepTime * 1000);
				}
			}
		} catch (InterruptedException e) {
			System.out.println("receiveJobMessage exit.");
		}

		System.out.println("receiveJobMessage is over!");
	}

	synchronized public static boolean isRegistered(String archiveId) {
		return jobPool.itemExists(archiveId);
	}

	synchronized public static DownloadArchiveThreadInfo register(
			String archiveId, DownloadArchiveThreadInfo ti) {
		return jobPool.putItem(archiveId, ti);
	}

	synchronized public static boolean isInWaitingList(String archiveId) {
		return waitingList.containsKey(archiveId);
	}

	synchronized public static void removeFromWaitingList(String archiveId) {
		waitingList.remove(archiveId);
	}

	synchronized public static boolean putIntoWaitingList(String archiveId,
			String jobId) {
		if (!isInWaitingList(archiveId)) {
			waitingList.put(archiveId, jobId);
			return true;
		}

		return false;
	}

	synchronized public static void unregister(String archiveId) {

		removeFromWaitingList(archiveId);
		jobPool.removeItem(archiveId);
	}

	synchronized public static void clear() {

		waitingList.clear();
		jobPool.clear();

	}

	@Override
	public void run() {

		try {

			receiveJobMessage();

		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
		}

	}

}
