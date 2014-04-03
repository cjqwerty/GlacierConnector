package org.fcrepo.federation.glacierconnector;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.modeshape.common.util.IoUtil;

/**
 * download file from Glacier work thread
 * 
 *
 */
import java.util.Random;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;

public class DownloadArchiveThread extends Thread {

	private String threadName;

	private CallbackInterface threadManager;
	private CallbackInterface postProcessor;

	private String jobId;
	private String archiveId;

	private String vaultName;

	private String cacheDirectory;

	private AmazonGlacierClient client;

	private DownloadArchiveThread() {
	}

	public DownloadArchiveThread(String threadName,
			CallbackInterface threadManager, CallbackInterface postProcessor) {
		if (threadName == null || threadName.equals("")) {
			Random g = new Random(9999);
			this.threadName = "DownloadArchiveThread" + g.nextInt();

		} else
			this.threadName = threadName;

		this.threadManager = threadManager;
		this.postProcessor = postProcessor;

	}

	public DownloadArchiveThread withGlacierClientCredential(String accessKey,
			String secretKey, String region) {

		this.client = GlacierClientFactory.newClient(accessKey, secretKey,
				"https://glacier." + region + ".amazonaws.com");

		return this;
	}

	public DownloadArchiveThread withVaultName(String vaultName) {
		this.vaultName = vaultName;
		return this;
	}

	public DownloadArchiveThread withJobId(String jobId) {
		this.jobId = jobId;
		return this;
	}

	public DownloadArchiveThread withArchiveId(String archiveId) {
		this.archiveId = archiveId;
		return this;
	}

	public DownloadArchiveThread withcacheDirectory(String cacheDirectory) {
		this.cacheDirectory = cacheDirectory;
		return this;
	}

	private InputStream GetDownloadStream() throws IOException {

		try {
			GetJobOutputRequest getJobOutputRequest = new GetJobOutputRequest()
					.withVaultName(vaultName).withJobId(jobId);
			GetJobOutputResult getJobOutputResult = client
					.getJobOutput(getJobOutputRequest);

			return getJobOutputResult.getBody();

		} catch (AmazonClientException e) {
			throw new IOException(e.getMessage());
		}

	}

	public void run() {

		try {

			System.out.println("enter thread  " + this.getName());

			try {

				File file = new File(cacheDirectory + '/' + vaultName + '/',
						archiveId);
				OutputStream ostream = new BufferedOutputStream(
						new FileOutputStream(file));
				IoUtil.write(GetDownloadStream(), ostream);

			} catch (IOException e) {

				e.printStackTrace();
			}

		} finally {

			System.out.println("thread  " + this.getName() + " end...");

			threadManager.CallBack(archiveId);
			postProcessor.CallBack(archiveId);

			System.out.println("thread  " + this.getName() + " end");

		}
	}
}
