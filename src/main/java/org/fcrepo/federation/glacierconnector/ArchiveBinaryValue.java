package org.fcrepo.federation.glacierconnector;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import javax.jcr.RepositoryException;

import org.modeshape.jcr.cache.DocumentStoreException;
import org.modeshape.jcr.mimetype.MimeTypeDetector;
import org.modeshape.jcr.value.BinaryKey;
import org.modeshape.jcr.value.BinaryValue;
import org.modeshape.jcr.value.binary.ExternalBinaryValue;

/**
 * A {@link BinaryValue} implementation used to read the content of a resolvable
 * URL. This class computes the {@link AbstractBinary#getMimeType() MIME type}
 * lazily.
 */

public class ArchiveBinaryValue extends ExternalBinaryValue {

	private static final long serialVersionUID = 1L;
	private String accessKey;
	private String secretKey;
	private String region;
	private String SNSTopicName;
	private String sqsQueueARN;
	private String vaultName;
	private String archiveId;

	private String cacheRootDirectory;

	// GlacierMessageListener glacierMessageListener;

	public ArchiveBinaryValue(BinaryKey key, String sourceName,
			String accessKey, String secretKey, String region,
			String SNSTopicName, String vaultName, String archiveId,
			GlacierMessageListener glacierMessageListener, long size,
			String nameHint, MimeTypeDetector mimeTypeDetector) {

		super(key, sourceName, vaultName + '/' + archiveId, size,
				"GlacireArcive", mimeTypeDetector);

		// this.glacierMessageListener = glacierMessageListener;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.region = region;

		this.vaultName = vaultName;
		this.archiveId = archiveId;

	}

	public ArchiveBinaryValue withCacheRootDirectory(String cacheRootDirectory) {
		this.cacheRootDirectory = cacheRootDirectory;
		return this;
	}

	public ArchiveBinaryValue withSQSQueueARN(String sqsQueueARN) {
		this.sqsQueueARN = sqsQueueARN;
		return this;
	}

	private InputStream getStreamFromCache() throws FileNotFoundException {
		return new BufferedInputStream(new FileInputStream(cacheRootDirectory + '/' + vaultName+ '/'+archiveId));
	}

	private String sendDownloadArchiveRequest() {
		
		System.out.println("Sending a download request to Glacier, archiveid : "+vaultName + '/'+archiveId);

		GlacireArchive glacireArchive = new GlacireArchive(accessKey,
				secretKey, region).withSNS(sqsQueueARN).withValultName(vaultName);

		return glacireArchive.sendDownloadRequest(
				GlacireArchive.JobType.GETARCHIVE.getJobTypeCode(), archiveId);

	}


	@Override
	public InputStream getStream() throws RepositoryException {
		try {

			if (archiveId != null
					&& !GlacierMessageListener.isInWaitingList(archiveId)) {

				File file = new File(cacheRootDirectory + '/' + vaultName,
						archiveId);

				if (file.exists()) {

					return getStreamFromCache();
				} else synchronized(this){

					
					String jobId = sendDownloadArchiveRequest();

					GlacierMessageListener.putIntoWaitingList(archiveId, jobId);

					throw new DocumentStoreException(archiveId,
							"Archive request is sent to Glacier, please try again after 24 hours.");
					

				}

			}
		} 
		catch(DocumentStoreException e){
			throw e;
		}
		catch (Exception e) {
			throw new RepositoryException(e);
		}

		return null;
	}
}
