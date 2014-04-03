package org.fcrepo.federation.glacierconnector;

public class DownloadArchiveThreadInfo {
	
	public String jobId;
	public String archiveId;
	public DownloadArchiveThread threadId;
	
	public DownloadArchiveThreadInfo(String jobId,String archiveId,DownloadArchiveThread threadId){
		this.jobId = jobId;
		this.archiveId = archiveId;
		this.threadId = threadId;
		
	}

}
