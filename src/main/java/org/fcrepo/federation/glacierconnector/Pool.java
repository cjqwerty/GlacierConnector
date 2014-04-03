package org.fcrepo.federation.glacierconnector;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Pool implements CallbackInterface {

	public static final int DEFAULT_MAX_AVAILABLE = 10;

	private SizeFixedMap<String, DownloadArchiveThreadInfo> ThreadIdMap;

	private Lock lock = new ReentrantLock();

	private ExecutorService executorService;

	private Pool() {
	}

	Pool(int poolSize) {
		ThreadIdMap = new SizeFixedMap<String, DownloadArchiveThreadInfo>(
				poolSize);

		executorService = Executors.newFixedThreadPool(poolSize);

	}

	synchronized public boolean itemExists(String key) {

		return ThreadIdMap.containsKey(key);

	}

	// synchronized
	public DownloadArchiveThreadInfo putItem(String key,
			DownloadArchiveThreadInfo ti) {
		DownloadArchiveThreadInfo returnValue = null;

		lock.lock();

		if (!ThreadIdMap.containsKey(key)) {

			returnValue = ThreadIdMap.put(key, ti);

		}

		lock.unlock();
		return returnValue;
	}

	void removeItem(String key) {

		System.out.println("Enter removeItem ...");

		lock.lock();
		System.out.println("thread " + Thread.currentThread().getName()
				+ " enter removeItem ..., get lock");

		ThreadIdMap.remove(key);

		lock.unlock();

		System.out.println("thread " + Thread.currentThread().getName()
				+ " enter removeItem ..., releae lock");

	}

	@Override
	synchronized public void CallBack(String archiveId) {
		System.out.println("Enter returnResult ...");

		DownloadArchiveThreadInfo downloadArchiveThreadInfo = ThreadIdMap
				.get(archiveId);
		if (null != downloadArchiveThreadInfo.threadId
				&& downloadArchiveThreadInfo.threadId.isAlive())
			downloadArchiveThreadInfo.threadId.interrupt();

		removeItem(archiveId);

		System.out.println("leave returnResult ");

	}

	synchronized public void clear() {

		lock.lock();

		Iterator<Map.Entry<String, DownloadArchiveThreadInfo>> iterator = ThreadIdMap
				.entrySet().iterator();

		while (iterator.hasNext()) {
			Map.Entry<String, DownloadArchiveThreadInfo> mapEntry = (Map.Entry<String, DownloadArchiveThreadInfo>) iterator
					.next();
			DownloadArchiveThreadInfo downloadArchiveThreadInfo = mapEntry
					.getValue();
			if (null != downloadArchiveThreadInfo.threadId
					&& downloadArchiveThreadInfo.threadId.isAlive())
				downloadArchiveThreadInfo.threadId.interrupt();
		}

		ThreadIdMap.clear();

		lock.unlock();
		executorService.shutdown();

	}

}
