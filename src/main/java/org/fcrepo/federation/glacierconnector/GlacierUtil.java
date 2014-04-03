package org.fcrepo.federation.glacierconnector;

import java.io.InputStream;
import com.amazonaws.services.cloudsearch.model.ResourceNotFoundException;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
import java.util.List;

import com.amazonaws.services.glacier.model.CreateVaultRequest;
import com.amazonaws.services.glacier.model.CreateVaultResult;
import com.amazonaws.services.glacier.model.DeleteArchiveRequest;
import com.amazonaws.services.glacier.model.DeleteVaultRequest;
import com.amazonaws.services.glacier.model.DescribeVaultOutput;
import com.amazonaws.services.glacier.model.DescribeVaultRequest;
import com.amazonaws.services.glacier.model.DescribeVaultResult;
import com.amazonaws.services.glacier.model.ListVaultsRequest;
import com.amazonaws.services.glacier.model.ListVaultsResult;
import com.amazonaws.services.glacier.model.UploadArchiveRequest;
import com.amazonaws.services.glacier.model.UploadArchiveResult;

public class GlacierUtil {

	private static void createVault(AmazonGlacierClient client, String vaultName) {
		CreateVaultRequest createVaultRequest = new CreateVaultRequest()
				.withVaultName(vaultName);
		CreateVaultResult createVaultResult = client
				.createVault(createVaultRequest);

		System.out.println("Created vault successfully: "
				+ createVaultResult.getLocation());
	}

	private static void describeVault(AmazonGlacierClient client,
			String vaultName) {
		DescribeVaultRequest describeVaultRequest = new DescribeVaultRequest()
				.withVaultName(vaultName);
		DescribeVaultResult describeVaultResult = client
				.describeVault(describeVaultRequest);

		System.out.println("Describing the vault: " + vaultName);
		System.out.print("CreationDate: "
				+ describeVaultResult.getCreationDate()
				+ "\nLastInventoryDate: "
				+ describeVaultResult.getLastInventoryDate()
				+ "\nNumberOfArchives: "
				+ describeVaultResult.getNumberOfArchives() + "\nSizeInBytes: "
				+ describeVaultResult.getSizeInBytes() + "\nVaultARN: "
				+ describeVaultResult.getVaultARN() + "\nVaultName: "
				+ describeVaultResult.getVaultName());
	}

	public static boolean isVaultExists(AmazonGlacierClient client,
			String vaultName) {

		boolean vaultExists = true;

		try {

			DescribeVaultRequest describeVaultRequest = new DescribeVaultRequest()
					.withVaultName(vaultName);
			client.describeVault(describeVaultRequest);
		} catch (ResourceNotFoundException e) {

			vaultExists = false;

		}

		return vaultExists;
	}

	private static void listVaults(AmazonGlacierClient client) {
		ListVaultsRequest listVaultsRequest = new ListVaultsRequest();
		ListVaultsResult listVaultsResult = client
				.listVaults(listVaultsRequest);

		List<DescribeVaultOutput> vaultList = listVaultsResult.getVaultList();
		System.out.println("\nDescribing all vaults (vault list):");
		for (DescribeVaultOutput vault : vaultList) {
			System.out.println("\nCreationDate: " + vault.getCreationDate()
					+ "\nLastInventoryDate: " + vault.getLastInventoryDate()
					+ "\nNumberOfArchives: " + vault.getNumberOfArchives()
					+ "\nSizeInBytes: " + vault.getSizeInBytes()
					+ "\nVaultARN: " + vault.getVaultARN() + "\nVaultName: "
					+ vault.getVaultName());
		}
	}

	private static void deleteVault(AmazonGlacierClient client, String vaultName) {
		DeleteVaultRequest request = new DeleteVaultRequest()
				.withVaultName(vaultName);
		client.deleteVault(request);
		System.out.println("Deleted vault: " + vaultName);
	}

	public static void upload(AmazonGlacierClient client, String vaultName,
			String id, InputStream is, long streamLength) throws Exception {
		UploadArchiveRequest request = new UploadArchiveRequest()
				.withVaultName(vaultName)
				.withChecksum(TreeHashGenerator.calculateTreeHash(is))
				.withBody(is).withContentLength(streamLength);

		request.setArchiveDescription(id);

		UploadArchiveResult uploadArchiveResult = client.uploadArchive(request);

		System.out.println("ArchiveID: " + uploadArchiveResult.getArchiveId());

	}

	public static void delete(AmazonGlacierClient client, String vaultName,
			String archiveId) throws Exception {
		client.deleteArchive(new DeleteArchiveRequest()
				.withVaultName(vaultName).withArchiveId(archiveId));

		System.out.println("Deleted archive successfully.");

	}

}
