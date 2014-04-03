package org.fcrepo.federation.glacierconnector;

/*
 * ModeShape (http://www.modeshape.org)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of
 * individual contributors.
 *
 * ModeShape is free software. Unless otherwise indicated, all code in ModeShape
 * is licensed to you under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * ModeShape is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.infinispan.schematic.document.Document;
import org.modeshape.common.util.StringUtil;
import org.modeshape.jcr.JcrI18n;
import org.modeshape.jcr.JcrLexicon;
import org.modeshape.jcr.RepositoryConfiguration;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.cache.DocumentStoreException;
import org.modeshape.jcr.federation.spi.Connector;
import org.modeshape.jcr.federation.spi.DocumentChanges;
import org.modeshape.jcr.federation.spi.DocumentReader;
import org.modeshape.jcr.federation.spi.DocumentWriter;
import org.modeshape.jcr.federation.spi.PageKey;
import org.modeshape.jcr.federation.spi.Pageable;
import org.modeshape.jcr.federation.spi.WritableConnector;
import org.modeshape.jcr.value.BinaryKey;
import org.modeshape.jcr.value.BinaryValue;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.Property;
import org.modeshape.jcr.value.binary.ExternalBinaryValue;
import org.modeshape.jcr.value.binary.UrlBinaryValue;

import com.amazonaws.services.glacier.AmazonGlacierClient;

/**
 * {@link Connector} implementation that exposes a single directory on the local
 * file system. This connector has several properties that must be configured
 * via the {@link RepositoryConfiguration}:
 * <ul>
 * <li><strong><code>directoryPath</code></strong> - The path to the file or
 * folder that is to be accessed by this connector.</li>
 * <li><strong><code>readOnly</code></strong> - A boolean flag that specifies
 * whether this source can create/modify/remove files and directories on the
 * file system to reflect changes in the JCR content. By default, sources are
 * not read-only.</li>
 * <li><strong><code>addMimeTypeMixin</code></strong> - A boolean flag that
 * specifies whether this connector should add the 'mix:mimeType' mixin to the
 * 'nt:resource' nodes to include the 'jcr:mimeType' property. If set to
 * <code>true</code>, the MIME type is computed immediately when the
 * 'nt:resource' node is accessed, which might be expensive for larger files.
 * This is <code>false</code> by default.</li>
 * <li><strong><code>extraPropertyStorage</code></strong> - An optional string
 * flag that specifies how this source handles "extra" properties that are not
 * stored via file system attributes. See {@link #extraPropertiesStorage} for
 * details. By default, extra properties are stored in the same Infinispan cache
 * that the repository uses.</li>
 * <li><strong><code>exclusionPattern</code></strong> - Optional property that
 * specifies a regular expression that is used to determine which files and
 * folders in the underlying file system are not exposed through this connector.
 * Files and folders with a name that matches the provided regular expression
 * will <i>not</i> be exposed by this source.</li>
 * <li><strong><code>inclusionPattern</code></strong> - Optional property that
 * specifies a regular expression that is used to determine which files and
 * folders in the underlying file system are exposed through this connector.
 * Files and folders with a name that matches the provided regular expression
 * will be exposed by this source.</li>
 * </ul>
 * Inclusion and exclusion patterns can be used separately or in combination.
 * For example, consider these cases:
 * <table cellspacing="0" cellpadding="1" border="1">
 * <tr>
 * <th>Inclusion Pattern</th>
 * <th>Exclusion Pattern</th>
 * <th>Examples</th>
 * </tr>
 * <tr>
 * <td>(.+)\\.txt$</td>
 * <td></td>
 * <td>Includes only files and directories whose names end in "<code>.txt</code>
 * " (e.g., "<code>something.txt</code>" ), but does not include files and other
 * folders such as "<code>something.jar</code>" or "
 * <code>something.txt.zip</code>".</td>
 * </tr>
 * <tr>
 * <td>(.+)\\.txt$</td>
 * <td>my.txt</td>
 * <td>Includes only files and directories whose names end in "<code>.txt</code>
 * " (e.g., "<code>something.txt</code>" ) with the exception of "
 * <code>my.txt</code>", and does not include files and other folders such as "
 * <code>something.jar</code>" or " <code>something.txt.zip</code>".</td>
 * </tr>
 * <tr>
 * <td>my.txt</td>
 * <td>.+</td>
 * <td>Excludes all files and directories except any named "<code>my.txt</code>
 * ".</td>
 * </tr>
 * </table>
 */
public class GlacierConnector extends WritableConnector implements Pageable {

	private static final String FILE_SEPARATOR = System
			.getProperty("file.separator");
	private static final String DELIMITER = "/";
	private static final String NT_FOLDER = "nt:folder";
	private static final String NT_FILE = "nt:file";
	private static final String NT_RESOURCE = "nt:resource";
	private static final String MIX_MIME_TYPE = "mix:mimeType";
	private static final String JCR_PRIMARY_TYPE = "jcr:primaryType";
	private static final String JCR_DATA = "jcr:data";
	private static final String JCR_MIME_TYPE = "jcr:mimeType";
	private static final String JCR_ENCODING = "jcr:encoding";
	private static final String JCR_CREATED = "jcr:created";
	private static final String JCR_CREATED_BY = "jcr:createdBy";
	private static final String JCR_LAST_MODIFIED = "jcr:lastModified";
	private static final String JCR_LAST_MODIFIED_BY = "jcr:lastModified";
	private static final String JCR_CONTENT = "jcr:content";
	private static final String JCR_CONTENT_SUFFIX = DELIMITER + JCR_CONTENT;
	private static final int JCR_CONTENT_SUFFIX_LENGTH = JCR_CONTENT_SUFFIX
			.length();

	private static final String BAGIT_CLOUDFILE_TYPE = "bagit:cloudFile";

	private static final String EXTRA_PROPERTIES_JSON = "json";
	private static final String EXTRA_PROPERTIES_LEGACY = "legacy";
	private static final String EXTRA_PROPERTIES_NONE = "none";

	private String accessKey;// = "AKIAIHUNOBIY2TKMUPGA";
	private String secretKey;// = "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";
	private String endpoint;

	private String localCacheDirectory;
	private String region;
	private String SQSQueueName;

	/**
	 * The string path for a {@link File} object that represents the top-level
	 * directory accessed by this connector. This is set via reflection and is
	 * required for this connector.
	 */
	private String directoryPath;
	private File directory;

	/**
	 * A string that is created in the
	 * {@link #initialize(NamespaceRegistry, NodeTypeManager)} method that
	 * represents the absolute path to the {@link #directory}. This path is
	 * removed from an absolute path of a file to obtain the ID of the node.
	 */
	private String directoryAbsolutePath;
	private int directoryAbsolutePathLength;

	/**
	 * A boolean flag that specifies whether this connector should add the
	 * 'mix:mimeType' mixin to the 'nt:resource' nodes to include the
	 * 'jcr:mimeType' property. If set to <code>true</code>, the MIME type is
	 * computed immediately when the 'nt:resource' node is accessed, which might
	 * be expensive for larger files. This is <code>false</code> by default.
	 */
	private boolean addMimeTypeMixin = false;

	/**
	 * The regular expression that, if matched by a file or folder, indicates
	 * that the file or folder should be included.
	 */
	private String inclusionPattern;

	/**
	 * The regular expression that, if matched by a file or folder, indicates
	 * that the file or folder should be ignored.
	 */
	private String exclusionPattern;

	/**
	 * The maximum number of children a folder will expose at any given time.
	 */
	private int pageSize = 20;

	/**
	 * The {@link FilenameFilter} implementation that is instantiated in the
	 * {@link #initialize(NamespaceRegistry, NodeTypeManager)} method.
	 */
	// private InclusionExclusionFilenameFilter filenameFilter;

	/**
	 * A string that specifies how the "extra" properties are to be stored,
	 * where an "extra" property is any JCR property that cannot be stored
	 * natively on the file system as file attributes. This field is set via
	 * reflection, and the value is expected to be one of these valid values:
	 * <ul>
	 * <li>"<code>store</code>" - Any extra properties are stored in the same
	 * Infinispan cache where the content is stored. This is the default and is
	 * used if the actual value doesn't match any of the other accepted values.</li>
	 * <li>"<code>json</code>" - Any extra properties are stored in a JSON file
	 * next to the file or directory.</li>
	 * <li>"<code>legacy</code>" - Any extra properties are stored in a
	 * ModeShape 2.x-compatible file next to the file or directory. This is
	 * generally discouraged unless you were using ModeShape 2.x and have a
	 * directory structure that already contains these files.</li>
	 * <li>"<code>none</code>" - An extra properties that prevents the storage
	 * of extra properties by throwing an exception when such extra properties
	 * are defined.</li>
	 * </ul>
	 */
	private String extraPropertiesStorage;

	private NamespaceRegistry registry;

	// private ExecutorService threadPool;

	Thread glacierMessageListenerThread;

	private GlacierMessageListener glacierMessageListener = null;

	@Override
	public void initialize(NamespaceRegistry registry,
			NodeTypeManager nodeTypeManager) throws RepositoryException,
			IOException {

		super.initialize(registry, nodeTypeManager);
		this.registry = registry;
		this.endpoint = "https://sqs." + region + ".amazonaws.com";

		this.glacierMessageListener = new GlacierMessageListener()
				.withSQSClient(accessKey, secretKey, region)
				.withSQS(SQSQueueName).withcacheDirectory(localCacheDirectory);

		glacierMessageListenerThread = new Thread(glacierMessageListener);

		glacierMessageListenerThread.start();

	}

	/**
	 * Get the namespace registry.
	 * 
	 * @return the namespace registry; never null
	 */
	NamespaceRegistry registry() {
		return registry;
	}

	/**
	 * Utility method for determining if the supplied identifier is for the
	 * "jcr:content" child node of a file. * Subclasses may override this method
	 * to change the format of the identifiers, but in that case should also
	 * override the {@link #fileFor(String)}, {@link #isRoot(String)}, and
	 * {@link #idFor(File)} methods.
	 * 
	 * @param id
	 *            the identifier; may not be null
	 * @return true if the identifier signals the "jcr:content" child node of a
	 *         file, or false otherwise
	 * @see #isRoot(String)
	 * @see #fileFor(String)
	 * @see #idFor(File)
	 */
	protected boolean isContentNode(String id) {
		return id.endsWith(JCR_CONTENT_SUFFIX);
	}

	/**
	 * Utility method for obtaining the {@link File} object that corresponds to
	 * the supplied identifier. Subclasses may override this method to change
	 * the format of the identifiers, but in that case should also override the
	 * {@link #isRoot(String)}, {@link #isContentNode(String)}, and
	 * {@link #idFor(File)} methods.
	 * 
	 * @param id
	 *            the identifier; may not be null
	 * @return the File object for the given identifier
	 * @see #isRoot(String)
	 * @see #isContentNode(String)
	 * @see #idFor(File)
	 */
	protected File fileFor(String id) {
		assert id.startsWith(DELIMITER);
		if (id.endsWith(DELIMITER)) {
			id = id.substring(0, id.length() - DELIMITER.length());
		}
		if (isContentNode(id)) {
			id = id.substring(0, id.length() - JCR_CONTENT_SUFFIX_LENGTH);
		}
		return new File(directory, id);
	}

	/**
	 * Utility method for determining if the node identifier is the identifier
	 * of the root node in this external source. Subclasses may override this
	 * method to change the format of the identifiers, but in that case should
	 * also override the {@link #fileFor(String)},
	 * {@link #isContentNode(String)}, and {@link #idFor(File)} methods.
	 * 
	 * @param id
	 *            the identifier; may not be null
	 * @return true if the identifier is for the root of this source, or false
	 *         otherwise
	 * @see #isContentNode(String)
	 * @see #fileFor(String)
	 * @see #idFor(File)
	 */
	protected boolean isRoot(String id) {
		return DELIMITER.equals(id);
	}

	protected String inventoryNameFromPath(String path) {

		String id = path;

		if (id.startsWith(DELIMITER)) {
			id = id.substring(DELIMITER.length());

		}
		if (id.endsWith(DELIMITER)) {
			id = id.substring(0, id.length() - DELIMITER.length());
		}
		if (isContentNode(id)) {
			id = id.substring(0, id.length() - JCR_CONTENT_SUFFIX_LENGTH);
		}

		return id;

	}

	protected ExternalBinaryValue binaryFor(String id) {
		try {
			// TODO byte[] sha1 = SecureHash.getHash(Algorithm.SHA_1, file);
			// BinaryKey key = new BinaryKey(sha1);
			String vaultName = ValutNameInPath(id);
			id = ArchiveIdInPath(id).replace(JCR_CONTENT_SUFFIX, "");
			BinaryKey key = new BinaryKey(id);

			return createBinaryValue(key, id, vaultName, id);
		} catch (RuntimeException e) {
			throw e;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	private String ValutNameInPath(String path) {
		int ind = path.indexOf('/', 1);

		if (ind > 1) {
			int offset = path.indexOf('/', 0);
			offset = (offset == 0) ? 1 : 0;
			return path.substring(offset, ind);
		}

		return null;
	}

	private String ArchiveIdInPath(String path) {
		int ind = path.indexOf('/', 1);

		if (ind > 1) {
			return path.substring(ind + 1);
		}

		return null;

	}

	protected ExternalBinaryValue createBinaryValue(BinaryKey key, String id,
			String vaultName, String archiveId) throws IOException {

		return new ArchiveBinaryValue(key, getSourceName(), accessKey,
				secretKey, region, "", vaultName, archiveId,
				glacierMessageListener, -1, id, getMimeTypeDetector())
				.withCacheRootDirectory(localCacheDirectory).withSQSQueueARN(
						glacierMessageListener.GetSQSQueueARN());
	}

	@Override
	public boolean hasDocument(String id) {

		return true;

	}

	private String getParentId(String id) {
		int index = id.lastIndexOf(DELIMITER);

		if (id.length() > 1 && index >= 0) {
			id = id.substring(0, index);
			return id.equalsIgnoreCase("") ? DELIMITER : id;

		}

		return null;
	}

	private String getChildName(String id) {
		int index = id.lastIndexOf(DELIMITER);

		if (id.length() > 1 && index >= 0) {
			id = id.substring(index + DELIMITER.length());
		}

		return id;
	}

	private boolean isVaultPath(String path) {
		return !isArchivePath(path);
	}

	private boolean isArchivePath(String path) {
		return path.matches(".+/.+");
	}

	@Override
	public Document getDocumentById(String id) {

		boolean isRoot = isRoot(id);
		DocumentWriter writer = null;
		boolean isResource = isContentNode(id);

		if (isResource) {
			System.out.println("isResource");
			writer = newDocument(id);
			BinaryValue binaryValue = binaryFor(id);
			writer.setPrimaryType(NT_RESOURCE);
			writer.addProperty(JCR_DATA, binaryValue);
			if (addMimeTypeMixin) {
				String mimeType = null;
				String encoding = null; // We don't really know this
				try {
					mimeType = binaryValue.getMimeType();
				} catch (Throwable e) {
					getLogger().error(e, JcrI18n.couldNotGetMimeType,
							getSourceName(), id, e.getMessage());
				}
				writer.addProperty(JCR_ENCODING, encoding);
				writer.addProperty(JCR_MIME_TYPE, mimeType);
			}
			writer.setNotQueryable();

		} else {

			if (isArchivePath(id)) {

				getLogger().trace("getDocumentById get an archive: " + id);

				writer = newDocument(id);
				writer.setPrimaryType(NT_FILE);
				// writer.setPrimaryType("bagit:cloudsFile");
				writer.addProperty(JCR_CREATED, factories().getDateFactory()
						.create());// TODO
				writer.addProperty(JCR_CREATED_BY, null); // ignored

				// writer.addMixinType(BAGIT_CLOUDFILE_TYPE);
				// writer.addProperty("bagit:absoluteURI",
				// providerUrlPrefix+DELIMITER+containerName+id);

				String childId = isRoot ? JCR_CONTENT_SUFFIX : id
						+ JCR_CONTENT_SUFFIX;
				writer.addChild(childId, JCR_CONTENT);

			} else {
				getLogger().trace("getDocumentById get a vault: " + id);

				try {
					writer = newVaultArchiveWriter(id, 0);

				} catch (IOException e) {

					e.printStackTrace();

					getLogger().trace(
							"getDocumentById exception: " + e.getStackTrace());

					throw new DocumentStoreException(id, e);
				}

			}

		}

		if (!isRoot) {
			// Set the reference to the parent ...

			writer.setParents(getParentId(id));
		}

		// Add the extra properties (if there are any), overwriting any
		// properties with the same names
		// (e.g., jcr:primaryType, jcr:mixinTypes, jcr:mimeType, etc.) ...
		writer.addProperties(extraPropertiesStore().getProperties(id));

		// Add the 'mix:mixinType' mixin; if other mixins are stored in the
		// extra properties, this will append ...
		if (addMimeTypeMixin) {
			writer.addMixinType(MIX_MIME_TYPE);
		}

		// Return the document ...
		return writer.document();
	}

	private DocumentWriter newVaultArchiveWriter(String path, int offset)
			throws JsonParseException, IOException {

		long totalChildren = 0;
		int nextOffset = 0;

		boolean root = isRoot(path);

		DocumentWriter writer = newDocument(path);
		writer.setPrimaryType(NT_FOLDER);
		writer.addProperty(JCR_CREATED, factories().getDateFactory().create());// TODO
		writer.addProperty(JCR_CREATED_BY, null); // ignored

		GlacierVaultArchive gva = new GlacierVaultArchive(localCacheDirectory);

		JsonNode archiveList = gva.VaultArchiveList(path);

		if (archiveList.size() > 0) {
			java.util.Iterator<JsonNode> iterator = archiveList.getElements();
			int i = 0;

			while (iterator.hasNext()) {
				JsonNode jn = iterator.next();
				String archiveId = jn.get("ArchiveId").getTextValue();

				System.out.println("Vault " + path + " has ArciveId "
						+ archiveId);

				totalChildren++;
				// only add a child if it's in the current page
				if (i >= offset && i < offset + pageSize) {
					// We use identifiers that contain the file/directory name
					writer.addChild(path + '/' + archiveId, archiveId);
					nextOffset = i + 1;

				}

				i++;
			}
		}

		// if there are still accessible children add the next page
		if (nextOffset < totalChildren) {
			writer.addPage(path, nextOffset, pageSize, totalChildren);
		}
		writer.setNotQueryable();

		return writer;
	}

	@Override
	public String getDocumentId(String path) {
		return path;
	}

	@Override
	public Collection<String> getDocumentPathsById(String id) {
		// this connector treats the ID as the path
		return Collections.singletonList(id);
	}

	@Override
	public ExternalBinaryValue getBinaryValue(String id) {

		return binaryFor(id);
	}

	@Override
	public boolean removeDocument(String id) {
		boolean isContentNode = isContentNode(id);
		String vaultName = ValutNameInPath(id);
		String archiveId = ArchiveIdInPath(id);

		if (isContentNode) {
			getLogger().trace(
					"Archive " + archiveId + " is deleted from Glacier vault "
							+ vaultName);

			archiveId = archiveId.substring(0, archiveId.length()
					- JCR_CONTENT_SUFFIX.length());

			AmazonGlacierClient client = GlacierClientFactory.newClient(
					accessKey, secretKey, "https://glacier." + region
							+ ".amazonaws.com/");

			try {
				GlacierUtil.delete(client, vaultName, archiveId);
			} catch (Exception e) {
				e.printStackTrace();
				throw new DocumentStoreException(id, e.getMessage());

			}
		}

		return true;
	}

	@Override
	public void storeDocument(Document document) {
		// Create a new directory or file described by the document ...
		DocumentReader reader = readDocument(document);
		String id = reader.getDocumentId();

		String vaultName = ValutNameInPath(id);
		String archiveId = ArchiveIdInPath(id);

		AmazonGlacierClient client = GlacierClientFactory.newClient(accessKey,
				secretKey, "https://glacier." + region + ".amazonaws.com/");

		boolean vaultExists = GlacierUtil.isVaultExists(client, vaultName);

		if (!vaultExists) {
			throw new DocumentStoreException(id,
					"Glacier vault does not exists.");
		}

		String primaryType = reader.getPrimaryTypeName();
		Map<Name, Property> properties = reader.getProperties();
		ExtraProperties extraProperties = extraPropertiesFor(id, false);
		extraProperties.addAll(properties).except(JCR_PRIMARY_TYPE,
				JCR_CREATED, JCR_LAST_MODIFIED, JCR_DATA);
		try {
			if (NT_FILE.equals(primaryType)) {
				// TODO file.createNewFile();
			} else if (NT_FOLDER.equals(primaryType)) {
				// TODO file.mkdirs();
			} else if (isContentNode(id)) {
				Property content = properties.get(JcrLexicon.DATA);
				BinaryValue binary = factories().getBinaryFactory().create(
						content.getFirstValue());
				GlacierUtil.upload(client, vaultName, id, binary.getStream(),
						binary.getSize());

				if (!NT_RESOURCE.equals(primaryType)) {
					// This is the "jcr:content" child, but the primary type is
					// non-standard so record it as an extra property
					extraProperties
							.add(properties.get(JcrLexicon.PRIMARY_TYPE));
				}
			}
			extraProperties.save();
		} catch (Exception e) {
			throw new DocumentStoreException(id, e);
		}
	}

	@Override
	public String newDocumentId(String parentId, Name newDocumentName,
			Name newDocumentPrimaryType) {
		StringBuilder id = new StringBuilder(parentId);
		if (!parentId.endsWith(DELIMITER)) {
			id.append(DELIMITER);
		}

		// We're only using the name to check, which can be a bit dangerous if
		// users don't follow the JCR conventions.
		// However, it matches what "isContentNode(...)" does.
		String childNameStr = getContext().getValueFactories()
				.getStringFactory().create(newDocumentName);
		if (JCR_CONTENT.equals(childNameStr)) {
			// This is for the "jcr:content" node underneath a file node. Since
			// this doesn't actually result in a file or folder
			// on the file system (it's merged into the file for the parent
			// 'nt:file' node), we'll keep the "jcr" namespace
			// prefix in the ID so that 'isContentNode(...)' works properly ...
			id.append(childNameStr);
		} else {
			// File systems don't universally deal well with ':' in the names,
			// and when they do it can be a bit awkward. Since we
			// don't often expect the node NAMES to contain namespaces (at leat
			// with this connector), we'll just
			// use the local part for the ID ...
			id.append(newDocumentName.getLocalName());
			if (!StringUtil.isBlank(newDocumentName.getNamespaceUri())) {
				// the FS connector does not support namespaces in names
				String ns = newDocumentName.getNamespaceUri();
				getLogger().warn(JcrI18n.fileConnectorNamespaceIgnored,
						getSourceName(), ns, id, childNameStr, parentId);
			}
		}
		return id.toString();
	}

	@Override
	public void updateDocument(DocumentChanges documentChanges) {
	}

	@Override
	public Document getChildren(PageKey pageKey) {
		// TODO
		return null;
	}

	/**
	 * Shutdown the connector by releasing all resources. This is called
	 * automatically by ModeShape when this Connector instance is no longer
	 * needed, and should never be called by the connector.
	 */
	public void shutdown() {
		getLogger().debug("shutdown is invoked. ");

		if (glacierMessageListenerThread != null
				&& glacierMessageListenerThread.isAlive())
			glacierMessageListenerThread.interrupt();
		GlacierMessageListener.clear();

		getLogger().trace("glacierMessageListenerThread is shutdown.");

	}
}