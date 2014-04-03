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

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.Property;
import org.junit.Before;
import org.junit.Test;
import org.modeshape.jcr.SingleUseAbstractTest;
import org.modeshape.jcr.api.JcrTools;
import org.modeshape.jcr.api.Session;
import org.modeshape.jcr.cache.DocumentStoreException;

public class GlacierConnectorTest extends SingleUseAbstractTest {

	protected static void printDocumentView(Session session, String path)
			throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		session.exportSystemView(path, baos, false, false);

		System.out.println(baos);

	}

	private JcrTools tools;

	public GlacierConnectorTest() {

	}

	@Before
	public void before() throws Exception {

		tools = new JcrTools();

		startRepositoryWithConfiguration(getClass().getClassLoader()
				.getResourceAsStream("repo-config-glacier.json"));
		registerNodeTypes("bagitCloudFile.cnd");
	}

	@Test
	public void shouldAllowDeletingNode() throws Exception {

		session.refresh(false);
		Node node = session
				.getNode("/glacierVault1/btNbDwIHtG6KfaUBMuQZABLPSFlzaeqCZxMGvp8bJysmBC9MdipZiRy0bDitLJeBjAXIgEZQ34ThTaosJoqkgNDtKJAUuhm3uzJFPXZPZBjrMQrxSz6yUl-TWEteBv7uRnwaRldntA");
		node.remove();
		session.save();
		System.out.println("delete done.");

	}

	@Test
	public void shouldAllowCreatingNode() throws Exception {

		String actualContent = "This is the content of the file in Glacier.";
		tools.uploadFile(session, "/glacierVault1/asfnew.txt",
				new ByteArrayInputStream(actualContent.getBytes()),
				"nt:folder", "bagit:cloudsFile");
		session.save();

		System.out.println("Upload is done.");

	}

	@Test
	public void shouldGetExceptionForNodeNotCached() throws Exception {

		Node file = session
				.getNode("/glacierVault1/AU3bZGWV7MioNg5coMyGXiG70XBicVCIyDGj1zvPkTYeK5fRzFJ_erEncLXoC_ag97AppVRyAl_-gm-hBs543EcCoEQb7bx-6LYqliQ1WPt7wtyzdPCLa1qd4Na4hq98PWX8hxFvlw");

		Node cnt = file.getNode("jcr:content");

		Property value = cnt.getProperty("jcr:data");

		Binary bv = (Binary) value.getValue().getBinary();

		InputStream is = null;
		try {
			is = bv.getStream();

		} catch (DocumentStoreException e) {
			System.err.println(e.getMessage());
			System.err.println("The archive should be got after 24 hours.");

		}

	}

	@Test
	public void shouldAccessBinaryContent() throws Exception {

		Session session = (Session) jcrSession();

		Node file = session
				.getNode("/glacierVault1/btNbDwIHtG6KfaUBMuQZABLPSFlzaeqCZxMGvp8bJysmBC9MdipZiRy0bDitLJeBjAXIgEZQ34ThTaosJoqkgNDtKJAUuhm3uzJFPXZPZBjrMQrxSz6yUl-TWEteBv7uRnwaRldntA");

		Node cnt = file.getNode("jcr:content");

		Property value = cnt.getProperty("jcr:data");

		Binary bv = (Binary) value.getValue().getBinary();

		InputStream is = null;
		try {
			is = bv.getStream();

		} catch (DocumentStoreException e) {
			System.err.println(e.getMessage());

		}

		System.out.println("archive content:");

		int content;
		while ((content = is.read()) != -1) {
			System.out.print((char) content);
		}

		System.out.println();
		System.out.println("Read Glacier node end");
	}

}
