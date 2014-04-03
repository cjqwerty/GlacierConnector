package org.fcrepo.federation.glacierconnector;

import java.io.FileReader;
import java.io.IOException;

//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
//import org.json.simple.parser.JSONParser;
//import org.json.simple.parser.ParseException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

public class GlacierVaultArchive {

	private String parentPath;

	public GlacierVaultArchive(String parentPath) {
		this.parentPath = parentPath;

	}

	public JsonNode VaultArchiveList(String vaultName)
			throws JsonParseException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory factory = mapper.getJsonFactory();
		FileReader reader = new FileReader(parentPath + vaultName + vaultName);

		JsonParser jp = factory.createJsonParser(reader);
		return mapper.readTree(jp).get("ArchiveList");

	}

}
