package org.fcrepo.federation.glacierconnector;

import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClient;

public class GlacierClientFactory {

	//default 
	public static AmazonGlacierClient newClient() throws IOException {
		AWSCredentials credentials = new PropertiesCredentials(
				GlacierClientFactory.class
						.getResourceAsStream("AwsCredentials.properties"));
		AmazonGlacierClient client = new AmazonGlacierClient(credentials);
		client.setEndpoint("https://glacier.us-east-1.amazonaws.com/");

		return client;

	}
	
	public static AmazonGlacierClient newClient(String accessKey,String secretKey,String endpoint){
		
		BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey,secretKey);
		
		AmazonGlacierClient client = new AmazonGlacierClient(credentials);
        client.setEndpoint(endpoint);
              
        return client;

	}
	

}
