package com.elasticsearch.kafka.service.utilities;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.stereotype.Component;

//@Component
public class RestClientFactory {
	
	public static RestHighLevelClient getHighLevelClient() {

		// https://xgml3ka9qv:ys5srjvgbo@kafka-twitter-7404311451.us-west-2.bonsaisearch.net:443

		String hostname = "kafka-twitter-7404311451.us-west-2.bonsaisearch.net";
		String username = "xgml3ka9qv";
		String password = "ys5srjvgbo";

		// not required in case of local ES
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						// TODO Auto-generated method stub
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}

				});

		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
}
