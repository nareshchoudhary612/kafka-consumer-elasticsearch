package com.elasticsearch.kafka.service.utilities;

import com.google.gson.JsonParser;
import org.springframework.stereotype.Component;

//@Component
public class TweetJsonParsor {
	
	

	public static String extractIDFromTweet(String tweet) {
		
		JsonParser jsonParser = new JsonParser();
		return jsonParser.parse(tweet).getAsJsonObject().get("id_str").getAsString();
		

	}
}
