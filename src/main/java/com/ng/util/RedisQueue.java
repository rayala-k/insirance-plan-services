package com.ng.util;

import java.io.IOException;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import io.searchbox.client.JestClient;
import redis.clients.jedis.Jedis;

public class RedisQueue {
	private JestClient client;

	public RedisQueue(JestClient client) {
		this.client = client;
	}

	Jedis jedis = new Jedis("localhost");

	public void addToQueue(JSONObject jsonObject) {

		jedis.rpush("queue", jsonObject.toString());

	}

/*	public void removeFromQueue() throws IOException, ParseException {
		List<String> messages = null;
	//	ElasticSearch.deleteIndex(client);
		
	//	while (true) {
			System.out.println("Waiting for a message in the queue");
			messages = jedis.blpop(0, "queue");
			System.out.println("Got the message");
			System.out.println("KEY:" + messages.get(0) + " VALUE:" + messages.get(1));
			String payload = messages.get(1);
			// Do some processing with the payload
			System.out.println("Message received:" + payload);
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject) jsonParser.parse(payload);
			ElasticSearch.storeData(client,jsonObject);

	//	}
	}*/

}
