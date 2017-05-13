package com.ng.util;


import java.io.IOException;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import redis.clients.jedis.Jedis;

public class MessageConsumer 
{
    public static void main( String[] args ) throws IOException, ParseException
    {
        Jedis jedis = new Jedis("localhost");   
        List<String> messages = null;
        while(true){
			System.out.println("Waiting for a message in the queue");
			messages = jedis.blpop(0, "queue");
			System.out.println("Got the message");
			System.out.println("KEY:" + messages.get(0) + " VALUE:" + messages.get(1));
			String payload = messages.get(1);
			// Do some processing with the payload
			System.out.println("Message received:" + payload);
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject) jsonParser.parse(payload);
			ElasticSearch.storeData(ElasticSearch.getJestConnection(),jsonObject);
        }

    }
}