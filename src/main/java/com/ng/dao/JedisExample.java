package com.ng.dao;

import redis.clients.jedis.Jedis;

public class JedisExample {

	public static void ping() {
		Jedis jedis = new Jedis("127.0.0.1", 6379);
//		jedis.auth("password");

		System.out.println("Connected to Redis");

		jedis.set("foo", "bar");
		String value = jedis.get("foo");
		System.out.println(value);
	}

}
