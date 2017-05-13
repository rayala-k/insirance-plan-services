package com.ng.util;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONObject;

import com.google.gson.JsonObject;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.Update;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.mapping.PutMapping;

public class ElasticSearch {

	public static final String ELASTIC_INDEX_NAME = "insuranceindex";
	public static final String INSURANCEDATA = "insurancedata";

	public static JestClient getJestConnection() {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200").multiThreaded(true).build());
		JestClient client = factory.getObject();
		return client;
	}

	public static void createNewIndex(JestClient client) throws IOException {
		Settings.Builder settingsBuilder = Settings.builder();
		settingsBuilder.put("number_of_shards", 5);
		settingsBuilder.put("number_of_replicas", 1);

		PutMapping putMapping = new PutMapping.Builder(ELASTIC_INDEX_NAME, INSURANCEDATA,
				"{ \"insurancedata\" : { \"properties\" : { \"service\" : {\"type\" : \"nested\"} } } }").build();
		client.execute(putMapping);

		client.execute(new CreateIndex.Builder(ELASTIC_INDEX_NAME).build());
	}

	public static void storeData(JestClient client, JSONObject jsonData) throws IOException {
		Map<String, Object> source = JSONUtility.toMap(jsonData);
		String uuid = (String) jsonData.get("uuid");
		// Entity source = new Entity("1230022", "pranjal222");
		Index index = new Index.Builder(source).index(ELASTIC_INDEX_NAME).type(INSURANCEDATA).id(uuid).build();
		client.execute(index);

	}

	public static void deleteIndex(JestClient client) throws IOException {
		DeleteIndex deleteIndex = new DeleteIndex.Builder(ELASTIC_INDEX_NAME).build();
		client.execute(deleteIndex);

	}

	public static JsonObject readAllData(final JestClient jestClient) throws Exception {

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchQuery("_id", "d80ed0d7-4033-472d-a988-d95b40cb42ed"));

		Search search = new Search.Builder(searchSourceBuilder.toString())
				// multiple index or types can be added.
				.addIndex(ELASTIC_INDEX_NAME).addType(INSURANCEDATA).build();

		SearchResult result = jestClient.execute(search);
		// // SearchSourceBuilder searchSourceBuilder = new
		// SearchSourceBuilder();
		// searchSourceBuilder.query(QueryBuilders.termQuery("note", "see"));

		/*
		 * Search search = new Search.Builder(searchSourceBuilder.toString())
		 * .addIndex(ELASTIC_INDEX_NAME).addType(INSURANCEDATA).build();
		 */
		// System.out.println(searchSourceBuilder.toString());
		// JestResult result = jestClient.execute(search);
		// String jsonResult = result.getJsonObject();
		// JSONParser jsonParser = new JSONParser();
		// JSONObject jsonObject = (JSONObject) jsonParser.parse(jsonResult);
		return result.getSourceAsObject(JsonObject.class);

	}

	public static void deleteEntryFromIndex(JestClient client, String uuid) throws IOException {
		client.execute(new Delete.Builder(uuid).index(ELASTIC_INDEX_NAME).type(INSURANCEDATA).build());

	}
	public static void patchEntryFromIndex(JestClient client, String uuid,JSONObject jsonData) throws IOException {
		Map<String, Object> source = JSONUtility.toMap(jsonData);
		client.execute(new Update.Builder(source).index(ELASTIC_INDEX_NAME).type(INSURANCEDATA).id(uuid).build());

	}

}
