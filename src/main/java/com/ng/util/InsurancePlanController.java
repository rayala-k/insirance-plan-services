package com.ng.util;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.security.RolesAllowed;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.WebRequest;

import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.ng.pojo.InsurancePlan;

import io.searchbox.client.JestClient;

@RestController
@Component
public class InsurancePlanController {

	public InsurancePlanController() throws IOException {
		this.client = ElasticSearch.getJestConnection();
		this.redisQueue = new RedisQueue(client);
		ElasticSearch.createNewIndex(client);
	}

	private Map<String, Map<String, Object>> map = new HashMap<String, Map<String, Object>>();
	private JestClient client;
	private RedisQueue redisQueue;

	// working
	@RequestMapping(value = "/{urlTypes}/{id}", method = RequestMethod.GET)
	@ResponseBody
	public JSONObject getSpecificData(@PathVariable String id, @PathVariable String urlTypes, WebRequest webRequest,
			HttpServletRequest request, HttpServletResponse response) throws java.text.ParseException, ParseException {
		JSONParser jsonParser = new JSONParser();

		String etagFromRequest = request.getHeader(HttpHeaders.IF_NONE_MATCH);

		String etagFromRedis = insurancedbConn.findInsurancePlan("etag_" + urlTypes + "_" + id);
		if (etagFromRequest == null || !etagFromRequest.equals(etagFromRedis)) {
			String idNew = urlTypes + "_" + id;
			return getPlan(idNew, jsonParser);

		} else {

			JSONObject jsonObject = new JSONObject();
			jsonObject.put("error", "No change in object");
			response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
			return jsonObject;
		}

	}

	// working
	@RequestMapping(value = "/{urlTypes}", method = RequestMethod.POST)
	@RolesAllowed({ "admin" })
	public String addInsuranceData(@RequestBody JSONObject jsonObject, @PathVariable String urlTypes,
			HttpServletResponse response)
			throws ProcessingException, IOException, ParseException, NoSuchAlgorithmException {
		String key = "plan";
		if (urlTypes.equalsIgnoreCase("plan")) {
			key = "plan";
		} else if (urlTypes.equalsIgnoreCase("service")) {
			key = "service";
		} else {
			addSchemaToRedis();
			return "schema added";
		}
		if (validateJSON(jsonObject)) {
			String tagCount = JSONUtility.generateUUID();
			// adding object to queue
			JSONObject jsonObjectForElasticSearch = jsonObject;
			jsonObjectForElasticSearch.put("uuid", tagCount);
			response.setHeader(HttpHeaders.IF_NONE_MATCH, addPostDataToRedisEtag(jsonObject, tagCount));
			String finalObj = addPostDataToRedis(jsonObject, tagCount);
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject2 = getPlan("plan_" + tagCount, jsonParser);
			redisQueue.addToQueue(jsonObject2);
			return finalObj;
		} else {
			return "failed";
		}

	}
	// working

	@RequestMapping(value = "/{urlTypes}/{id}", method = RequestMethod.PUT)
	public String updateInsuranceData(@RequestBody JSONObject jsonObject, @PathVariable String id,
			@PathVariable String urlTypes, HttpServletResponse response)
			throws NoSuchAlgorithmException, ProcessingException, IOException, ParseException {
		if (validateJSON(jsonObject)) {
			ElasticSearch.deleteEntryFromIndex(client, id);
			jsonObject.put("uuid", id);

			updateDataToRedis(jsonObject, id, urlTypes);
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject2 = getPlan("plan_" + id, jsonParser);
			ElasticSearch.storeData(client, jsonObject2);
			InsurancePlan etag = new InsurancePlan("etag_" + id, JSONUtility.generateEtag(jsonObject.toString()));
			insurancedbConn.saveInsuranceInfo(etag);
			response.setHeader(HttpHeaders.IF_NONE_MATCH, JSONUtility.generateEtag(jsonObject.toString()));

			return "success from put";
		} else {
			return "json schema validation failed";
		}

	}

	// working
	@RequestMapping(value = "/{urlTypes}/{id}", method = RequestMethod.DELETE)
	public String deleteInsuranceData(@PathVariable String id, @PathVariable String urlTypes) throws IOException {
		// delete from elastic search
		ElasticSearch.deleteEntryFromIndex(client, id);

		id = urlTypes + "_" + id;
		deleteRecursively(id);
		insurancedbConn.deleteInsurancePlan(id);
		return "success from delete";

	}

	// working
	@RequestMapping(value = { "{parentId}/{urlTypes}/{id}",
			"{parentId}/{urlTypes}/{id}/{subId}" }, method = RequestMethod.PATCH)
	public String patchGreeting(@RequestBody JSONObject newJsonObject, @PathVariable String id,
			@PathVariable String urlTypes, @PathVariable Optional<String> subId, @PathVariable String parentId)
			throws ParseException, IOException {
		id = urlTypes + "_" + id;
		if (subId.isPresent()) {
			id = id + "_" + subId.get();
		}

		JSONParser jsonParser = new JSONParser();
		String content = insurancedbConn.findInsurancePlan(id);
		JSONObject oldJSONObject = (JSONObject) jsonParser.parse(content);
		JSONObject finalJSON = JSONUtility.patch(oldJSONObject, newJsonObject);
		InsurancePlan insurancePlan = new InsurancePlan(id, finalJSON.toString());
		insurancedbConn.saveInsuranceInfo(insurancePlan);
		JSONObject jsonObject2 = getPlan("plan_" + parentId, jsonParser);
		ElasticSearch.patchEntryFromIndex(client, parentId, jsonObject2);
		return "success from patch for " + id;

	}

	@RequestMapping(value = "/jsonPath", method = RequestMethod.GET)
	public Object getJSONPath(@RequestParam("data") String itemid) throws Exception {
		JsonObject jsonResult = ElasticSearch.readAllData(client);
		Object document = Configuration.defaultConfiguration().jsonProvider().parse(jsonResult.toString());
		Object result = JsonPath.read(document, itemid);
		return result;

	}

	private JSONObject getPlan(String id, JSONParser jsonParser) throws ParseException {

		String simplePropertiesMap = insurancedbConn.findInsurancePlan(id);
		if (null != simplePropertiesMap && !simplePropertiesMap.isEmpty()) {
			JSONObject jsonObject = (JSONObject) jsonParser.parse(simplePropertiesMap);
			createRelations(id, jsonParser, jsonObject);
			return jsonObject;
		} else {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("error", "plan does not exists");
			return jsonObject;
		}
	}

	private void createRelations(String id, JSONParser jsonParser, JSONObject jsonObject) throws ParseException {
		String str = insurancedbConn.findPlanRelationShips("rel_" + id);
		if (null != str && !str.isEmpty()) {
			str = str.replace("[", "").replace("]", "");
			String[] relationShips = str.split(",");
			for (String rel : relationShips) {
				rel = rel.trim();
				String nestSimpleProp = insurancedbConn.findInsurancePlan(rel.replaceAll("ref_", ""));
				if (!nestSimpleProp.contains("[")) {
					JSONObject jsonObject1 = (JSONObject) jsonParser.parse(nestSimpleProp);
					rel = rel.replaceAll("ref_", "");

					createRelations(rel, jsonParser, jsonObject1);
					jsonObject.put(rel.substring(0, rel.indexOf("_")), jsonObject1);

				} else {
					nestSimpleProp = nestSimpleProp.replace("[", "").replace("]", "");
					String[] nestSimplePropArr = nestSimpleProp.split(",");
					JSONArray jsonArray = new JSONArray();
					for (String rel1 : nestSimplePropArr) {
						String rel1value = insurancedbConn.findInsurancePlan(rel1.trim().replaceAll("ref_", ""));
						JSONObject jsonObject1 = (JSONObject) jsonParser.parse(rel1value);
						jsonArray.add(jsonObject1);
					}
					jsonObject.put(rel.toString().substring(0, rel.toString().indexOf("_")), jsonArray);
				}
			}
		}
	}

	private void deleteRecursively(String id) {
		String str = insurancedbConn.findPlanRelationShips("rel_" + id);
		str = str.replace("[", "").replace("]", "");
		String[] relationShips = str.split(",");
		for (String rel : relationShips) {
			String nestSimpleProp = insurancedbConn.findInsurancePlan(rel.trim());
			if (null != nestSimpleProp) {
				if (!nestSimpleProp.contains("[")) {
					deleteRecursively(nestSimpleProp);
					if (!rel.contains("ref_")) {
						insurancedbConn.deleteInsurancePlan(rel.trim());
					}
				} else {
					nestSimpleProp = nestSimpleProp.replace("[", "").replace("]", "");
					String[] nestSimplePropArr = nestSimpleProp.split(",");

					for (String rel1 : nestSimplePropArr) {
						if (!rel1.contains("ref_")) {
							insurancedbConn.deleteInsurancePlan(rel1.trim());
						}
					}
					insurancedbConn.deleteInsurancePlan(rel.trim());
				}
			}
		}
		insurancedbConn.deleteInsurancePlan("rel_" + id);
	}

	private void deleteRecursivelyFromPut(String id) {
		String str = insurancedbConn.findPlanRelationShips("rel_" + id);
		if (null != str && !str.isEmpty()) {
			str = str.toString().replace("[", "").replace("]", "");
			String[] relationShips = str.split(",");
			for (String rel : relationShips) {
				String nestSimpleProp = insurancedbConn.findInsurancePlan(rel.trim().toString());
				if (null != nestSimpleProp) {
					if (!nestSimpleProp.contains("[")) {
						deleteRecursively(nestSimpleProp);
						insurancedbConn.deleteInsurancePlan(rel.trim());
					} else {
						nestSimpleProp = nestSimpleProp.toString().replace("[", "").replace("]", "");
						String[] nestSimplePropArr = nestSimpleProp.split(",");

						for (String rel1 : nestSimplePropArr) {
							insurancedbConn.deleteInsurancePlan(rel1.trim());
						}
						insurancedbConn.deleteInsurancePlan(rel.trim());
					}
				}
			}
			insurancedbConn.deleteInsurancePlan("rel_" + id);
		}
	}

	public boolean validateJSON(JSONObject jsonObject) throws ProcessingException, IOException, ParseException {
		JSONParser jsonParser = new JSONParser();
		String jsonSchema = insurancedbConn.findInsurancePlan("jsonSchema");
		JSONObject jsonSchemaObj = (JSONObject) jsonParser.parse(jsonSchema);

		if (ValidationUtils.isJsonValid(jsonSchemaObj.toJSONString(), jsonObject.toJSONString())) {
			return true;
		} else {
			return false;
		}

	}

	public void addSchemaToRedis() throws FileNotFoundException, IOException, ParseException {
		JSONParser parser = new JSONParser();

		Object obj;
		obj = parser.parse(new FileReader("D:/files/schema-1.json"));
		JSONObject jsonSchema = (JSONObject) obj;

		InsurancePlan insurancePlan = new InsurancePlan("jsonSchema", jsonSchema.toString());
		insurancedbConn.saveInsuranceInfo(insurancePlan);

	}

	@Autowired
	private InsuranceDBConn insurancedbConn;

	public String addPostDataToRedis(JSONObject jsonObject, String tagCount)
			throws UnsupportedEncodingException, NoSuchAlgorithmException {
		JSONUtility jsonUitility = new JSONUtility();

		/*
		 * String etagValue = JSONUitility.generateEtag(jsonObject.toString());
		 * insurancePlan etag = new insurancePlan("etag_plan_" + tagCount,
		 * etagValue);
		 */

		return jsonUitility.jsonToMapAndStoreInRedis(jsonObject, insurancedbConn, tagCount);

		// insurancedbConn.saveinsurancePlan(etag);
		// return etagValue;

	}

	public String addPostDataToRedisEtag(JSONObject jsonObject, String tagCount)
			throws UnsupportedEncodingException, NoSuchAlgorithmException {

		String etagValue = JSONUtility.generateEtag(jsonObject.toString());
		InsurancePlan etag = new InsurancePlan("etag_plan_" + tagCount, etagValue);
		insurancedbConn.saveInsuranceInfo(etag);
		return etagValue;
	}

	public void updateDataToRedis(JSONObject jsonObject, String id, String key) {

		JSONUtility jsonUitility = new JSONUtility();
		deleteRecursivelyFromPut("plan_" + id);
		insurancedbConn.deleteInsurancePlan(id);
		jsonUitility.jsonToMapAndStoreInRedis(jsonObject, insurancedbConn, id);

	}

	public void mergeDataToRedis(JSONObject jsonObject, String id, String key) {
		InsurancePlan insurancePlan = new InsurancePlan(id, jsonObject.toString());
		insurancedbConn.updateInsurancePlan(insurancePlan);
		insurancedbConn.updateInsurancePlan(insurancePlan);
		map.put(insurancePlan.getId(), JSONUtility.jsonToMap(jsonObject));

	}

	public JSONObject getSpecficPlan(String id) throws ParseException {
		JSONParser jsonParser = new JSONParser();

		String simplePropertiesMap = insurancedbConn.findInsurancePlan(id);
		if (null != simplePropertiesMap && !simplePropertiesMap.isEmpty()) {
			JSONObject jsonObject = (JSONObject) jsonParser.parse(simplePropertiesMap);
			createRelations(id, jsonParser, jsonObject);
			return jsonObject;
		}

		return null;
	}

}
