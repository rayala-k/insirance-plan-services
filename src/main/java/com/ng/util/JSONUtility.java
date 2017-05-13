package com.ng.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.ng.pojo.InsurancePlan;

public class JSONUtility {
	public InsuranceDBConn insuranceDBConn;
	public long counter;

	public static Map<String, Object> jsonToMap(JSONObject json) {
		Map<String, Object> retMap = new HashMap<String, Object>();

		if (json != null) {
			retMap = toMap(json);
		}
		return retMap;
	}

	public static Map<String, Object> toMap(JSONObject object) {
		Map<String, Object> map = new HashMap<String, Object>();

		Iterator<String> keysItr = object.keySet().iterator();
		while (keysItr.hasNext()) {
			String key = keysItr.next();
			Object value = object.get(key);

			if (value instanceof JSONArray) {
				value = toList((JSONArray) value);
			}

			else if (value instanceof JSONObject) {
				value = toMap((JSONObject) value);
			}
			map.put(key, value);
		}
		return map;
	}

	public static List<Object> toList(JSONArray array) {
		List<Object> list = new ArrayList<Object>();
		for (int i = 0; i < array.size(); i++) {
			Object value = array.get(i);
			if (value instanceof JSONArray) {
				value = toList((JSONArray) value);
			}

			else if (value instanceof JSONObject) {
				value = toMap((JSONObject) value);
			}
			list.add(value);
		}
		return list;
	}

	public static boolean map(JSONObject oldObject, String searchedKey, Object value) {
		Map<String, Object> mappedone = jsonToMap(oldObject);
		return isKeyExisting(mappedone, searchedKey, value);
	}

	public static boolean isKeyExisting(Map mappedone, String searchedKey, Object value) {

		Iterator<String> keys = mappedone.keySet().iterator();
		while (keys.hasNext()) {
			String key = keys.next();

			if (mappedone.get(key) instanceof Map) {
				isKeyExisting((Map) mappedone.get(key), searchedKey, value);
			}
		}

		return true;
	}

	public static JSONObject patch(org.json.simple.JSONObject jsonObject1, org.json.simple.JSONObject jsonObject) {

		Iterator<?> keys = jsonObject.keySet().iterator();
		while (keys.hasNext()) {
			String key = (String) keys.next();
			Object newValue = jsonObject.get(key);
			jsonObject1.put(key, newValue);

		}
		return jsonObject1;
	}

	public static boolean keyExists(JSONObject object, String searchedKey, Object value) {
		boolean exists = object.containsKey(searchedKey);
		if (!exists) {
			Iterator<String> keys = object.keySet().iterator();
			while (keys.hasNext()) {
				String key = keys.next();
				if (object.get(key) instanceof Map) {
					JSONObject map = (JSONObject) object.get(key);
					exists = keyExists(map, searchedKey, value);

				}

			}

		}
		return exists;
	}

	public static String getCurrentTimeStamp() {
		Date date = new java.util.Date();
		return new Timestamp(date.getTime()).toString();
	}

	public static String getCalenderInstance() {

		return Calendar.getInstance().getTime().toString();
	}

	public static boolean checkIfModfied(String clientLastModfiedTimeStamp, String redisLastModfiedTimeStamp)
			throws ParseException {
		SimpleDateFormat s1 = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
		Date dateOne = s1.parse(clientLastModfiedTimeStamp);
		Date dateTwo = s1.parse(redisLastModfiedTimeStamp);
		if (dateOne.equals(dateTwo)) {
			return false;
		} else {
			return true;
		}
	}

	public String jsonToMapAndStoreInRedis(JSONObject json, InsuranceDBConn insuranceDBConn, String uuid) {
		this.insuranceDBConn = insuranceDBConn;
		if (json != null) {
			return toMap1(json, uuid);
		}
		return null;
	}

	public String toMap1(JSONObject object, String uuid) {
		Map<String, Object> simplePropertiesMap = new HashMap<String, Object>();
		/// id generation
		String localCount = uuid;
		Iterator<String> keysItr = object.keySet().iterator();
		Set relationship = new HashSet();
		while (keysItr.hasNext()) {
			String key = keysItr.next();
			Object value = object.get(key);

			if (value instanceof Map) {
				// id_relationship,Arraylist09alue--key
				// key+uuid value--redis
				String subUUID = JSONUtility.generateUUID();
				if (!checkIfRef((Map) value)) {
					relationship.add(key + "_" + subUUID);
				} else {
					relationship.add("ref_" + key + "_" + subUUID);
				}
				iterateMap(key + "_" + subUUID, (Map) value, localCount);

			} else if (value instanceof ArrayList) {
				List valuesList = new ArrayList();
				Set secondaryRelationship = new HashSet();
				String mainArrKey = key + "_" + JSONUtility.generateUUID();
				;
				for (Object val : (List) value) {

					if (val instanceof Map) {
						// serviceList_1
						relationship.add(mainArrKey);
						String secUUID = JSONUtility.generateUUID();
						if (!checkIfRef((Map) val)) {
							secondaryRelationship.add(mainArrKey + "_" + secUUID);
						} else {
							secondaryRelationship.add("ref_" + mainArrKey + "_" + secUUID);
						} // and
							// serviceLiset_1_2
						String nextKey = mainArrKey + "_" + secUUID;

						iterateMap(nextKey, (Map) val, secUUID);
					} else {
						valuesList.add(val);
					}

				}
				if (valuesList.size() > 0) {
					simplePropertiesMap.put("uuid", uuid);
					simplePropertiesMap.put(key, valuesList);
				}
				if (secondaryRelationship.size() > 0) {
					InsurancePlan secentityRelationship = new InsurancePlan(mainArrKey, secondaryRelationship.toString());
					insuranceDBConn.saveInsuranceInfo(secentityRelationship);
				}
			}

			else {
				// id,map---redis
				simplePropertiesMap.put("uuid", uuid);
				simplePropertiesMap.put(key, value);

			}

		}
		// Adding simple properties with main plan key in redis
		String planKey = "plan_" + localCount;
		if (!simplePropertiesMap.isEmpty()) {
			InsurancePlan entity = new InsurancePlan(planKey, new JSONObject(simplePropertiesMap).toString());
			insuranceDBConn.saveInsuranceInfo(entity);
		}

		// Storing relationships with main plan relationship key
		if (relationship.size() > 0) {
			InsurancePlan entityRelationship = new InsurancePlan("rel_" + planKey, relationship.toString());
			insuranceDBConn.saveInsuranceInfo(entityRelationship);
		}

		return "plan/"+localCount;
	}

	private void iterateMap(String mainKey, Map valueMap, String localCount) {

		Map<String, Object> simplePropertiesMap = new HashMap<String, Object>();
		Iterator<String> keysItr = valueMap.keySet().iterator();
		Set relationship = new HashSet();
		while (keysItr.hasNext()) {
			String key = keysItr.next();
			Object value = valueMap.get(key);

			if (value instanceof Map) {
				// id_relationship,Arraylist value--key
				// key+uuid value--redis
				String newUUIDSecondLevel = JSONUtility.generateUUID();
				if (!checkIfRef((Map) value)) {
					relationship.add(key + "_" + newUUIDSecondLevel);
				} else {
					relationship.add("ref_" + key + "_" + newUUIDSecondLevel);
				}

				iterateMap(key + "_" + newUUIDSecondLevel, (Map) value, newUUIDSecondLevel);

			} else if (value instanceof ArrayList) {
				List valuesList = new ArrayList();
				String arrylistCount = localCount;
				for (Object val : (List) value) {
					if (val instanceof Map) {
						iterateMap(key + "_", (Map) val, arrylistCount);
					} else {
						valuesList.add(val);
					}
				}
				simplePropertiesMap.put(key, valuesList);

			} else {
				// id,map---redis
				simplePropertiesMap.put("uuid", mainKey.substring(mainKey.indexOf('_') + 1, mainKey.length()));
				simplePropertiesMap.put(key, value);

			}
		}
		// Adding simple properties with main plan key in redis

		if (!simplePropertiesMap.isEmpty() || relationship.size() > 0) {

			InsurancePlan entity = new InsurancePlan(mainKey, new JSONObject(simplePropertiesMap).toString());
			insuranceDBConn.saveInsuranceInfo(entity);
		}

		// Storing realtionships with main plan relationship key
		if (relationship.size() > 0) {
			InsurancePlan entityRelationship = new InsurancePlan("rel_" + mainKey, relationship.toString());
			insuranceDBConn.saveInsuranceInfo(entityRelationship);
		}

	}

	public boolean checkIfRef(Map map) {
		if (map.containsKey("ref") && null != map.get("ref")) {
			return (boolean) map.get("ref");

		}
		return false;

	}

	public static String generateEtag(String jsonObject) throws UnsupportedEncodingException, NoSuchAlgorithmException {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] array = md.digest(jsonObject.getBytes());
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < array.length; ++i) {
				sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
			}
			return sb.toString();
		} catch (java.security.NoSuchAlgorithmException e) {
		}
		return null;

	}

	public static String generateUUID() {
		return UUID.randomUUID().toString();
	}
}
