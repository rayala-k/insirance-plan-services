package com.ng.pojo;

import java.io.Serializable;


public class InsurancePlan implements Serializable {

	private final String jsonId;
	private final String jsonContent;

	public InsurancePlan(String id, String content) {
		this.jsonId = id;
		this.jsonContent = content;
	}

	public String getId() {
		return jsonId;
	}

	public String getContent() {
		return jsonContent;
	}
}
