package com.ng.util;

import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import com.ng.pojo.InsurancePlan;

@Repository
public class InsuranceDBConn {

	 
	    private static final String KEY = "APlans";
	     
	    private RedisTemplate<String, InsurancePlan> redisTemplate;
	    private HashOperations hashOps;
	 
	    @Autowired
	    public InsuranceDBConn(RedisTemplate redisTemplate) {
	        this.redisTemplate = redisTemplate;
	    }
	 
	    @PostConstruct
	    private void init() {
	        hashOps = redisTemplate.opsForHash();
	    }
	     
	    public void saveInsuranceInfo(InsurancePlan ip) {
	        hashOps.put(KEY, ip.getId(), ip.getContent());
	    }
	 
	    public void updateInsurancePlan(InsurancePlan ip) {
	        hashOps.put(KEY, ip.getId(), ip.getContent());
	    }
	 
	    public String findInsurancePlan(String id) {
	        String content= (String) hashOps.get(KEY, id);
	        return content;
	    }
	    public String findPlanRelationShips(String id) {
	        String content= (String) hashOps.get(KEY, id);
	        return content;
	    }
	 
	    public Map<String, String> findAllInsurancePlans() {
	    	Map<String, String> con = hashOps.entries(KEY);
	    	
	        return con;
	    }
	 
	    public void deleteInsurancePlan(String id) {
	        hashOps.delete(KEY, id);
	    }
	}