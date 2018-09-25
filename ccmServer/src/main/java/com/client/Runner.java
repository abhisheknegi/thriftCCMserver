package com.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;

import com.pack.InvalidOperationException;
import com.pack.ccmProviderService.Client;

public class Runner {
	
	public static void main(String[] args) throws InvalidOperationException, TException, InterruptedException, IOException {
		ccmClient client = new ccmClient();
		Client ccm = client.getClient();
		
		String type = "config";
		getAllValues(ccm, type);
		
	}
	
	public static void getGroup(Client ccm, String type, String group) throws InvalidOperationException, TException {
		ccm.getGroupValues(type, group).entrySet().forEach(rec -> {
			System.out.println(rec.getKey() + ":" + rec.getValue());
		});
	}

	public static void getAllValues(Client ccm, String type) throws InvalidOperationException, TException {
		ccm.getAllValues(type).entrySet().forEach(rec -> {
			System.out.println(rec.getKey() + ":" + rec.getValue());
		});
	}
	
	public static void setSingleValue(Client ccm, String type, String key, String value) throws InvalidOperationException, TException {
		ccm.setSingleValue(type, key, value);
	}
	
	public static void setAllValues(Client ccm, String type, Map<String, String> keyvaluemap) throws InvalidOperationException, TException {
		
		keyvaluemap.entrySet().forEach(rec -> {
			try {
				ccm.setSingleValue(type, rec.getKey(), rec.getValue());
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}
	
}