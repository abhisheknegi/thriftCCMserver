package com.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;

import com.pack.InvalidOperationException;
import com.pack.ccmProviderService.Client;

public class Runner {
	
	private static String f = "spark.context-settings.spark.app.name,"
			+ "spark.context-settings.spark.cores.max,"
			+ "spark.context-settings.spark.batch.duration,"
			+ "kafka.config.auto.offset.reset,kafka.settings.input.topics,"
			+ "kafka.config.bootstrap.servers,"
			+ "kafka.config.group.id,"
			+ "spark.context-settings.spark.cassandra.connection.host,"
			+ "other.settings.spark.cassandra.keyspace,"
			+ "other.settings.external.logging,"
			+ "other.settings.cassandra.edit.quorum,"
			+ "other.settings.cassandra.read.quorum,"
			+ "spark.context-settings.master";

	//public static void notMain(String[] args)
	public static void notMain()
			throws InvalidOperationException, TException, InterruptedException, IOException {
		ccmClient client = new ccmClient();
		Client ccm = client.getClient();
		
		String type = "config_logging";
		
		Map<String, String> configs = new HashMap<>();
		configs.put("other.settings.tester", "spark://feeds-298183233-1-383317444.prod.gm-pna-streaming.mexicoecomm.dfw5.prod.walmart.com:7077");
		configs.put("spark.context-settings.master", "spark://feeds-298183233-1-383317444.prod.gm-pna-streaming.mexicoecomm.dfw5.prod.walmart.com:7077");
		configs.put("spark.context-settings.spark.cores.max", "5");
		
		//setForAlltype(ccm, configs);
		setForAtype(ccm, type, configs);
		
		getAllSettings(ccm);
		
		getOneSetting(ccm,"config_logging");

	}
	
	public static void getOneSetting(Client ccm, String type) throws InvalidOperationException, TException {
		for (String field : f.split(",")) {
			System.out.println(field + "===> " + ccm.getSingleValue(type, field));
		}
	}

	public static void getAllSettings(Client ccm) throws InvalidOperationException, TException {
		ccm.getTypes().stream().forEach(t -> {
			try {
				System.out.println(t);
				System.out.println(" ");
				for (String field : f.split(",")) {
					System.out.println(field + "===> " + ccm.getSingleValue(t, field));
				}
				System.out.println(" ");
				System.out.println(" ");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		});
	}
	
	public static void setForAlltype(Client ccm, Map<String, String> configs) throws InvalidOperationException, TException {
		
		ccm.getTypes().stream().forEach(t -> {
			configs.entrySet().forEach(c -> {
				try {
					ccm.setSingleValue(t, c.getKey(), c.getValue());
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		});
	}
	
	public static void setForAtype(Client ccm, String type, Map<String, String> configs) throws InvalidOperationException, TException {
		configs.entrySet().forEach(x -> {
			try {
				ccm.setSingleValue(type, x.getKey(), x.getValue());
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}
	
}