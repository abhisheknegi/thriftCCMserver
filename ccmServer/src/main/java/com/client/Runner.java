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
		
		String type = "config_rank";
		//getAllValues(ccm,type);
		
		Map<String, String> keyvaluemap = new HashMap<>();
		keyvaluemap.put("other.settings.spark.cassandra.keyspace", "wmx_gm_pna");
		
		//
		keyvaluemap.put("other.settings.spark.batch.duration", "20");
		keyvaluemap.put("other.settings.tester", "spark://feeds-298183233-1-383317444.prod.gm-pna-streaming.mexicoecomm.dfw5.prod.walmart.com:7077");
		keyvaluemap.put("other.settings.loglevel", "WARN");
		keyvaluemap.put("other.settings.cassandra.edit.quorum", "EACH_QUORUM");
		keyvaluemap.put("other.settings.cassandra.read.quorum", "LOCAL_QUORUM");
		keyvaluemap.put("other.settings.external.logging", "true");
		
		//
		keyvaluemap.put("spark.context-settings.spark.app.name", "mx-pna-rank-data-ingestion");
		keyvaluemap.put("spark.context-settings.master", "spark://feeds-298183233-1-383317444.prod.gm-pna-streaming.mexicoecomm.dfw5.prod.walmart.com:7077");
		
		//
		keyvaluemap.put("spark.context-settings.spark.cores.max", "5");
		keyvaluemap.put("spark.context-settings.spark.executor.memory", "2G");
		
		//
		keyvaluemap.put("spark.context-settings.spark.batch.duration", "20");
		keyvaluemap.put("spark.context-settings.spark.driver.allowMultipleContexts", "true");
		keyvaluemap.put("spark.context-settings.spark.locality.wait", "20000");
		keyvaluemap.put("spark.context-settings.spark.submit.deployMode", "cluster");
		keyvaluemap.put("spark.context-settings.spark.shuffle.service.enabled", "false");
		keyvaluemap.put("spark.context-settings.spark.dynamicAllocation.enabled", "false");
		keyvaluemap.put("spark.context-settings.spark.dynamicAllocation.executorIdleTimeout", "20s");
		keyvaluemap.put("spark.context-settings.spark.dynamicAllocation.initialExecutors", "1");
		keyvaluemap.put("spark.context-settings.spark.cassandra.connection.local_dc", "cdc");
		keyvaluemap.put("spark.context-settings.spark.cassandra.connection.host", "cass-300917153-3-324207800.dev.wmx-gm-pna-dev.ms-df-cassandra.cdcstg2.prod.walmart.com,cass-300917153-2-324207797.dev.wmx-gm-pna-dev.ms-df-cassandra.cdcstg2.prod.walmart.com,cass-300917153-1-324207794.dev.wmx-gm-pna-dev.ms-df-cassandra.cdcstg2.prod.walmart.com");
		keyvaluemap.put("spark.context-settings.spark.cassandra.auth.username", "app");
		keyvaluemap.put("spark.context-settings.spark.cassandra.auth.password", "app");
		keyvaluemap.put("spark.context-settings.spark.cassandra.input.fetch.size_in_rows", "1000");
		keyvaluemap.put("spark.context-settings.spark.cassandra.input.split.size_in_mb", "500");
		keyvaluemap.put("spark.context-settings.spark.cassandra.connection.connections_per_executor_max", "10000");
		keyvaluemap.put("spark.context-settings.spark.cassandra.connection.keep_alive_ms", "60000");
		keyvaluemap.put("spark.context-settings.spark.cassandra.output.concurrent.writes", "1");
		keyvaluemap.put("spark.context-settings.spark.streaming.backpressure.enabled", "true");
		keyvaluemap.put("spark.context-settings.spark.streaming.backpressure.initialRate", "100");
		keyvaluemap.put("spark.context-settings.spark.streaming.kafka.maxRatePerPartition", "100");
		keyvaluemap.put("spark.context-settings.spark.streaming.receiver.maxRate", "100");
		keyvaluemap.put("kafka.config.bootstrap.servers", "kafka.kafka-cluster-shared.non-prod-1.walmart.com:9092");
		
		//
		keyvaluemap.put("kafka.config.group.id", "mx-rank-data-ingestion");
		keyvaluemap.put("kafka.config.auto.offset.reset", "latest");
		keyvaluemap.put("kafka.config.enable.auto.commit", "false");
		
		//
		keyvaluemap.put("kafka.settings.input.topics", "wmx-mg-pna-offer-calculation-events");
		keyvaluemap.put("kafka.settings.logging.topic", "wmx-mg-pna-offer-logging");
		
		//setAllValues(ccm, type, keyvaluemap);

		getGroup(ccm, type, "kafka.config");		
		System.out.println("\nBefore=> " + ccm.getSingleValue(type, "kafka.config.enable.auto.commit"));
		ccm.setSingleValue(type, "kafka.config.enable.auto.commit", "false");
		System.out.println("After=> " + ccm.getSingleValue(type, "kafka.config.enable.auto.commit"));
	}
	
	public static void getGroup(Client ccm, String type, String group) throws InvalidOperationException, TException {
		ccm.getGroupValues(type, group).entrySet().forEach(rec -> {
			System.out.println(rec.getKey() + ": " + rec.getValue());
		});
	}

	public static void getAllValues(Client ccm, String type) throws InvalidOperationException, TException {
		ccm.getAllValues(type).entrySet().forEach(rec -> {
			System.out.println(rec.getKey() + ": " + rec.getValue());
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
		
		getAllValues(ccm, type);
		
	}
	
}