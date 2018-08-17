package com.client;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.thrift.TException;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.pack.InvalidOperationException;
import com.pack.ccmProviderService;

@RestController
public class Controller {
	
	private ccmProviderService.Client ccmClient;
	private static Configuration conf = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL).build();
	private static final String TYPE = "$.type";
	private static final String GROUP = "$.group";
	private static final String KEY = "$.key";
	private static final String VALUE = "$.value";

	@RequestMapping(value = "/", method = RequestMethod.GET, produces = TEXT_PLAIN)
	public String getMethod() {
		return "API is Active !!";
	}
	@RequestMapping(value = "/pingServer", method = RequestMethod.GET, produces = TEXT_PLAIN)
	public String getPing() throws InvalidOperationException, TException, InterruptedException {
		if(ccmClient == null) {
			ccmClient = new ccmClient().getClient();
		}
		return String.valueOf(ccmClient.ping());
	}
	@RequestMapping(value = "/getAllValues", method = RequestMethod.POST, consumes = TEXT_PLAIN, produces = TEXT_PLAIN)
	public String getAllValues(@RequestBody String message) throws InterruptedException, ExecutionException, InvalidOperationException, TException {
		if(ccmClient == null) {
			ccmClient = new ccmClient().getClient();
		}
		DocumentContext inventDoc = JsonPath.using(conf).parse(message);
		List<String> str = new ArrayList<>();
		ccmClient.getAllValues(inventDoc.read(TYPE, String.class)).entrySet().forEach(q -> str.add(q.getKey() + " ==> " + q.getValue()));
		return String.join("\n", str);
	}
	@RequestMapping(value = "/getSingleValue", method = RequestMethod.POST, consumes = TEXT_PLAIN, produces = TEXT_PLAIN)
	public String getSingleValue(@RequestBody String message) throws InterruptedException, ExecutionException, InvalidOperationException, TException {
		if(ccmClient == null) ccmClient = new ccmClient().getClient();
		DocumentContext inventDoc = JsonPath.using(conf).parse(message);
		return ccmClient.getSingleValue(inventDoc.read(TYPE, String.class), inventDoc.read(KEY, String.class));
	}
	@RequestMapping(value = "/setSingleValue", method = RequestMethod.POST, consumes = TEXT_PLAIN, produces = TEXT_PLAIN)
	public String setSingleValue(@RequestBody String message) throws InterruptedException, ExecutionException, InvalidOperationException, TException {
		if(ccmClient == null) ccmClient = new ccmClient().getClient();
		DocumentContext inventDoc = JsonPath.using(conf).parse(message);
		String type = inventDoc.read(TYPE, String.class);
		String key = inventDoc.read(KEY, String.class);
		ccmClient.setSingleValue(inventDoc.read(TYPE, String.class), inventDoc.read(TYPE, String.class), inventDoc.read(VALUE, String.class));
		return ccmClient.getSingleValue(type, key);
	}
	@RequestMapping(value = "/getGroupValue", method = RequestMethod.POST, consumes = TEXT_PLAIN, produces = TEXT_PLAIN)
	public String getGroupValue(@RequestBody String message) throws InterruptedException, ExecutionException, InvalidOperationException, TException {
		if(ccmClient == null) ccmClient = new ccmClient().getClient();
		DocumentContext inventDoc = JsonPath.using(conf).parse(message);
		List<String> str = new ArrayList<>();
		ccmClient.getGroupValues(inventDoc.read(TYPE, String.class), inventDoc.read(GROUP, String.class)).entrySet().forEach(q -> str.add(q.getKey() + " ==> " + q.getValue()));
		return String.join("\n", str);
	}
	@RequestMapping(value = "/setGroupValue", method = RequestMethod.POST, consumes = TEXT_PLAIN, produces = TEXT_PLAIN)
	public String setGroupValue(@RequestBody String message) throws InterruptedException, ExecutionException, InvalidOperationException, TException {
		return "Method not implemented for REST Client.\nUse RPC client.";
	}
	@RequestMapping(value = "/getTypes", method = RequestMethod.GET, produces = TEXT_PLAIN)
	public String getTypes() throws InvalidOperationException, TException, InterruptedException {
		if(ccmClient == null) ccmClient = new ccmClient().getClient();
		return String.join("\n", ccmClient.getTypes());
	}
	@RequestMapping(value = "/getGroups", method = RequestMethod.POST, consumes = TEXT_PLAIN, produces = TEXT_PLAIN)
	public String getGroups(@RequestBody String message) throws InterruptedException, ExecutionException, InvalidOperationException, TException {
		if(ccmClient == null) ccmClient = new ccmClient().getClient();
		DocumentContext inventDoc = JsonPath.using(conf).parse(message);
		return String.join("\n", ccmClient.getGroups(inventDoc.read(TYPE, String.class)));
	}
	@RequestMapping(value = "/refreshConfigs", method = RequestMethod.POST, consumes = TEXT_PLAIN, produces = TEXT_PLAIN)
	public String refreshConfigs(@RequestBody String message) throws InterruptedException, ExecutionException, InvalidOperationException, TException {		
		return "Method not yet implemented";
	}
	@RequestMapping(value = "/persistConfigs", method = RequestMethod.POST, consumes = TEXT_PLAIN, produces = TEXT_PLAIN)
	public String persistConfigs(@RequestBody String message) throws InterruptedException, ExecutionException, InvalidOperationException, TException {
		return "Method not yet implemented";	
	}
}
