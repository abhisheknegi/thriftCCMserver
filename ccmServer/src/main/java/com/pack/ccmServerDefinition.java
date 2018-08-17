package com.pack;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.pack.ccmProviderService.Processor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ccmServerDefinition {

	public static void start(dataClass configData, String env, int port) throws TTransportException {

		TNonblockingServerTransport transport = new TNonblockingServerSocket(port);
		TNonblockingServer.Args nBlockServer = new TNonblockingServer.Args(transport);

		ccmProviderServiceImpl impl = new ccmProviderServiceImpl(configData, env);
		Processor<ccmProviderServiceImpl> processor = new ccmProviderService.Processor<>(impl);

		TServer server = new TNonblockingServer(nBlockServer.processor(processor));

		ccmServerEventHandler cse = new ccmServerEventHandler();
		server.setServerEventHandler(cse);
		
		server.serve();
	}

	//public static void main() throws TTransportException {
	public static void notMain(String[] args) throws TTransportException {
		String env = args[0];
		Integer port = Integer.valueOf(args[1]);
		start(initializeAllConfigs(env), env, port);
	}

	public static dataClass initializeAllConfigs(String env) {
		String folder = "/app/pna/ccm/" + env + "/";
		String configs = "config_offer.json," + "config_price.json," + "config_seller.json," + "config_avail.json,"
				+ "config_logging.json," + "config_rank.json";

		Map<String, Map<String, String>> directMaster = new HashMap<>();
		Map<String, Map<String, Map<String, String>>> groupMaster = new HashMap<>();

		for (String config : configs.split(",")) {
			File file = new File(folder + config);
			if (file.exists()) {
				directMaster.put(config.split("\\.")[0], directReferenceConfigs(config, file));
				groupMaster.put(config.split("\\.")[0], groupReferenceConfigs(config, file));
			} else {
				System.out.println("File doesn't exist...");
			}
		}

		dataClass response = new dataClass();
		response.setDirectMaster(directMaster);
		response.setGroupMaster(groupMaster);
		return response;
	}

	public static Map<String, String> directReferenceConfigs(String config, File file) {

		Config fromFile = ConfigFactory.parseFile(new File(file.getPath()));
		Map<String, String> directChild = new HashMap<>();
		fromFile.entrySet().forEach(rec -> {
			directChild.put(rec.getKey().replace("\"", ""), rec.getValue().render().replace("\"", ""));
		});

		return directChild;
	}

	public static Map<String, Map<String, String>> groupReferenceConfigs(String config, File file) {

		Config fromFile = ConfigFactory.parseFile(new File(file.getPath()));
		Map<String, Map<String, String>> groupChild = new HashMap<>();

		fromFile.entrySet().forEach(rec -> {
			if (rec.getKey().contains(".")) {
				String[] keys = rec.getKey().replace("\"", "").split("\\.");
				if (keys.length > 2) {
					String groupId = keys[0] + "." + keys[1];
					Map<String, String> values = new HashMap<>();
					if (!groupChild.containsKey(groupId)) {
						values = new HashMap<>();
						groupChild.put(groupId, values);
					} else {
						values = groupChild.get(groupId);
					}
					String key = rec.getKey().replace("\"", "").replace((keys[0] + "." + keys[1] + "."), "");
					values.put(key, rec.getValue().render().replaceAll("\"", ""));
				}
			}
		});
		return groupChild;
	}

	public static class ccmServerContext implements ServerContext {
		int connectionId = 0;
		public ccmServerContext() {
		}
		public ccmServerContext(int connectionId, String type) {
			this.connectionId = connectionId;
		}
		public int getConnectionId() {
			return connectionId;
		}
		public void setConnectionId(int connectionId) {
			this.connectionId = connectionId;
		}
	}

	public static class ccmServerEventHandler implements TServerEventHandler {
		private int id = 0;
		public void preServe() {
			System.out.println(
					"TServerEventHandler.preServe - called only once before server starts accepting connections");
		}
		public ServerContext createContext(TProtocol input, TProtocol output) {
			ccmServerContext ctx = new ccmServerContext();
			id++;
			ctx.setConnectionId(id);
			System.out.println("New connection #" + ctx.getConnectionId() + " established");
			return ctx;
		}
		public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
			ccmServerContext ctx = (ccmServerContext) serverContext;
			System.out.println("Connection #" + ctx.getConnectionId() + " terminated");
		}
		public void processContext(ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
			ccmServerContext ctx = (ccmServerContext) serverContext;
			System.out.println("Request processed for connection # " + ctx.getConnectionId());
		}
	}
}