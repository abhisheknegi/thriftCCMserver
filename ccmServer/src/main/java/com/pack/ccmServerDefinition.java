package com.pack;

import java.sql.Connection;
import java.sql.SQLException;

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
import com.util.localDatabaseConfigs;

public class ccmServerDefinition {

	public static void start(localDatabaseConfigs conf, int port) throws TTransportException {

		TNonblockingServerTransport transport = new TNonblockingServerSocket(port);
		TNonblockingServer.Args nBlockServer = new TNonblockingServer.Args(transport);

		ccmProviderServiceImpl impl = new ccmProviderServiceImpl(conf);
		Processor<ccmProviderServiceImpl> processor = new ccmProviderService.Processor<>(impl);

		TServer server = new TNonblockingServer(nBlockServer.processor(processor));

		ccmServerEventHandler cse = new ccmServerEventHandler();
		server.setServerEventHandler(cse);

		server.serve();
	}

	public static void main(String[] args) throws TTransportException, SQLException {
		if (args.length > 1) {
			start(setupDbConnection(args[0]), Integer.valueOf(args[1]));
		} else {
			System.out.println("ERROR: Required input not present....processing exiting.");
			Runtime.getRuntime().exit(-1);
		}
	}

	public static localDatabaseConfigs setupDbConnection(String env) throws SQLException {
		localDatabaseConfigs conf = new localDatabaseConfigs(env);
		Connection conn = conf.getDbConnection();
		if(!conn.isClosed()) {
			return conf;
		}else {
			System.out.println("ERROR: Check DB Connection....processing exiting.");
			Runtime.getRuntime().exit(-1);
			return null;
		}
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
			//ccmServerContext ctx = (ccmServerContext) serverContext;
			//System.out.println("Request processed for connection # " + ctx.getConnectionId());
		}
	}
}