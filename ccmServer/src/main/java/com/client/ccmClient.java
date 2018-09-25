package com.client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.pack.InvalidOperationException;
import com.pack.ccmProviderService;

public class ccmClient {

	ccmProviderService.Client client;
	
	public ccmClient() {}

	public ccmProviderService.Client getClient() throws InvalidOperationException, TException, InterruptedException {
		if (client == null || !client.ping()) {
			TTransport transport = new TFramedTransport(new TSocket("localhost", 9090));
			TProtocol protocol = new TBinaryProtocol(transport);
			this.client = new ccmProviderService.Client(protocol);
			transport.open();
			Runtime.getRuntime().addShutdownHook(new Thread(() -> transport.close()));
			if (!client.ping()) {
				System.out.println("CCM Server is down...");
			}
		}
		return client;
	}
}