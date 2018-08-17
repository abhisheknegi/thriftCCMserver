package com.pack;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

public class ccmProviderServiceImpl implements ccmProviderService.Iface {
	dataClass configData;
	String env;
	public ccmProviderServiceImpl(dataClass configData, String env) {
		this.configData = configData;
		this.env = env;
	}
	@Override
	public boolean ping() throws InvalidOperationException, TException {
		return true;
	}
	@Override
	public String getSingleValue(String type, String key) throws InvalidOperationException, TException {
		return configData.getDirectMaster().get(type).get(key);
	}
	@Override
	public Map<String, String> getAllValues(String type) throws InvalidOperationException, TException {
		return configData.getDirectMaster().get(type);
	}
	@Override
	public Map<String, String> getGroupValues(String type, String group) throws InvalidOperationException, TException {
		return configData.getGroupMaster().get(type).get(group);
	}
	@Override
	public List<String> getTypes() throws InvalidOperationException, TException {
		List<String> response = new ArrayList<>();
		configData.getDirectMaster().keySet().forEach(r -> response.add(r));
		return response;
	}
	@Override
	public List<String> getGroups(String type) throws InvalidOperationException, TException {
		List<String> response = new ArrayList<>();
		configData.getGroupMaster().get(type).keySet().forEach(r -> response.add(r));
		return response;
	}
	@Override
	public void setSingleValue(String type, String key, String value) throws InvalidOperationException, TException {
		configData.getDirectMaster().get(type).remove(key);
		configData.getDirectMaster().get(type).put(key, value);
		if (key.contains(".")) {
			setGroupSpecificValue(type, key, value);
		}
		persistConfigs(type);
	}
	private void setGroupSpecificValue(String type, String keywithgroup, String value) {
		String[] a = keywithgroup.split("\\.");
		String elemKey = keywithgroup.replace((a[0] + "." + a[1] + "."), "");
		configData.getGroupMaster().get(type).get(a[0] + "." + a[1]).remove(elemKey);
		configData.getGroupMaster().get(type).get(a[0] + "." + a[1]).put(elemKey, value);
	}
	@Override
	public void setGroupValue(String type, String group, Map<String, String> values)
			throws InvalidOperationException, TException {
		configData.getGroupMaster().get(type).remove(group);
		configData.getGroupMaster().get(type).put(group, values);
		setDirectSpecificValue(type, group, values);
		persistConfigs(type);
	}
	private void setDirectSpecificValue(String type, String group, Map<String, String> values) {
		values.entrySet().forEach(e -> {
			String key = group + "." + e.getKey();
			configData.getDirectMaster().get(type).remove(key);
			configData.getDirectMaster().get(type).put(key, e.getValue());
		});
	}
	@Override
	public void refreshConfigs() throws InvalidOperationException, TException {
		// TODO Auto-generated method stub
	}
	@Override
	public void persistConfigs(String type) throws InvalidOperationException, TException {
		String folder = "/app/pna/ccm/" + this.env + "/";
		List<String> configs = new ArrayList<>();
		String filename = folder + type + ".json";
		File newfile = new File(filename);
		configData.getDirectMaster().get(type).entrySet().forEach(x -> {
			File basefile = new File(filename);
			basefile.delete();
			String value = "\"" + x.getKey() + "\":\"" + x.getValue() + "\"";
			configs.add(value);
		});
		String output = "{\n" + String.join(",\n", configs) + "\n}";
		try {
			FileWriter fw = new FileWriter(newfile, false);
			BufferedWriter writer = new BufferedWriter(fw);
			writer.write(output);
			writer.flush(); writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}