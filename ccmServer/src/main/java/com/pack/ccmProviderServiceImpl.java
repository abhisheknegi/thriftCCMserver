package com.pack;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import com.util.dbDataObject;
import com.util.localDatabaseConfigs;

public class ccmProviderServiceImpl implements ccmProviderService.Iface {
	
	localDatabaseConfigs conf;
	
	public ccmProviderServiceImpl(localDatabaseConfigs conf) {
		this.conf = conf;
	}
	@Override
	public boolean ping() throws InvalidOperationException, TException {
		try {
			return !conf.getDbConnection().isClosed();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}
	@Override
	public String getSingleValue(String type, String key) throws InvalidOperationException, TException {
		return conf.getSingleValue(type, key);
	}
	@Override
	public Map<String, String> getAllValues(String type) throws InvalidOperationException, TException {
		return conf.getAllValues(type);
	}
	@Override
	public Map<String, String> getGroupValues(String type, String group) throws InvalidOperationException, TException {
		return conf.getGroupValues(type, group);
	}
	@Override
	public List<String> getTypes() throws InvalidOperationException, TException {
		return conf.getTypes();
	}
	@Override
	public List<String> getGroups(String type) throws InvalidOperationException, TException {
		return conf.getGroups(type);
	}
	@Override
	public void setSingleValue(String type, String key, String value) throws InvalidOperationException, TException {
		dbDataObject insertObj = new dbDataObject();
		insertObj.setType(type);
		insertObj.setKey(key);
		insertObj.setValue(value);
		conf.setRecord(insertObj);
	}
	@Override
	public void setGroupValue(String type, String group, Map<String, String> keyvaluemap)
			throws InvalidOperationException, TException {
		dbDataObject insertObj = new dbDataObject();
		keyvaluemap.entrySet().forEach(rec -> {
			insertObj.setType(type);
			insertObj.setGroup(group);
			insertObj.setKey(rec.getKey());
			insertObj.setValue(rec.getValue());
			conf.setRecord(insertObj);
		});
	}
}