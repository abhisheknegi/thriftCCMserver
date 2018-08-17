package com.pack;

import java.util.Map;

public class dataClass {
	
	public Map<String, Map<String, String>> directMaster;
	public Map<String, Map<String, Map<String, String>>> groupMaster;
	
	public Map<String, Map<String, String>> getDirectMaster() {
		return this.directMaster;
	}
	public void setDirectMaster(Map<String, Map<String, String>> directMaster) {
		this.directMaster = directMaster;
	}
	public Map<String, Map<String, Map<String, String>>> getGroupMaster() {
		return this.groupMaster;
	}
	public void setGroupMaster(Map<String, Map<String, Map<String, String>>> groupMaster) {
		this.groupMaster = groupMaster;
	}

}
