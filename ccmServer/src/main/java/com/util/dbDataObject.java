package com.util;

public class dbDataObject {
	
	private String type = null;
	private String group = null;
	private String key = null;
	private String value = null;
	
	public dbDataObject(String type, String group, String key, String value) {
		this.type = type;
		this.group = group;
		this.key = key;
		this.value = value;
	}
	public dbDataObject() {
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getGroup() {
		return group;
	}
	public void setGroup(String group) {
		this.group = group;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}

}
