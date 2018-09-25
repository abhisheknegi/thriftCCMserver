package com.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class localDatabaseConfigs {
	
	private String env = null;
	private Connection connect = null;
	private PreparedStatement ps0 = null;
	private PreparedStatement ps1 = null;
	private PreparedStatement ps2 = null;
	private PreparedStatement ps3 = null;
	private PreparedStatement ps4 = null;
	private PreparedStatement ps5 = null;
	private PreparedStatement ps6 = null;

	public localDatabaseConfigs(String env) {
		this.env = env;
	}

	protected void createTable() throws SQLException {
		Statement statement = connect.createStatement();
		statement.setQueryTimeout(5);
		statement.executeUpdate("drop table if exists configs");
		statement.execute(
				"create table configs (type string, group1 string, key string, value string, PRIMARY KEY (type, key)");
		statement.close();
	}

	public void setRecord(dbDataObject data) {
		try {
			if(ps0 == null) {
				String sql = "insert into configs values(?,?,?,?)";
				ps0 = connect.prepareStatement(sql);
			}
			ps0.setString(1, data.getType());
			if(data.getGroup() == null) {
				String[] st = data.getKey().split("\\.");
				data.setGroup(st[0]+"."+st[1]);
			}
			ps0.setString(2, data.getGroup());
			ps0.setString(3, data.getKey());
			ps0.setString(4, data.getValue());
			ps0.executeUpdate();
			connect.commit();
		} catch (SQLException e) {
			clearDbMessage();
			System.out.println("Error Code => " + e.getErrorCode() + " :: " + e.getMessage() + ", => Triggering Update.");
			updateRecord(data);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public void updateRecord(dbDataObject data) {
		try {
			if(ps6 == null) {
				String sql = "update configs set value = ? where type = ? and key = ?";
				ps6 = connect.prepareStatement(sql);
			}
			ps6.setString(1, data.getValue());
			ps6.setString(2, data.getType());
			ps6.setString(3, data.getKey());
			int i = ps6.executeUpdate();
			System.out.println("Update Successful, Code: " + i);
			connect.commit();
		} catch (Exception e) {
			clearDbMessage();
			System.out.println("Error => " + e.getMessage());
			e.printStackTrace();
		}
	}

	public String getSingleValue(String type, String key) {
		try {
			String resp = null;
			if(ps1 == null) {
				ps1 = connect.prepareStatement("select * from configs where type = ? and key = ?");
			}
			ps1.setString(1, type);
			ps1.setString(2, key);
			ResultSet rs = ps1.executeQuery();
			while (rs.next()) {
				resp = rs.getString("value");
			}
			rs.close();
			return resp;
		} catch (Exception e) {
			clearDbMessage();
			System.out.println("Error => " + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	public Map<String, String> getGroupValues(String type, String group) {
		try {
			Map<String, String> resp = new HashMap<>();
			if(ps2 == null) {
				ps2 = connect.prepareStatement("select * from configs where type = ? and group1 = ?");
			}
			ps2.setString(1, type);
			ps2.setString(2, group);
			ResultSet rs = ps2.executeQuery();
			while (rs.next()) {
				resp.put(rs.getString("key"), rs.getString("value"));
			}
			rs.close();
			return resp;
		} catch (Exception e) {
			clearDbMessage();
			System.out.println("Error => " + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	public Map<String, String> getAllValues(String type) {
		try {
			Map<String, String> resp = new HashMap<>();
			if(ps3 == null) {
				ps3 = connect.prepareStatement("select * from configs where type =?");
			}
			ps3.setString(1, type);
			ResultSet rs = ps3.executeQuery();
			while (rs.next()) {
				resp.put(rs.getString("key"), rs.getString("value"));
			}
			rs.close();
			return resp;
		} catch (Exception e) {
			clearDbMessage();
			System.out.println("Error => " + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	public List<String> getTypes(){
		try {
			List<String> resp = new ArrayList<>();
			if(ps4 == null) {
				String sql = "select type from configs";
				ps4 = connect.prepareStatement(sql);
			}
			ResultSet rs = ps4.executeQuery();
			while (rs.next()) {
				resp.add(rs.getString(0));
			}
			rs.close();
			return resp;
		} catch (Exception e) {
			clearDbMessage();
			System.out.println("Error => " + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	public List<String> getGroups(String type){
		try {
			List<String> resp = new ArrayList<>();
			if(ps5 == null) {
				ps5 = connect.prepareStatement("select group1 from configs where type = ?");
			}
			ps5.setString(1, type);
			ResultSet rs = ps5.executeQuery();
			while (rs.next()) {
				resp.add(rs.getString(0));
			}
			rs.close();
			return resp;
		} catch (Exception e) {
			clearDbMessage();
			System.out.println("Error => " + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	public Connection getDbConnection() throws SQLException {

		if (connect == null || connect.isClosed()) {
			String conn = "jdbc:sqlite:/Users/vn0bp7s/sqlite_db/" + this.env + ".db";
			try {
				Class.forName("org.sqlite.JDBC");
				this.connect = DriverManager.getConnection(conn);
				this.connect.setAutoCommit(true);
			} catch (Exception e) {
				clearDbMessage();
				System.out.println("Error => " + e.getMessage());
				e.printStackTrace();
			}
		}
		this.connect.setAutoCommit(false);
		return this.connect;
	}
	
	private void clearDbMessage() {
		try {
			connect.clearWarnings();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
