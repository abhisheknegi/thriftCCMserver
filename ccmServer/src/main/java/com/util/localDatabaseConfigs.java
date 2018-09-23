package com.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class localDatabaseConfigs {
	
	public static void createTable(Connection conn, String table) throws SQLException {
		Statement statement = conn.createStatement();
		statement.setQueryTimeout(5);
		statement.executeUpdate("drop table if exists " + table);
		statement.execute("create table " + table + " (type string, group1 string, key_1 string PRIMARY KEY, value_1 string)");
		statement.close();
	}
	
	public static void setRecord(Connection conn, String table, dbDataObject data) throws SQLException {
		PreparedStatement ps = conn.prepareStatement("insert into " + table + " values(?,?,?,?)");
		ps.setString(1, data.getType());
		ps.setString(2, data.getGroup1());
		ps.setString(3, data.getKey());
		ps.setString(4, data.getValue());
		ps.executeUpdate();
		ps.closeOnCompletion();
	}
	
	public static String getSingleValue(Connection conn, String table, String type, String key) throws SQLException {
		String resp=null;
		String sql = String.format("select * from %s where type = %s and key = '%s'", table, type, key);
		ResultSet rs = conn.prepareStatement(sql).executeQuery();
		while(rs.next()) {
			resp = rs.getString("value");
		}
		return resp;
	}
	
	public static Map<String, String> getGroupValue(Connection conn, String table, String type, String group) throws SQLException {
		Map<String, String> resp = new HashMap<>();
		String sql = String.format("select * from %s where type = %s and group1 = '%s'", table, type, group);
		ResultSet rs = conn.prepareStatement(sql).executeQuery();
		while(rs.next()) {
			resp.put(rs.getString("key"), rs.getString("value"));
		}
		return resp;
	}
	
	public static Connection getDbConnection() throws SQLException {

		Connection connection = null;

		try {
			Class.forName("org.sqlite.JDBC");
			connection = DriverManager.getConnection("jdbc:sqlite:~/sqlite_db/config.db");
			connection.setAutoCommit(true);
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (connection.isClosed()) {
			return null;
		} else {
			return connection;
		}

	}

}
