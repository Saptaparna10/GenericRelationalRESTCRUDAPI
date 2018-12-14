package edu.northeastern.cs5200;

import java.sql.*;

public class Connection {
	private static final String DRIVER = "com.mysql.jdbc.Driver";
	private static final String URL = "jdbc:mysql://cs5200-fall2018-das.ckclq5cchaam.us-east-2.rds.amazonaws.com/cs5200_fall2018_das_extra_credit";
	private static final String USER = "das";
	private static final String PASSWORD = "#sappyNSEC10";
	private static java.sql.Connection dbConnection = null;

	public static java.sql.Connection getConnection() throws ClassNotFoundException, SQLException {
		Class.forName(DRIVER);

		if (dbConnection == null) {
			dbConnection = DriverManager.getConnection(URL, USER, PASSWORD);
			return dbConnection;
		} else { return dbConnection; } 
	}

	public static void closeConnection() {
		try {
			if(dbConnection != null) {
				dbConnection.close();
				dbConnection = null; 
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}


}
