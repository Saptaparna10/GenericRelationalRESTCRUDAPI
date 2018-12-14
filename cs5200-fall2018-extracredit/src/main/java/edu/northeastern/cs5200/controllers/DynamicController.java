package edu.northeastern.cs5200.controllers;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.json.*;

import java.sql.PreparedStatement;



@Controller
public class DynamicController {

	@PostMapping("/api/{table}")
	private @ResponseBody String createTable(@RequestBody String data, @PathVariable String table){

		JSONObject jsonObj = new JSONObject(data);
		Connection con = null;
		PreparedStatement statement0 = null;
		PreparedStatement statement1 = null;
		PreparedStatement statement2 = null;


		try{
			con = edu.northeastern.cs5200.Connection.getConnection();

			String sql0 = "SELECT *  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'cs5200_fall2018_das_extra_credit' AND  TABLE_NAME = '"+table+"'";
			statement0 = con.prepareStatement(sql0);
			ResultSet rs0 = statement0.executeQuery();

			//table does not exist
			if(!rs0.first()) {
				String sql1 = "CREATE TABLE  "+ table + "(ID int NOT NULL PRIMARY KEY AUTO_INCREMENT,";

				String sql2 = "insert into "+ table + "(";
				String values = " values (";

				for (Object key : jsonObj.keySet()) {
					//based on you key types
					String keyStr = (String)key;
					Object keyvalue = jsonObj.get(keyStr);

					//Print key and value
					System.out.println("key: "+ keyStr + " value: " + keyvalue);

					sql1+= key + " varchar(255),";
					sql2+=keyStr+",";
					values+= "'"+keyvalue + "',";

				}
				sql1 = sql1.substring(0, sql1.length()-1);
				sql1+= ")";
				sql2 = sql2.substring(0, sql2.length()-1);
				sql2+= ")";
				values = values.substring(0, values.length()-1);
				sql2+=values;
				sql2+= ")";
				System.out.println(sql1);
				System.out.println(sql2);

				// inserting in person table
				statement1 = con.prepareStatement(sql1);
				statement2 = con.prepareStatement(sql2);

				statement1.executeUpdate();
				statement2.executeUpdate();
			}
			else {

				String sql3 = "select * from "+table;
				String sql2 = "insert into "+ table + "(";
				String values = " values (";
				statement1 = con.prepareStatement(sql3);
				ResultSet rs1 = statement1.executeQuery();
				ResultSetMetaData rsmd = rs1.getMetaData();


				for (Object key : jsonObj.keySet()) {
					//based on you key types
					String keyStr = (String)key;
					Object keyvalue = jsonObj.get(keyStr);

					//Print key and value
					System.out.println("key: "+ keyStr + " value: " + keyvalue);

					if(exists(rsmd, keyStr)) {
						sql2+=keyStr+",";
						values+= "'"+keyvalue + "',";
					}
					else {
						String sqlAlter = "alter table "+ table+ " add "+ keyStr + " varchar(255)";
						sql2+=keyStr+",";
						values+= "'"+keyvalue + "',";
						PreparedStatement statementAlter = con.prepareStatement(sqlAlter);
						statementAlter.executeUpdate();
					}

				}
				sql2 = sql2.substring(0, sql2.length()-1);
				sql2+= ")";
				values = values.substring(0, values.length()-1);
				sql2+=values;
				sql2+= ")";
				System.out.println(sql2);

				statement1 = con.prepareStatement(sql2);					
				statement1.executeUpdate();

			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			edu.northeastern.cs5200.Connection.closeConnection();

		}
		return jsonObj.toString();

	}

	@GetMapping("/api/{table}")
	@ResponseBody
	private String getTable(@PathVariable String table){

		JSONArray arr= new JSONArray();
		Connection con=null;
		PreparedStatement statement1=null;
		PreparedStatement statement2=null;
		try {
			String sql1 = "SELECT *  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'cs5200_fall2018_das_extra_credit' AND  TABLE_NAME = '"+table+"'";
			String sql2 = "SELECT *  FROM "+table;
			con = edu.northeastern.cs5200.Connection.getConnection();
			statement1 = con.prepareStatement(sql1);
			ResultSet rs1 = statement1.executeQuery();

			if(!rs1.first())
				return null;

			statement2 = con.prepareStatement(sql2);
			ResultSet rs2 = statement2.executeQuery();
			ResultSetMetaData rsmd = rs2.getMetaData();
			int cols = rsmd.getColumnCount();				

			JSONObject obj = null;
			while(rs2.next()) {
				obj=new JSONObject();
				for(int i=1; i<=cols; i++) {
					System.out.println(rsmd.getColumnLabel(i) + "" + rs2.getString(i));
					obj.append(rsmd.getColumnLabel(i), rs2.getString(i));
				}
				arr.put(obj);
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			edu.northeastern.cs5200.Connection.closeConnection();

		}
		System.out.println(arr.toString());
		return arr.toString();

	}

	@GetMapping("/api/{table}?{predicates}")
	@ResponseBody
	private String getTableWithFilter(@PathVariable String table, @PathVariable String predicates){


		JSONArray arr= new JSONArray();
		Connection con=null;
		PreparedStatement statement1=null;
		PreparedStatement statement2=null;
		try {
			String sql1 = "SELECT *  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'cs5200_fall2018_das_extra_credit' AND  TABLE_NAME = '"+table+"'";
			String sql2 = "SELECT *  FROM "+table + " where " + predicates;
			con = edu.northeastern.cs5200.Connection.getConnection();
			statement1 = con.prepareStatement(sql1);
			ResultSet rs1 = statement1.executeQuery();

			if(!rs1.first())
				return null;

			statement2 = con.prepareStatement(sql2);
			ResultSet rs2 = statement2.executeQuery();
			ResultSetMetaData rsmd = rs2.getMetaData();
			int cols = rsmd.getColumnCount();				

			JSONObject obj = null;
			while(rs2.next()) {
				obj=new JSONObject();
				for(int i=1; i<=cols; i++) {
					System.out.println(rsmd.getColumnLabel(i) + "" + rs2.getString(i));
					obj.append(rsmd.getColumnLabel(i), rs2.getString(i));
				}
				arr.put(obj);
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			edu.northeastern.cs5200.Connection.closeConnection();

		}
		System.out.println(arr.toString());
		return arr.toString();

	}

	@GetMapping("/api/{table}/{id}")
	@ResponseBody
	private String getRowInTable(@PathVariable String table, @PathVariable String id){


		//System.out.println("id "+ String.valueOf(Integer.parseInt(id)) + "id-1" + String.valueOf(Integer.parseInt(id)-1));
		JSONArray arr= new JSONArray();
		Connection con=null;
		PreparedStatement statement1=null;
		PreparedStatement statement2=null;
		try {
			String sql1 = "SELECT *  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'cs5200_fall2018_das_extra_credit' AND  TABLE_NAME = '"+table+"'";
			String sql2 = "SELECT *  FROM "+table + " where id = "+id;

			con = edu.northeastern.cs5200.Connection.getConnection();
			statement1 = con.prepareStatement(sql1);
			ResultSet rs1 = statement1.executeQuery();

			if(!rs1.first())
				return null;

			statement2 = con.prepareStatement(sql2);
			ResultSet rs2 = statement2.executeQuery();
			ResultSetMetaData rsmd = rs2.getMetaData();
			int cols = rsmd.getColumnCount();				

			JSONObject obj = null;
			while(rs2.next()) {
				obj=new JSONObject();
				for(int i=1; i<=cols; i++) {
					System.out.println(rsmd.getColumnLabel(i) + "" + rs2.getString(i));
					obj.append(rsmd.getColumnLabel(i), rs2.getString(i));
				}
				arr.put(obj);
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			edu.northeastern.cs5200.Connection.closeConnection();

		}
		System.out.println(arr.toString());
		return arr.toString();


	}

	@PutMapping("/api/{table}/{id}")
	@ResponseBody
	private String updateRowInTable(@RequestBody String data, @PathVariable String table, @PathVariable String id){
		
		JSONObject jsonObj = new JSONObject(data);
		Connection con=null;
		PreparedStatement statement1=null;
		try {
			String sql1 = "SELECT *  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'cs5200_fall2018_das_extra_credit' AND  TABLE_NAME = '"+table+"'";
			String sql2 = "update "+table+ " set ";

			con = edu.northeastern.cs5200.Connection.getConnection();
			statement1 = con.prepareStatement(sql1);
			ResultSet rs1 = statement1.executeQuery();

			if(!rs1.first())
				return null;

//			statement2 = con.prepareStatement(sql2);
//			statement2.executeUpdate();

			//select
			String sql3 = "select * from "+table+" where id="+id;
			statement1 = con.prepareStatement(sql3);
			ResultSet rs2 = statement1.executeQuery();
			ResultSetMetaData rsmd = rs2.getMetaData();
			
			if(!rs2.first())
				return null;


			for (Object key : jsonObj.keySet()) {
				//based on you key types
				String keyStr = (String)key;
				Object keyvalue = jsonObj.get(keyStr);

				//Print key and value
				System.out.println("key: "+ keyStr + " value: " + keyvalue);

				if(exists(rsmd, keyStr)) {
					sql2+=keyStr+"= '"+keyvalue+"',";
				}
				else {
					String sqlAlter = "alter table "+ table+ " add "+ keyStr + " varchar(255)";
					sql2+=keyStr+"= '"+keyvalue+"',";
					PreparedStatement statementAlter = con.prepareStatement(sqlAlter);
					statementAlter.executeUpdate();
				}

			}
			sql2 = sql2.substring(0, sql2.length()-1);
			sql2+= " where id="+id;
					
			System.out.println(sql2);

			statement1 = con.prepareStatement(sql2);					
			statement1.executeUpdate();

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			edu.northeastern.cs5200.Connection.closeConnection();

		}
		return jsonObj.toString();


	}
	
	@DeleteMapping("/api/{table}/{id}")
	@ResponseBody
	private ResponseEntity<?> deleteRowInTable(@PathVariable String table, @PathVariable String id){

		Connection con=null;
		PreparedStatement statement1=null;
		PreparedStatement statement2=null;
		try {
			String sql1 = "SELECT *  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'cs5200_fall2018_das_extra_credit' AND  TABLE_NAME = '"+table+"'";
			String sql2 = "delete FROM "+table + " where id = " + id;

			con = edu.northeastern.cs5200.Connection.getConnection();
			statement1 = con.prepareStatement(sql1);
			ResultSet rs1 = statement1.executeQuery();

			if(!rs1.first())
				return null;

			statement2 = con.prepareStatement(sql2);
			statement2.executeUpdate();
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
			ResponseEntity<?> res= new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
			return res;
		} finally {
			edu.northeastern.cs5200.Connection.closeConnection();

		}
		ResponseEntity<?> res= new ResponseEntity<>(HttpStatus.OK);
		return res;
	}
	
	@DeleteMapping("/api/{table}")
	@ResponseBody
	private ResponseEntity<?> deleteAllRowsInTable(@PathVariable String table){

		Connection con=null;
		PreparedStatement statement1=null;
		PreparedStatement statement2=null;
		try {
			String sql1 = "SELECT *  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'cs5200_fall2018_das_extra_credit' AND  TABLE_NAME = '"+table+"'";
			String sql2 = "delete FROM "+table ;

			con = edu.northeastern.cs5200.Connection.getConnection();
			statement1 = con.prepareStatement(sql1);
			ResultSet rs1 = statement1.executeQuery();

			if(!rs1.first())
				return null;

			statement2 = con.prepareStatement(sql2);
			statement2.executeUpdate();
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
			ResponseEntity<?> res= new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
			return res;
		} finally {
			edu.northeastern.cs5200.Connection.closeConnection();

		}
		ResponseEntity<?> res= new ResponseEntity<>(HttpStatus.OK);
		return res;
	}
	
	
	@PostMapping("/api/{table1}/{id1}/{table2}/{id2}")
	private @ResponseBody String createMultipleTables(@RequestBody String data, @PathVariable String table){

		JSONObject jsonObj = new JSONObject(data);
		Connection con = null;
		PreparedStatement statement0 = null;
		PreparedStatement statement1 = null;
		PreparedStatement statement2 = null;


		try{
			con = edu.northeastern.cs5200.Connection.getConnection();

			String sql0 = "SELECT *  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'cs5200_fall2018_das_extra_credit' AND  TABLE_NAME = '"+table+"'";
			statement0 = con.prepareStatement(sql0);
			ResultSet rs0 = statement0.executeQuery();

			//table does not exist
			if(!rs0.first()) {
				String sql1 = "CREATE TABLE  "+ table + "(ID int NOT NULL PRIMARY KEY AUTO_INCREMENT,";

				String sql2 = "insert into "+ table + "(";
				String values = " values (";

				for (Object key : jsonObj.keySet()) {
					//based on you key types
					String keyStr = (String)key;
					Object keyvalue = jsonObj.get(keyStr);

					//Print key and value
					System.out.println("key: "+ keyStr + " value: " + keyvalue);

					sql1+= key + " varchar(255),";
					sql2+=keyStr+",";
					values+= "'"+keyvalue + "',";

				}
				sql1 = sql1.substring(0, sql1.length()-1);
				sql1+= ")";
				sql2 = sql2.substring(0, sql2.length()-1);
				sql2+= ")";
				values = values.substring(0, values.length()-1);
				sql2+=values;
				sql2+= ")";
				System.out.println(sql1);
				System.out.println(sql2);

				// inserting in person table
				statement1 = con.prepareStatement(sql1);
				statement2 = con.prepareStatement(sql2);

				statement1.executeUpdate();
				statement2.executeUpdate();
			}
			else {

				String sql3 = "select * from "+table;
				String sql2 = "insert into "+ table + "(";
				String values = " values (";
				statement1 = con.prepareStatement(sql3);
				ResultSet rs1 = statement1.executeQuery();
				ResultSetMetaData rsmd = rs1.getMetaData();


				for (Object key : jsonObj.keySet()) {
					//based on you key types
					String keyStr = (String)key;
					Object keyvalue = jsonObj.get(keyStr);

					//Print key and value
					System.out.println("key: "+ keyStr + " value: " + keyvalue);

					if(exists(rsmd, keyStr)) {
						sql2+=keyStr+",";
						values+= "'"+keyvalue + "',";
					}
					else {
						String sqlAlter = "alter table "+ table+ " add "+ keyStr + " varchar(255)";
						sql2+=keyStr+",";
						values+= "'"+keyvalue + "',";
						PreparedStatement statementAlter = con.prepareStatement(sqlAlter);
						statementAlter.executeUpdate();
					}

				}
				sql2 = sql2.substring(0, sql2.length()-1);
				sql2+= ")";
				values = values.substring(0, values.length()-1);
				sql2+=values;
				sql2+= ")";
				System.out.println(sql2);

				statement1 = con.prepareStatement(sql2);					
				statement1.executeUpdate();

			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			edu.northeastern.cs5200.Connection.closeConnection();

		}
		return jsonObj.toString();

	}

	private boolean exists(ResultSetMetaData rsmd, String col) throws SQLException {
		for(int i=1; i<=rsmd.getColumnCount();i++) {
			if(rsmd.getColumnName(i).equals(col))
				return true;
		}
		
		return false;
	}

}
