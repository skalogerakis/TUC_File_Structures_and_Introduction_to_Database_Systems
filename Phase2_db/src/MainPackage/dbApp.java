package MainPackage;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class dbApp {
	
	Connection conn;
	Connection connBackup;
	//public Scanner scn;
	static dbApp db;
	
	public dbApp(){
		conn = null;
		connBackup = null;
		
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//BigDecimal a = new BigDecimal("8.1");
		db = new dbApp();
		//db.Connect_db();
		Scanner scn = new Scanner(System.in);
		db.printMenu();
		int scanner = scn.nextInt();
		while(scanner!=0) {
			if(scanner == 1) {
				System.out.println("Connecting on database");
				db.Connect_db();
				System.out.println("");
			}else if(scanner == 2) {
				System.out.println("Insert am to show analytic grades: ");
				
				String am = scn.next();
				db.AmAnalyticGrades(am);
				System.out.println("");
			}else if(scanner == 3) {
				System.out.println("Insert am:");
				String am1 = scn.next();
				System.out.println("Insert course:");
				String courseCode = scn.next();
				System.out.println("First check the latest grade");
				db.showLatestGrade(am1, courseCode);
				System.out.println("Now insert the grade you wish:");
				BigDecimal grade = scn.nextBigDecimal();
				db.changeLatestGrade(am1, courseCode, grade);
				db.showLatestGrade(am1, courseCode);
				System.out.println("");
			}else if(scanner == 4) {
				System.out.println("Give the exact name of backup database");
				String dbName = scn.next();
				System.out.println("Give table name you wish to create safety copy:");
				String tDupl = scn.next();
				db.backupQues(dbName, tDupl);
				System.out.println("");
			}else if(scanner == 5) {
				db.commit();
				System.out.println("");
			}else {
				db.abort();
				System.out.println("");
			}
			db.printMenu();
			scanner = scn.nextInt();
		}
		
		
		
		
		
		//db.AmAnalyticGrades("2013000002");
		//db.showLatestGrade("2012000002","еме 301");
		//db.changeLatestGrade("2012000002","еме 301", a);
		//db.showLatestGrade("2012000002","еме 301");
		//db.commit();
		//db.backupQues("Project_backup","Supports");
		
		
		
	}
	
	//Question 1
	public void Connect_db() {
		try {
			//WATCH THE FINAL EDITION!!!!
			Class.forName("org.postgresql.Driver");
			conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/Project_ver5", "postgres", "1234");
			System.out.println("Successful connection");
			//Disable auto-commit
			conn.setAutoCommit(false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void Connect_db(String connName) {
		try {
			//WATCH THE FINAL EDITION!!!!
			Class.forName("org.postgresql.Driver");
			connBackup = DriverManager.getConnection("jdbc:postgresql://localhost:5432/"+connName, "postgres", "1234");
			System.out.println("Successful connection");
			//Disable auto-commit
			connBackup.setAutoCommit(false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	//Question 2
	public void AmAnalyticGrades(String am) {
		
		try {
			PreparedStatement pst = conn.prepareStatement("SELECT s.am, reg.course_code, reg.exam_grade, reg.lab_grade, reg.final_grade \r\n" + 
					"FROM \"Register\" AS reg, \"Student\" AS s\r\n" + 
					"WHERE reg.register_status = 'pass'\r\n" + 
					"AND reg.amka = s.amka\r\n" + 
					"AND s.am = ? ");
			pst.setString(1, am);
			
			ResultSet result = pst.executeQuery();
			while(result.next()) {
				System.out.println("Am: "+result.getString(1)+"/ Course Code: "+result.getString(2)+"/ Exam Grade: "+result.getFloat(3)+"/ Lab Grade: "+result.getFloat(4)+"/ Final Grade: "+result.getFloat(5));
				
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//Question 3(Changes only final grade)
	//PART 1
	//AT FIRST show am, course, final_grade
	public void showLatestGrade(String am, String c_code) {
		
		try {
			PreparedStatement pst = conn.prepareStatement("SELECT s.am, reg.course_code, reg.final_grade \r\n" + 
					"FROM \"Register\" AS reg, \"Student\" AS s, \"Semester\" AS sem\r\n" + 
					"WHERE reg.register_status IN ('pass','fail')\r\n" + 
					"AND sem.semester_id = reg.serial_number\r\n" + 
					"AND reg.amka = s.amka\r\n" + 
					"AND s.am = ? \r\n" + 
					"AND reg.course_code = ? \r\n" + 
					"ORDER BY sem.end_date DESC\r\n" + 
					"LIMIT 1\r\n");
			
			pst.setString(1, am);
			pst.setString(2, c_code);
			
			ResultSet res = pst.executeQuery();
			while(res.next()) {
				System.out.println("Am: "+res.getString(1)+"/ Course Code: "+res.getString(2)+"/ Final Grade: "+res.getBigDecimal(3));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	//Question 3(Changes only final grade)
	//PART 2
	//NOW CHANGE LATEST GRADE
	
	public void changeLatestGrade(String am, String course_code, BigDecimal newGrade) {
		try {
			PreparedStatement pst = conn.prepareStatement("UPDATE \"Register\" r\r\n" + 
					"SET final_grade = ?" + 
					"FROM \"Student\" s\r\n" + 
					"WHERE r.amka = s.amka\r\n" + 
					"AND serial_number IN(SELECT reg.serial_number\r\n" + 
					"					FROM \"Register\" AS reg, \"Student\" AS s, \"Semester\" AS sem\r\n" + 
					"					WHERE reg.register_status IN ('pass','fail')\r\n" + 
					"					AND sem.semester_id = reg.serial_number\r\n" + 
					"					AND reg.amka = s.amka\r\n" + 
					"					AND s.am = ?\r\n" + 
					"					AND reg.course_code = ?\r\n" + 
					"					ORDER BY sem.end_date DESC\r\n" + 
					"					LIMIT 1)\r\n" + 
					"AND s.am = ?\r\n" + 
					"AND r.course_code = ?");
				pst.setBigDecimal(1, newGrade);
				pst.setString(2, am);
				pst.setString(3, course_code);
				pst.setString(4, am);
				pst.setString(5, course_code);
				pst.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//Question 4
	public void backupQues(String connDbName, String tableName) {
		db.Connect_db(connDbName);
		DatabaseMetaData dbMeta;
		int tableFlag = 1;
		try {
			dbMeta = conn.getMetaData();
			//Found online
			// check if  table is there
			
			ResultSet tables = dbMeta.getTables(null, null, tableName, null);
			if (tables.next()) {
			  // Table exists
				System.out.println("Table name exists at initial database");
				
			}
			else {
			  // Table does not exist
				throw new java.lang.Error("Table name doesn't exist at initial database");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			dbMeta = connBackup.getMetaData();
			
			ResultSet tables = dbMeta.getTables(null, null, tableName, null);
			if (tables.next()) {
			  // Table exists
				System.out.println("Table name exists at backup database");
			}
			else {
			  // Table does not exist
				System.out.println("Table name doesn't exist at initial database");
				tableFlag = 0;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(tableFlag ==1) {
			//case when table already exists in backup database
			try {
				PreparedStatement pst = connBackup.prepareStatement("DELETE FROM \""+tableName+"\"");
				pst.execute();
				connBackup.commit();
				System.out.println("Delete all elements from existing table in backup database");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else {
			//case when table doesn't exists and we use the function given 
			PreparedStatement pst;
			try {
				pst = conn.prepareStatement("SELECT * FROM generateTableDDL(?)");
				
				pst.setString(1, tableName);
				ResultSet resSet = pst.executeQuery();
				ResultSetMetaData rsm = resSet.getMetaData();

				int columnCount = rsm.getColumnCount();

				while (resSet.next()) {
					for (int i = 1; i <= columnCount; i++)
						pst = connBackup.prepareStatement(resSet.getString(i));
						pst.execute();
					
				}
				connBackup.commit();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

		}
		//the following code will be used in both cases to copy all data from initial db
		//in the first case we delete all data from existing table and insert data from initial db whereas in the second case we create the table
		//casting is in both cases mandatory
		try {
			PreparedStatement pst = conn.prepareStatement("SELECT * FROM \"" + tableName + "\"");

			ResultSet result = pst.executeQuery();
			ResultSetMetaData rsm = result.getMetaData();
			int columnCount = rsm.getColumnCount();
			String[] labelName = new String[columnCount];
			String[] TypelabelName = new String[columnCount];
			
			for (int j = 1; j <= columnCount; j++) {
				labelName[j-1] = rsm.getColumnLabel(j);
				TypelabelName[j-1] = "?::"+rsm.getColumnTypeName(j); //for casting purposes only
			}
			
			String concLabelName = String.join(", ", labelName);
			String concTypelabelName = String.join(", ", TypelabelName);

			//insert into table with form INSERT INTO table(value1,valu2,..) VALUES (data1,data2....)
			//cast into different type each type depending on initial database type (BONUS)
			PreparedStatement ins = connBackup.prepareStatement(
					"INSERT INTO \"" + tableName + "\" (" + concLabelName + ") VALUES (" + concTypelabelName + ");");

			while (result.next()) {
				for (int j = 1; j <= columnCount; j++) {
					ins.setString(j, result.getString(j));
				}
				ins.execute();
			}
			connBackup.commit();
			System.out.println("Finished.");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public void commit() {
		try {
			conn.commit();
			System.out.println("Commit Successful");
		}catch(SQLException e) {
			System.out.println("Commit failed");
			e.printStackTrace();
		}
		
	}
	
	public void abort() {
		
		try {
			conn.rollback();
			System.out.println("Rollback Successful");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Rollback Failed");
			e.printStackTrace();
		}
		
	}
	
	public void waitForEnter() {
		Scanner scan = new Scanner(System.in);
		System.out.println("Press Enter......");
		scan.nextLine();
		
	}
	
	public void printMenu() {
		System.out.println("Welcome!!");
		System.out.println("Choose one of the following options to execute\n\n");
		System.out.println("0. Exit");
		System.out.println("1. Connect");
		System.out.println("2. Analytic grades based on AM");
		System.out.println("3. Change student's grade");
		System.out.println("4. Create backup on different db");
		System.out.println("5. Commit");
		System.out.println("6. Abort");
		
	}

}
