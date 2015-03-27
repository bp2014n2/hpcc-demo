import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import de.hpi.hpcc.parsing.ECLLayouts;


public class JDBCTest {

	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "de.hpi.hpcc.main.HPCCDriver";  
	static final String DB_URL = "jdbc:hpcc://54.93.130.121:8010";  
	static final String PG_DRIVER = "org.postgresql.Driver";  
	static final String PG_URL = "jdbc:postgresql://54.93.194.65:5432/i2b2";  
	static final String PGUSER = "jdbc:postgresql://54.93.194.65:5432/i2b2";  
	static final String PGPASS = "jdbc:postgresql://54.93.194.65:5432/i2b2";  

	//  Database credentials
	
	public static void main(String[] args) {
//		String query;
//		query = "SELECT DISTINCT substring(concept_cd from 1 for 7) AS concept_cd_sub FROM i2b2demodata.concept_dimension WHERE (concept_cd LIKE 'ATC:%' OR concept_cd LIKE 'ICD:%')";
//		query = "SELECT patient_num, sex_cd, birth_date FROM i2b2demodata.patient_dimension WHERE FALSE OR patient_num IN ( SELECT patient_num FROM i2b2demodata.qt_patient_set_collection WHERE result_instance_id = 7)";
//		query = "SELECT patient_num, sex_cd, birth_date FROM i2b2demodata.patient_dimension WHERE FALSE OR patient_num IN ( SELECT patient_num FROM i2b2demodata.qt_patient_set_collection WHERE result_instance_id = 7)";
//		query = "SELECT patient_num, concept_cd_sub, count(*) AS counts FROM ( SELECT patient_num, substring(concept_cd from 1 for 7) AS concept_cd_sub FROM i2b2demodata.observation_fact WHERE concept_cd IN ( SELECT concept_cd FROM i2b2demodata.concept_dimension WHERE (concept_cd LIKE 'ATC:%' OR concept_cd LIKE 'ICD:%')) AND (start_date >= '2007-01-01T00:00:00' AND start_date <= '2008-01-01T00:00:00')/* AND (FALSE OR patient_num IN ( SELECT patient_num FROM i2b2demodata.qt_patient_set_collection WHERE result_instance_id = 7))*/) observations GROUP BY patient_num, concept_cd_sub";
//		query = "SELECT patient_num, concept_cd, count(*) AS count FROM ( SELECT patient_num, substring(concept_cd from 1 for 7) AS concept_cd FROM i2b2demodata.observation_fact WHERE concept_cd IN ( SELECT concept_cd FROM i2b2demodata.concept_dimension WHERE (concept_cd LIKE 'ATC:%' OR concept_cd LIKE 'ICD:%')) AND (start_date >= '2010-01-01T00:00:00' AND start_date <= '2011-01-01T00:00:00') AND (FALSE OR patient_num IN ( SELECT patient_num FROM i2b2demodata.qt_patient_set_collection WHERE result_instance_id = 7))) observations GROUP BY patient_num, concept_cd";
//		query = "SELECT patient_num, 'target' as concept_cd, count(*) AS count FROM ( SELECT patient_num FROM i2b2demodata.observation_fact WHERE concept_cd IN ( SELECT concept_cd FROM i2b2demodata.concept_dimension WHERE concept_path LIKE '\\ICD\\M00-M99\\M50-M54\\M54\\%') AND (start_date >= '2008-01-01T00:00:00' AND start_date <= '2009-01-01T00:00:00') AND (FALSE OR patient_num IN ( SELECT patient_num FROM i2b2demodata.qt_patient_set_collection WHERE result_instance_id = 7))) observations GROUP BY patient_num";
//		query = "SELECT description FROM i2b2demodata.qt_query_result_instance WHERE result_instance_id = 7";
//		query = "SELECT description FROM i2b2demodata.qt_query_result_instance WHERE result_instance_id = 7";
		Connection conn = null;
//		Statement stmt = null;
		PreparedStatement stmt = null;
		Properties conProperties = new Properties();
		BufferedInputStream stream;
		try {
			stream = new BufferedInputStream(new FileInputStream("connection.properties"));
			conProperties.load(stream);
			stream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String user = conProperties.getProperty("user");
		String pass = conProperties.getProperty("password");
		String url = conProperties.getProperty("url");
		Properties properties = new Properties();
		properties.setProperty("TargetCluster", "thor");
		properties.setProperty("ConnectTimeoutMilli", "120000");
		properties.setProperty("ReadTimeoutMilli", "120000");
		properties.setProperty("username", user);
		properties.setProperty("password", pass);
		Scanner in;
		in = new Scanner(System.in);
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getDriver(url).connect(url, properties);
//			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			e1.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		System.out.println("hsql shell v1.01");
		System.out.println("Enter \"\\q\" to quit!");
		while(true) {
			System.out.print("#hpcc ");
			String sql = in.nextLine();
			if (sql.equals("\\q")) break;
			else if (sql.equals("create tables")) createTables(conn);
			else if (sql.equals("fill tables")) fillTables(conn);
			else if (sql.equals("drop empty tables")) dropEmptyTables(conn);
			else if (sql.equals("drop certain tables")) dropCertainTables(conn);
			else
				try {
					stmt = conn.prepareStatement(sql);
	                stmt.setString(1, "DS");
				    ResultSet rs = stmt.executeQuery();
//					stmt = conn.createStatement();
//				    ResultSet rs = stmt.executeQuery(sql);
				    if (rs != null) {
					    System.out.println("Received Result Set");
					    rs.last();
					    int rowCount = rs.getRow();
					    System.out.println("Returned "+rowCount+" rows");
					    if (rowCount > 0)
					    	if (rowCount < 100) printResultSet(rs); else {
					    		System.out.print("Print all these rows? (y|n) :");
					    		if (in.nextLine().equals("y")) printResultSet(rs);
					    	}
				    } else System.out.println("No Result Set returned");
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
		in.close();
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Goodbye, see you soon!");
		

	}

	private static void dropCertainTables(Connection conn) {
		Statement stmt;
		String[] tables = {"code_lookup","encounter_mapping","qt_breakdown_path","qt_privilege","qt_query_result_type","qt_query_status_type","set_type"};
		for (String table : tables) {
			try {
				stmt = conn.createStatement();
				stmt.executeQuery("drop table "+ table);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private static void fillTables(Connection conn) {
		Connection pgconn = null;
		Statement pgstmt = null;
		Statement stmt = null;
		try {
			Class.forName(PG_DRIVER);
			pgconn = DriverManager.getConnection(PG_URL, PGUSER, PGPASS);
			pgconn.setAutoCommit(false);		
			HashMap<String, String> layouts = ECLLayouts.getLayouts();
			String[] tables = {"code_lookup","encounter_mapping","qt_breakdown_path","qt_privilege","qt_query_result_type","qt_query_status_type","set_type"};
			for (String table : tables) {
				try {
					boolean isEmpty = true;
					stmt = conn.createStatement();
					ResultSet rs = stmt.executeQuery("select * from "+table+" limit 1");
					if (rs != null) {isEmpty = !rs.next();}
					if (rs == null || isEmpty) {
						System.out.println("Filling table "+table+"...");
						pgstmt = pgconn.createStatement();
						ResultSet pgrs = pgstmt.executeQuery("select * from "+table);;
					    if (pgrs != null) {
						    System.out.println("Received Result Set");
						    insertDataIntoTable(pgrs, table); 
					    } else System.out.println("No Result Set returned");
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void insertDataIntoTable(ResultSet rs, String table) {
		try {
			String row;
				PrintWriter ecl = new PrintWriter("fill_"+table+".ecl", "UTF-8");
			ecl.println("IMPORT STD;");
			ecl.println(table+"_record := "+ECLLayouts.getLayouts().get(table));
			ecl.print("OUTPUT(DATASET([");
			while(rs.next()) {
				row = "";
				for(int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					row += (row.equals(""))?"":", ";
					String value = rs.getString(i);
					String dataType = ECLLayouts.getECLDataType(table, rs.getMetaData().getColumnName(i)).toLowerCase();
					boolean isNumeric = dataType.contains("unsigned") || dataType.contains("integer");
					value = (value == null) ? (isNumeric?"0":"") : value;
					row += (isNumeric) ? value : "'"+value+"'";
		    	}
				ecl.println("{"+row+"},");
		    }
			String newTablePath = table+System.currentTimeMillis();
			ecl.println("],"+table+"_record),,'~i2b2demodata::"+newTablePath+"',OVERWRITE);");
			ecl.println("SEQUENTIAL(STD.File.StartSuperFileTransaction(),");
			ecl.println("STD.File.AddSuperFile('~i2b2demodata::"+table+"', '~i2b2demodata::"+newTablePath+"'),");
			ecl.println("STD.File.FinishSuperFileTransaction());");
			ecl.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	private static void createTables(Connection conn) {
		Statement stmt = null;
		HashMap<String, String> layouts = ECLLayouts.getLayouts();
		for (String table : layouts.keySet()) {
			try {
				System.out.println("Creating table "+table+"...");
				stmt = conn.createStatement();
				stmt.executeQuery("create table "+table);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}	
	
	private static void dropEmptyTables(Connection conn) {
		Statement stmt = null;
		HashMap<String, String> layouts = ECLLayouts.getLayouts();
		StringBuilder rowCounts = new StringBuilder();
		for (String table : layouts.keySet()) {
			try {
				stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("select count(*) from "+table);
				if (rs != null) {
					String count = "unknown";
					if (rs.next())
						count = rs.getString(1);
					rowCounts.append(table+": "+count+" rows");
					if (count.equals("0")) {
						stmt = conn.createStatement();
						stmt.executeQuery("drop table "+table);
						rowCounts.append("  -- dropped");
					}
					rowCounts.append("\n");
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println(rowCounts.toString());
	}

	public static void printResultSet(ResultSet rs) throws SQLException {
		rs.beforeFirst();
		List<String[]> rows = new ArrayList<String[]>();
		int[] lengths = new int[rs.getMetaData().getColumnCount()];
		String[] header = new String[rs.getMetaData().getColumnCount()];
		for(int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
			header[i - 1] = rs.getMetaData().getColumnName(i);
    		lengths[i - 1] = Math.max(header[i - 1].length(), lengths[i - 1]);
    	}
		while(rs.next()) {
	    	String[] row = new String[rs.getMetaData().getColumnCount()];
			for(int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				String value = rs.getString(i);
				row[i - 1] = (value == null)? "" : value.trim();
	    		lengths[i - 1] = Math.max(row[i - 1].length(), lengths[i - 1]);
	    	}
			rows.add(row);
	    }		
		System.out.print('|');
		for(int i = 0; i < header.length; i++) {
			System.out.format(" %-" + (lengths[i] + 1) + "s|", header[i]);
		}
    	System.out.println();
    	System.out.print('|');
		for(int i = 0; i < header.length; i++) {
			System.out.print(new String(new char[lengths[i] + 2]).replace("\0", "-"));
			System.out.print("|");
		}
    	System.out.println();
		for(String[] row : rows) {
	    	System.out.print('|');
			for(int i = 0; i < row.length; i++) {
	    		System.out.format(" %-" + (lengths[i] + 1) + "s|", row[i]);
	    	}
	    	System.out.println();
	    }
	}

}
