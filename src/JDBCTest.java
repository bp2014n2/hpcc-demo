import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;


public class JDBCTest {

	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "de.hpi.hpcc.main.HPCCDriver";
	
	public static void main(String[] args) {

		Properties properties = new Properties();
		BufferedInputStream stream;
		try {
			stream = new BufferedInputStream(new FileInputStream("connection.properties"));
			properties.load(stream);
			stream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String user = properties.getProperty("user");
		String pass = properties.getProperty("password");
		String url = properties.getProperty("url");
		
		Connection conn = null;
		Statement stmt = null;
		Scanner in;
		in = new Scanner(System.in);
		System.out.println("hsql shell v1.01");
		System.out.println("Enter \"\\q\" to quit!");
		while(true) {
			System.out.print("#hpcc ");
			String sql = in.nextLine();
			if (sql.equals("\\q")) break;
			try {
				Class.forName(JDBC_DRIVER);
				conn = DriverManager.getConnection(url,user,pass);
				stmt = conn.createStatement();
			    ResultSet rs = stmt.executeQuery(sql);
			    printResultSet(rs);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		in.close();
		System.out.println("Goodbye, see you soon!");
		
	}
	
	public static void printResultSet(ResultSet rs) throws SQLException {
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
