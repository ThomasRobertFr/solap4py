package src.core;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.json.*;

import org.olap4j.OlapConnection;


public class Solap4py {

	private OlapConnection olapConnection;
	
	public Solap4py() {
		try {
			Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");
			Connection connection = DriverManager.getConnection("jdbc:xmla:Server=http://postgres:westcoast@192.168.1.1:8080/geomondrian/xmla");
			this.olapConnection = connection.unwrap(OlapConnection.class);
			
		} catch (ClassNotFoundException e) {
			System.err.println(e);
		} catch (SQLException e) {
			System.err.println(e);
		}
		
	}
	
	public void select() {
		
	}
	
	public void getMetadata() {
		
	}
	
}