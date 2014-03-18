package src.core;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;

import org.olap4j.OlapConnection;
import org.olap4j.OlapDatabaseMetaData;
import org.olap4j.OlapException;


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
	
	
	
	
	public String getMetadata(String param) throws Exception {

		JsonObject query = Json.createReader(new StringReader(param)).readObject();
		Metadata m = new Metadata(this.olapConnection);
		
		return m.query(query).toString();
	}

	
	
	
	
	public static void main(String[] args) throws Exception {
		
		String query = "{\"schema\" : [ \"Traffic\"],\"cube\" : [],\"dimension\" : [],\"measure\" : [],\"hierarchy\" : [],\"level\" : [],\"member\" : [],\"property\" : []}";
		
		Solap4py p = new Solap4py();
		String metadata = p.getMetadata(query);
	
		System.out.println(metadata);
	}
	
}
