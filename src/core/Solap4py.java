package core;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;

import org.olap4j.OlapConnection;
import org.olap4j.OlapException;


public class Solap4py {
	
	private static final String[] levels = { "schema", "cube", "dimension"  };
	
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
	
	public String select(String input) {
		JsonObject inputJson = Json.createReader(new StringReader(input)).readObject();
		JsonObjectBuilder output = Json.createObjectBuilder();
		
		String schema = inputJson.getString("schema");
		JsonObject cubeJson = inputJson.getJsonObject("cube");
		String cubeName = cubeJson.getString("name");
		JsonArray measuresJson = cubeJson.getJsonArray("measures");
		//TODO		
		
		return "";
	}
	
	
	
	
	public String getMetadata(String param) {

		org.olap4j.metadata.Catalog catalog = null;
		
		try {
			catalog = this.olapConnection.getOlapCatalog();
		} catch (OlapException e) {

		}
		
		JsonObject query = Json.createReader(new StringReader(param)).readObject();
		JsonObjectBuilder result = Json.createObjectBuilder();

		for (String level : Solap4py.levels) {
			JsonArray array = query.getJsonArray(level);
			for (JsonString element : array.getValuesAs(JsonString.class)) {
				
			}
		}
		
		
		return result.build().toString();
	}

	
	public static void main(String[] args) {
		
		String query = "{\"schema\" : [ \"Traffic\"],\"cube\" : [],\"dimension\" : [],\"measure\" : [],\"hierarchy\" : [],\"level\" : [],\"member\" : [],\"property\" : []}";
		
		Solap4py p = new Solap4py();
		String metadata = p.getMetadata(query);
	
		System.out.println(metadata);
	}
	
}
