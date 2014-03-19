package core;


import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Schema;


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
	
public String select(String input) {
		String res = new String();
		JsonObject inputJson = Json.createReader(new StringReader(input)).readObject();
		JsonObjectBuilder output = Json.createObjectBuilder();
		boolean error = false;
		Cube cubeObject = null; //TODO todo
		
		String stringSchema;
		try{
			stringSchema = inputJson.getString("schema");
		}
		catch(JsonException e){
			stringSchema = null;
		}
		
		
		if(stringSchema != null){
			output.add("error", "OK");
			JsonObjectBuilder values = Json.createObjectBuilder();
			
			
			
			try{
				Catalog catalog = olapConnection.getOlapCatalog();
				NamedList<Schema> schemas = catalog.getSchemas();
				
				// Get schema named stringSchema
				Schema schema = schemas.get(stringSchema);
				throw new Error(ErrorType.BAD_REQUEST, "Schema not found");
			}
			catch(Error err){
				res = err.getJSON().toString();
				error = true;
			}
			catch(OlapException jex){
				res = new Error(ErrorType.SERVER_ERROR, jex.getMessage()).getJSON().toString();
				error = true;
			}
			
			if (!error){
				
				JsonObject cubeJson;
				try{
					cubeJson = inputJson.getJsonObject("cube");
				}
				catch(JsonException e){
					cubeJson = null;
				}
				
				if(cubeJson != null){
					String cubeName;
					try{
						cubeName = cubeJson.getString("name");
						// TODO Get cube in olap source
					}
					catch(JsonException e){
						cubeName = null;
						res = new Error(ErrorType.BAD_REQUEST, "name of cube cannot be found").getJSON().toString();
					}
					/*
					catch(){
						TODO error if cube does not exist in olap source
					}*/
					
					if(cubeName != null){
						
					
						JsonArray measuresJson;
						try{
							measuresJson = cubeJson.getJsonArray("measures");
						}
						catch(JsonException e){
							measuresJson = null;
							//TODO measures = all measures
						}
						
						JsonObject dimension;
						try{
							dimension = cubeJson.getJsonObject("dimension");
						}
						catch(JsonException e){
							dimension = null;
						}
						
						String dimensionJsonRes;
						try{
							dimensionJsonRes = selectDimension(dimension, cubeObject, res);
						}
						catch(Error e){
							res = e.getJSON().toString();
						}
						
						
					}
					
					
				}
			}
			
		}
		
		res = output.build().toString();
		return res;
	}
	
	
	
	private String selectDimension(JsonObject dimension, Cube cubeObject, String json) throws Error{
		String res = new String(json);
		Dimension dimensionObject = null;
		
		String dimensionName;
		try{
			dimensionName = dimension.getString("name");
			NamedList<Dimension> allDimensions = cubeObject.getDimensions();
			
		}
		catch(JsonException e){
			dimensionName = null;
			throw new Error(ErrorType.BAD_REQUEST, "name of dimension cannot be found");
		}
		
		
		
		boolean range;
		try{
			range = dimension.getBoolean("range");
		}
		catch(JsonException e){
			range = false;
		}
		JsonArray ids;
		try{
			ids = dimension.getJsonArray("id");
		}
		catch(JsonException e){
			//TODO ids = all members
		}
		
		String hierarchyName;
		try{
			hierarchyName = dimension.getString("hierarchy");
		}
		catch(JsonException e){
			//TODO hierarchyName = first hierarchy in xml;
		}
		
		String aggregation;
		try{
			aggregation = dimension.getString("aggregation");
		}
		catch(JsonException e){
			aggregation = null;
		}
		
		boolean measure;
		try{
			measure = dimension.getBoolean("measure");
		}
		catch(JsonException e){
			measure = false;
		}
		
		JsonObject subDimension;
		try{
			subDimension = dimension.getJsonObject("dimension");
		}
		catch(JsonException e){
			subDimension = null;
		}
		if(subDimension != null){
			res = selectDimension(subDimension, cubeObject, res);
		}
			

		return res;
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
