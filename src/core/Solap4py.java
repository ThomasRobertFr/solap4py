package src.core;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;


public class Solap4py {

	
	
	static final String[] levels = { "schema", "cube", "dimension"  };
	
	
	public Solap4py() {
		
	}
	
	public void select() {
		
	}
	
	public String getMetadata(String param) {
		
		JsonObject query = Json.createReader(new StringReader(param)).readObject();
		JsonObjectBuilder result = Json.createObjectBuilder();

		for(String level : Solap4py.levels) {
			JsonArray array = query.getJsonArray(level);
			for(JsonString element : array.getValuesAs(JsonString.class)) {
				
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