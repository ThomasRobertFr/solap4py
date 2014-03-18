package core;

import java.util.List;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Schema;



public class Metadata {

	
	private Catalog catalog;
	
	
	
	
	public Metadata(OlapConnection connection) {
		try {
			this.catalog = connection.getOlapCatalog();
		} catch(OlapException e) {
			e.printStackTrace();
		}
	}
	
	
	
	public JsonObject query(JsonObject query) throws Exception {

		JsonObjectBuilder result = Json.createObjectBuilder();
		result.add("error", "OK");
		JsonObjectBuilder schemas = Json.createObjectBuilder();
		this.getSchemas(query, schemas);
		result.add("data", schemas.build());

		return result.build();
	}
	
	
	private void getSchemas(JsonObject query, JsonObjectBuilder result) throws Exception {
		
		if(query.containsKey("schema") == true) {
			JsonArrayBuilder schemasBuilder = Json.createArrayBuilder();
			NamedList<Schema> schemas = this.catalog.getSchemas();
			
			for(Schema schema : schemas) {
				JsonObjectBuilder schemaBuilder = Json.createObjectBuilder();
				schemaBuilder.add("name", schema.getName());
				this.getCubes(query, schemaBuilder, schema);
				schemasBuilder.add(schemaBuilder.build());
			}
			
			result.add("schemas", schemasBuilder.build());
		}
	}
	
	
	private void getCubes(JsonObject query, JsonObjectBuilder result, Schema schema) throws OlapException {
		if(query.containsKey("cube") == true) {
			JsonArrayBuilder cubesBuilder = Json.createArrayBuilder();
			NamedList<Cube> cubes = schema.getCubes();
			
			for(Cube cube : cubes) {
				JsonObjectBuilder cubeBuilder = Json.createObjectBuilder();
				cubeBuilder.add("name", cube.getName());
				cubeBuilder.add("caption", cube.getCaption());
				this.getDimensions(query, cubeBuilder, cube);
				this.getMeasures(query, cubeBuilder, cube);
				cubesBuilder.add(cubeBuilder.build());
			}
			
			result.add("cubes", cubesBuilder.build());
		}
	}

	
	private void getDimensions(JsonObject query, JsonObjectBuilder result, Cube cube) throws OlapException {
		if(query.containsKey("dimension") == true) {
			JsonArrayBuilder dimensionsBuilder = Json.createArrayBuilder();
			NamedList<Dimension> dimensions = cube.getDimensions();
			
			for(Dimension dimension : dimensions) {
				if(dimension.getName().equals("Measures") == false) {
					JsonObjectBuilder dimensionBuilder = Json.createObjectBuilder();
					dimensionBuilder.add("name", dimension.getName());
					dimensionBuilder.add("caption", dimension.getCaption());
					this.getHierarchies(query, result, dimension);
					dimensionsBuilder.add(dimensionBuilder.build());
				}
			}
			
			result.add("dimensions", dimensionsBuilder.build());
		}
	}

	
	private void getMeasures(JsonObject query, JsonObjectBuilder result, Cube cube) throws OlapException {
		if(query.containsKey("measure") == true) {
			JsonArrayBuilder measuresBuilder = Json.createArrayBuilder();
			List<Measure> measures = cube.getMeasures();
			
			for(Measure measure : measures) {
				JsonObjectBuilder dimensionBuilder = Json.createObjectBuilder();
				dimensionBuilder.add("name", measure.getName());
				dimensionBuilder.add("caption", measure.getCaption());
				dimensionBuilder.add("type", measure.getDatatype().toString());
				dimensionBuilder.add("aggregator", measure.getAggregator().toString());
				measuresBuilder.add(dimensionBuilder.build());
			}
			
			result.add("measures", measuresBuilder.build());
		}
	}
	
	
	private void getHierarchies(JsonObject query, JsonObjectBuilder result, Dimension dimension) throws OlapException {
		if(query.containsKey("hierarchy") == true) {
			JsonArrayBuilder hierarchiesBuilder = Json.createArrayBuilder();
			NamedList<Hierarchy> hierarchies = dimension.getHierarchies();
			
			for(Hierarchy hierarchy : hierarchies) {
				JsonObjectBuilder hierarchyBuilder = Json.createObjectBuilder();
				hierarchyBuilder.add("name", hierarchy.getName());
				hierarchyBuilder.add("caption", hierarchy.getCaption());
				this.getLevels(query, hierarchyBuilder, hierarchy);
				hierarchiesBuilder.add(hierarchyBuilder.build());
			}
			
			result.add("hierarchies", hierarchiesBuilder.build());
		}
	}
	
	
	private void getLevels(JsonObject query, JsonObjectBuilder result, Hierarchy hierarchy) throws OlapException {
		if(query.containsKey("level") == true) {
			JsonArrayBuilder levelsBuilder = Json.createArrayBuilder();
			NamedList<Level> levels = hierarchy.getLevels();
			
			for(Level level : levels) {
				JsonObjectBuilder levelBuilder = Json.createObjectBuilder();
				levelBuilder.add("name", level.getName());
				levelBuilder.add("caption", level.getCaption());
				levelBuilder.add("type", level.getLevelType().toString());
				this.getMembers(query, result, level);
				levelsBuilder.add(levelBuilder.build());
			}
			
			result.add("levels", levelsBuilder.build());
		}
	}
	
	private void getMembers(JsonObject query, JsonObjectBuilder result, Level level) throws OlapException {
		if(query.containsKey("member") == true) {
			JsonArrayBuilder membersBuilder = Json.createArrayBuilder();
			List<Member> members = level.getMembers();
			
			for(Member member : members) {
				JsonObjectBuilder memberBuilder = Json.createObjectBuilder();
				memberBuilder.add("name", level.getName());
				memberBuilder.add("caption", level.getCaption());
				memberBuilder.add("type", level.getLevelType().toString());
				membersBuilder.add(memberBuilder.build());
			}
			
			result.add("members", membersBuilder.build());
		}
	}
}
