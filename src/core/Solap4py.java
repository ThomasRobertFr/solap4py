package core;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.olap4j.Axis;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Schema;
import org.olap4j.query.Query;
import org.olap4j.query.QueryDimension;


public class Solap4py {

	private OlapConnection olapConnection;
	private Catalog catalog;
	
	public Solap4py() {
		try {
			Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");
			Connection connection = DriverManager
					.getConnection("jdbc:xmla:Server=http://postgres:westcoast@192.168.1.1:8080/geomondrian/xmla");
			this.olapConnection = connection.unwrap(OlapConnection.class);
			this.catalog = olapConnection.getOlapCatalog();

		} catch (ClassNotFoundException e) {
			System.err.println(e);
		} catch (SQLException e) {
			System.err.println(e);
		}

	}

	public String select(String input) {
		String result = null;
		JsonObject inputJson = Json.createReader(new StringReader(input)).readObject();
		
		Schema schema = null;
		Cube cubeObject = null; // TODO todo
		
		try {
			try {
				// If not, result stays "null"
				if (inputJson.containsKey("schema")) {
					// If not, result stays "null"
					if (inputJson.containsKey("cube")) {
						schema = this.catalog.getSchemas().get(inputJson.getString("schema"));
						JsonObject cubeJson = inputJson.getJsonObject("cube");
						JsonArray measuresJson;
						if (cubeJson.containsKey("name")) {
							// Get the Cube object (Olap4J) associated with this name
							cubeObject = schema.getCubes().get(cubeJson.getString("name"));
							
							if (cubeJson.containsKey("measures")) {
								measuresJson = cubeJson.getJsonArray("measures");
								// Measures from array
							} else {
								throw new Error(ErrorType.BAD_REQUEST, "No measure specified");
							}
							
							// TODO by Pierre.
							if (cubeJson.containsKey("dimension")){
								// Not implemented
								selectDimension(cubeJson.getJsonObject("dimension"), cubeObject );
							} else {
								// All the dimensions are aggregated
							}
						}
						else {
							throw new Error(ErrorType.BAD_REQUEST, "Cube name not specified");
						}
						
						// Initialize the query to be executed
						Query myQuery = new Query("Select Query", cubeObject);

						QueryDimension measuresDim = myQuery.getDimension("Measures");
						// Put the "Measures" dimension on columns of the expected result
						myQuery.getAxis(Axis.COLUMNS).addDimension(measuresDim);

						// Add each measures on columns
						for (JsonValue measureJson : measuresJson) {
							myQuery.getDimension("Measures").include(cubeObject.lookupMember(IdentifierNode.ofNames("Measures", measureJson.toString()).getSegmentList()));
						}
					}
				}
			} 
			catch (OlapException olapEx) {
				throw new Error(ErrorType.SERVER_ERROR, olapEx.getMessage());
			}
			catch (SQLException sqlEx) {
				throw new Error(ErrorType.SERVER_ERROR, sqlEx.getMessage());
			}
		} catch (Error err) {
			result = err.getJSON().toString();
		}
		
		return result;
	}

	private String selectDimension(JsonObject dimension, Cube cubeObject /*,
			String json*/) throws Error {
		String res=null; //= new String(json);
		Dimension dimensionObject = null;
		Hierarchy hierarchyObject = null;

		String dimensionName;
		try {
			dimensionName = dimension.getString("name");
			NamedList<Dimension> allDimensions = cubeObject.getDimensions();
			dimensionObject = allDimensions.get(dimensionName);
		} catch (JsonException e) {
			dimensionName = null;
			throw new Error(ErrorType.BAD_REQUEST,
					"name of dimension cannot be found");
		}

		NamedList<Hierarchy> allHierarchies = dimensionObject.getHierarchies();

		boolean range;
		try {
			range = dimension.getBoolean("range");
		} catch (JsonException e) {
			range = false;
		}

		String hierarchyName;
		try {
			hierarchyName = dimension.getString("hierarchy");
			hierarchyObject = allHierarchies.get(hierarchyName);
		} catch (JsonException e) {
			if (allHierarchies.isEmpty())
				throw new Error(ErrorType.NO_HIERARCHY, new String(
						"No Hierarchy can be found in ").concat(dimensionName)
						.concat(" dimension"));
			else
				hierarchyObject = dimensionObject.getDefaultHierarchy();
		}

		JsonArray ids;
		try {
			ids = dimension.getJsonArray("id");
			if (range && ids.size() != 2)
				throw new Error(ErrorType.DIMENSION_ID_COUNT,
						"there should be 2 ID because of range = true");
		} catch (JsonException e) {
			NamedList<Level> allLevels = hierarchyObject.getLevels();
			ArrayList<List<Member>> allMembers = new ArrayList<List<Member>>();
			for (Level i : allLevels) {
				try {
					allMembers.add(i.getMembers());
				} catch (OlapException e1) {
					throw new Error(ErrorType.BAD_REQUEST, "Database error");
				}
			}
		}

		String aggregation = null;
		try {
			aggregation = dimension.getString("aggregation");
		} catch (JsonException e) {
			aggregation = null;
		}

		boolean measure;
		try {
			measure = dimension.getBoolean("measure");
		} catch (JsonException e) {
			measure = false;
		}

		JsonObject subDimension;
		try {
			subDimension = dimension.getJsonObject("dimension");
		} catch (JsonException e) {
			subDimension = null;
		}
		if (subDimension != null) {
			//res = selectDimension(subDimension, cubeObject, res);
		}

		return res;
	}

	public String getMetadata(String param) throws Exception {

		JsonObject query = Json.createReader(new StringReader(param))
				.readObject();
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
