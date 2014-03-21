import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import core.Solap4py;

import javax.json.Json;
import javax.json.JsonObject;


public class TestSolap4py {

	@Test
	public void selectTest(){
		Solap4py solap4py = new Solap4py();

		JsonObject model = Json.createObjectBuilder()
				   .add("schema", "Traffic")
				   .add("cube", Json.createObjectBuilder()
						   .add("name", "Traffic")
						   .add("measures", Json.createArrayBuilder()
								   .add("Quantity").add("Value"))
						   .add("dimension", Json.createObjectBuilder()
								   .add("name", "Time")
								   .add("range", false)
								   .add("id", Json.createArrayBuilder()
										   .add("2000").add("2009"))
								   .add("aggregation", false)
								   .add("dimension", Json.createObjectBuilder()
										   .add("name", "Geo")
										   .add("range", false)
										   .add("id", Json.createArrayBuilder()
												   .add("France"))
										   .add("hierarchy", "Name")
										   .add("aggregation", "region")
										   .add("measure", true)
										   .add("dimension", Json.createObjectBuilder()
												.add("name", "Product")
												.add("range", false)
												.add("id", Json.createArrayBuilder())
												.add("measure", true)
											)
									)
							)
					).build();
		String query = model.toString();
		String res = solap4py.select(query);
		System.out.println(res);								   
				   
				   
				   
				
				
	}

}
