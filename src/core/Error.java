package core;

import javax.json.Json;
import javax.json.JsonObject;

enum ErrorType {
	BAD_REQUEST, 
	NOT_SUPPORTED, 
	SERVER_ERROR, 
	NO_HIERARCHY,
	DIMENSION_ID_COUNT
};

@SuppressWarnings("serial")
public class Error extends Exception {

	private String description;
	private ErrorType type;

	public Error(ErrorType type, String description) {
		super(description);
		this.type = type;
		this.description = description;

	}

	public JsonObject getJSON() {

		JsonObject objectJson = Json.createObjectBuilder()
				.add("error", type.toString()).add("data", description).build();

		return objectJson;

	}

	public static void controle() throws Error {
		throw new Error(ErrorType.BAD_REQUEST, "Error description");
	}

	public static void main(java.lang.String[] args) {
		try {
			controle();
		} catch (Error e) {
			System.out.println(e.getJSON().toString());
		}
		;
	}

}
