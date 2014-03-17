package src.core;

import javax.json.*;

enum ErrorType {
	BAD_REQUEST, NOT_SUPPORTED
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

	// TODO
	public JsonObject getJSON() {
		JsonObject objectJson = Json.createObjectBuilder()
				.add("error", type.toString())
				.add("data",description)
				.build();
		
		return objectJson;
	}

}
