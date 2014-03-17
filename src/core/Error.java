package src.core;

import javax.json.*;

enum Type {
	BAD_REQUEST, NOT_SUPPORTED
};

@SuppressWarnings("serial")
public class Error extends Exception {

	private String description;
	private Type type;

	public Error(Type type, String description) {
		super(description);
		this.type = type;
		this.description = description;

	}

	// TODO
	public void getJSON() {
		
	}

}
