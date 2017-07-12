package com.stackroute.datamunging.query;

public class Restriction {
	
	private String propertyName;
	
	private String propertyValue;
	
	private String condition;
	
	private int propertyPosition;

	public int getPropertyPosition() {
		return propertyPosition;
	}


	public Restriction(int propertyPosition, String propertyName, String propertyValue, String condition) {
		this.propertyPosition = propertyPosition;
		this.propertyName = propertyName;
		this.propertyValue = propertyValue;
		this.condition = condition;
	}

	public String getPropertyName() {
		return propertyName;
	}

	public String getPropertyValue() {
		return propertyValue;
	}

	public String getCondition() {
		return condition;
	}
	
	
	
	

}
