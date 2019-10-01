package com.upgrad.bigdata.project;

public class ZipCode {
	
	/* The class ZipCode is used as part of the ZipCode Distance Calculation utility. It contains a parameterized constructor which stores details regarding a ZipCode.
	 * 
	 * It contains the following data elements:
	 * 1. latitude
	 * 2. longitude
	 * 3. city
	 * 4. state_name
	 * 5. postId
	 * 
	 * */
	
	double latitude;
	double longitude;
	String city;
	String stateName;
	String postId;

	public ZipCode(double latitude, double longitude, String city, String stateName, String postId) {
		
		this.latitude = latitude;
		this.longitude = longitude;
		this.city = city;
		this.stateName = stateName;
		this.postId = postId;
	
	}

}
