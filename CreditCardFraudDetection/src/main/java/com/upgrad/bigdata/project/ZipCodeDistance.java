package com.upgrad.bigdata.project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class ZipCodeDistance {
	
	/* The class ZipCodeDistance is a ZipCode Distance Calculation utility. It calculates the distance between 02 ZipCodes in Kilometers
	 * 
	 * */

	static HashMap<String, ZipCode> zipCodesMap = new HashMap<>();
	
	static final String FILE_NAME = "data/zipCodePosId.csv";
	
	private ZipCodeDistance(){
		
		// Do Nothing
		
	}
	
	/* This method initializes the zipCodesMap HashMap with Key as ZipCode (type as String) and Value as ZipCode (in object of type ZipCode)
	 * The method reads data from a .csv file and creates an object of type ZipCode with all ZipCode details initialized.
	 * The ZipCode object is added to the HashMap with Key as ZipCode (type as String)
	 *  */
	public static void initializeZipCodeMap() throws NumberFormatException, IOException {

		BufferedReader br = null;
		
		try{
			
			br = new BufferedReader(new FileReader(FILE_NAME));
	
			String line = null;
	
			while ((line = br.readLine()) != null) {
			
				String [] str    = line.split(",");
	
				String zipCode   = str[0];
	
				double latitude  = Double.parseDouble(str[1]);
				double longitude = Double.parseDouble(str[2]);
			
				String city      = str[3];
				String stateName = str[4];
				String postId    = str[5];
	
				ZipCode zipCodeData = new ZipCode(latitude, longitude, city, stateName, postId);
	
				zipCodesMap.put(zipCode, zipCodeData);
			
			}
		
		}catch(Exception e){
		
			e.printStackTrace();
			
		}finally{
				
			if (br!= null){
			
				br.close();
				
			}
			
		}
	}
	 
	/* This method calculates the distance between 02 ZipCodes in Kilometers */
	public static double getDistanceViaZipCode(String zipcode1, String zipcode2) {
		
		ZipCode z1 = zipCodesMap.get(zipcode1);
		ZipCode z2 = zipCodesMap.get(zipcode2);
		
		return distance(z1.latitude, z1.longitude, z2.latitude, z2.longitude);
	
	}

	private static double distance(double latitude1, double longitude1, double latitude2, double longitude2) {
		
		double theta = longitude1 - longitude2;
		
		double dist  = Math.sin(deg2rad(latitude1)) * Math.sin(deg2rad(latitude2))
					  + Math.cos(deg2rad(latitude1)) * Math.cos(deg2rad(latitude2)) * Math.cos(deg2rad(theta));
		
		dist = Math.acos(dist);
		
		dist = rad2deg(dist);
		
		dist = dist * 60 * 1.1515;
		
		dist = dist * 1.609344;

		return dist;
	
	}

	private static double rad2deg(double rad) {
		
		return rad * 180.0 / Math.PI;
	
	}

	private static double deg2rad(double deg) {
		
		return deg * Math.PI / 180.0;
	
	}
	
}
