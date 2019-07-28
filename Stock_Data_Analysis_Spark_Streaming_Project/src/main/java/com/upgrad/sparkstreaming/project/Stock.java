package com.upgrad.sparkstreaming.project;

import java.io.Serializable;

public class Stock implements Serializable {
	
	/* The class Stock is an implementation of a serializable POJO with composition of Price data. Each object of the class Stock stores a Stock record for a particular stock.
	 * It contains the following data elements:
	 * 1. symbol
	 * 2. timestamp
	 * 3. priceData - which is of type Price. Price is also a serializable POJO.
	 * 4. total number of periods - this is defined as a static variable.
	 * 
	 * All the above elements are declared as private member variables accessible through their own getter and setter methods.
	 * */
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String symbol;
	
	private String timestamp;
	
	private Price priceData;
	
	private static int totalNoPeriod=0;
	
	public String getSymbol() {
		
		return symbol;
	
	}
	
	public String getTimestamp() {
	
		return timestamp;
	
	}
	
	public void setSymbol(String symbol) {
		
		this.symbol = symbol;
	
	}
	
	public void setTimestamp(String timestamp) {
		
		this.timestamp = timestamp;
	
	}

	public Price getPriceData() {
		
		return priceData;
	
	}

	public void setPriceData(Price priceData) {
		
		this.priceData = priceData;
	
	}
	
	/* Prints the data elements of the Stock object on the console */
	public void print() {		
	
		System.out.println("Symbol:    "+symbol);
		System.out.println("Timestamp: "+timestamp);
		priceData.print();	
	
	}

	public static int getTotalNoPeriod() {
	
		return totalNoPeriod;
	
	}
	
	public static void setTotalNoPeriod(int totalNoPeriod) {
		
		Stock.totalNoPeriod = totalNoPeriod;
	
	}
	
	/* Reset the total number of period to 0. */
	public static void resetTotalNoPeriod() {
		
		Stock.totalNoPeriod = 0;
	
	}
	
	/* Increments the total number of period by 1. */
	public static void incrTotalNoPeriod() {
		
		Stock.totalNoPeriod = Stock.totalNoPeriod+1;
	
	}
	
}