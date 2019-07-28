package com.upgrad.sparkstreaming.project;

import java.io.Serializable;

public class Price implements Serializable {	
	
	/* The class Price is an implementation of a serializable POJO. Each object of the class Price stores a Stock Price record for a particular stock.
	 * It contains the following data elements:
	 * 1. Open Price
	 * 2. High
	 * 3. Low
	 * 4. Close Price
	 * 5. Volume
	 * 
	 * All the above data elements are declared as private member variables accessible through their own getter and setter methods.
	 * */
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Double open;
	
	private Double high;
	
	private Double low;
	
	private Double close;
	
	private Integer volume;
	
	private Double closeSum;
	
	public Double getOpen() {
		
		return open;
	
	}
	
	public Double getHigh() {
	
		return high;
	
	}
	
	public Double getLow() {
		
		return low;
	
	}
	
	public Double getClose() {
	
		return close;
	
	}
	
	public Integer getVolume() {
		
		return volume;
	
	}
	
	public void setOpen(Double open) {
	
		this.open = open;
	
	}
	
	public void setHigh(Double high) {
	
		this.high = high;
	
	}
	
	public void setLow(Double low) {
	
		this.low = low;
	
	}
	
	public void setClose(Double close) {
	
		this.close = close;
	
	}
	
	public void setVolume(Integer volume) {
		
		this.volume = volume;
	
	}
	
	/* Prints the data elements of the Price object on the console */
	public void print() {		
	
		System.out.println("Open:      "+open);
		System.out.println("High:      "+high);
		System.out.println("Low:       "+low);
		System.out.println("Close:     "+close);
		System.out.println("Volume:    "+volume);		
	
	}

	public Double getCloseSum() {
	
		return closeSum;
	
	}

	public void setCloseSum(Double closeSum) {
	
		this.closeSum = closeSum;
	
	}

}