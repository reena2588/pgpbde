package com.upgrad.bigdata.project;

import java.io.Serializable;

public class TransactionPOJO implements Serializable {
	
	/* The class TransactionPOJO is an implementation of a serializable POJO (Plain Old Java Object). Each object of the class TransactionPOJO stores a Transaction record for a particular transaction received from Kafka Server.
	 * It contains the following data elements:
	 * 1. cardID
	 * 2. memberID
	 * 3. amount
	 * 4. posID
	 * 5. postCode
	 * 6. transactionDate
	 * 7. status
	 * 
	 * All the above elements are declared as private member variables accessible through their own getter and setter methods.
	 * */

	private static final long serialVersionUID = 1L;
	
	private String cardID;
	private String memberID;
	private String amount;
	private String posID;
	private String postCode;
	private String transactionDate;
	private String status;
	
	public String getCardID() {
		
		return cardID;
	
	}
	
	public void setCardID(String cardID) {
	
		this.cardID = cardID;
	
	}
	
	public String getMemberID() {
	
		return memberID;
	
	}
	
	public void setMemberID(String memberID) {
	
		this.memberID = memberID;
	
	}
	
	public String getAmount() {
	
		return amount;
	
	}
	
	public void setAmount(String amount) {
	
		this.amount = amount;
	
	}
	
	public String getPosID() {
	
		return posID;
	
	}
	
	public void setPosID(String posID) {
	
		this.posID = posID;
	
	}
	
	public String getPostCode() {
	
		return postCode;
	
	}
	
	public void setPostCode(String postCode) {
	
		this.postCode = postCode;
	
	}
	
	public String getTransactionDate() {
	
		return transactionDate;
	
	}
	
	public void setTransactionDate(String transactionDate) {
	
		this.transactionDate = transactionDate;
	
	}
	
	/* Prints the data elements of the TransactionPOJO object on the console */
	public void print() {
		
		System.out.println("");
		System.out.println("Card ID:            "+cardID);
		System.out.println("Member ID:          "+memberID);
		System.out.println("Amount:             "+amount);
		System.out.println("POS ID:             "+posID);
		System.out.println("Post Code:          "+postCode);
		System.out.println("Transaction Date:   "+transactionDate);
		System.out.println("Transaction Status: "+status);
		System.out.println("");
		System.out.println("----------------------------------------------------");
	
	}
	
	/* Overridden implementation of the toString() method */
	public String toString() {				
		
		return "Card ID: "+cardID+" | "+"Member ID: "+memberID+" | "+"Amount: "+amount+" | "+"POS ID: "
				+posID+" | "+"Post Code: "+postCode+" | "+"Transaction Date: "+transactionDate+" | "
				+ "Transaction Status: "+status;
	}

	public String getStatus() {
	
		return status;
	
	}

	public void setStatus(String status) {
		
		this.status = status;
	
	}
	
}
