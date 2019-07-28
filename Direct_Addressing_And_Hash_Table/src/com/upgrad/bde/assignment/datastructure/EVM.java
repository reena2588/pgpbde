package com.upgrad.bde.assignment.datastructure;

/**
 * The <b> EVM </b> class is an <b> ADT (Abstract Data Type) </b> for storing the <b> EVM (Electronic Voting Machine) </b> records.
 * <p>
 * This ADT is used for both <b> Direct Addressing </b> and <b> Hash Table </b> implementations.
 * <p>
 * It hosts <b> 02 </b> data elements <b> voterId </b> and <b> candidateId </b> for storing EVM records. 
 * <p>
 * It hosts public getter and setter methods for the data elements.
 */
public class EVM {
	
	private int voterId; 
	private short candidateId;
	
	private EVM next;
	
	/**
	 * The constructor constructs an object of type <b> EVM </b> by setting values for <b> voterId </b> and <b> candidateId </b>.
	 * 
	 * @param  voterId is a 6 digit number of type <b> int </b> which acts as the <b> Key </b> for the record.
	 * @param  candidateId is a 3 digit number of type <b> short </b> which acts as the <b> Value </b> of the record.		 
	 */	 
	public EVM(int voterId, short candidateId) {
		
		this.voterId     = voterId;
		this.candidateId = candidateId;
	
	}
	
	/**
	 * This method returns the voterId for a particular <b> EVM </b> record.
	 * 
	 * @return voterId is a 6 digit number of type <b> int </b> which acts as the <b> Key </b> for the record.
	 */
	public int getVoterID() {
		
		return this.voterId;
	
	}
	
	/**
	 * This method returns the candidateId for a particular <b> EVM </b> record.
	 * 
	 * @return candidateId is a 3 digit number of type <b> short </b> which acts as the <b> Value </b> of the record.
	 */
	public short getCandidateID() {
		
		return this.candidateId;
	
	}
	
	/**
	 * This method sets value for the candidateId field for a particular <b> EVM </b> record.
	 * 
	 * @param candidateId is a 3 digit number of type <b> short </b> which acts as the <b> Value </b> of the record.
	 */	
	public void setCandidateID(short candidateId) {
    	
    	this.candidateId = candidateId;
    
    }
	
	public EVM getNext() {
		
		return this.next;
		
	}
	
	public void setNext(EVM next) {
		
		this.next = next;
		
	}
			
}