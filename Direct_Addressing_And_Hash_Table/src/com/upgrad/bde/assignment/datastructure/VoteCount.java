package com.upgrad.bde.assignment.datastructure;

/**
 * The <b> VoteCount </b> class is an <b> ADT (Abstract Data Type) </b> for storing the <b> Candidate Vote Count </b> records.
 * <p> 
 * This ADT is used for both <b> Direct Addressing </b> and <b> Hash Table </b> implementations.
 * <p>
 * It hosts <b> 02 </b> data elements <b> candidateId </b> and <b> voterCount </b> for storing candidate's vote count data. 
 * <p>
 * It hosts public getter and setter methods for the data elements.
 */
public class VoteCount{
	
	private short candidateId;
	private int voterCount;
	
	private VoteCount next;

	/**
	 * The constructor constructs an object of type <b> VoteCount </b> by setting values for <b> candidateId </b> and <b> voterCount </b>.
	 * <p>
	 * By default, the voterCount is set to 1. 
	 * 
	 * @param  candidateId is a 3 digit number of type <b> short </b> which acts as the <b> Value </b> of the record.		 
	 */
	public VoteCount (short candidateId){
		
		this.candidateId = candidateId;
		this.voterCount  = 1;
	
	}
	
	/**
	 * This method returns the voter count for a particular <b> VoteCount </b> record.
	 * 
	 * @return voter_id is a number of type <b> int </b> which represents total votes for a candidate.
	 */
	public int getVoterCount() {
		
		return this.voterCount;
	
	}
	
	/**
	 * This method returns the candidateId for a particular <b> VoteCount </b> record.
	 * 
	 * @return candidateId is a 3 digit number of type <b> short </b> which acts as the <b> Key </b> for the record.
	 */
	public short getCandidateID() {
		
		return this.candidateId;
		
	}
	
	/**
	 * This method increments the voter count for a particular <b> VoteCount </b> record by 1.
	 * 
	 */
	public void incrementVoterCount() {
		
		this.voterCount++;
	
	}
	
	public VoteCount getNext() {
		
		return this.next;
		
	}
	
	public void setNext(VoteCount next) {
		
		this.next = next;
		
	}
	
}