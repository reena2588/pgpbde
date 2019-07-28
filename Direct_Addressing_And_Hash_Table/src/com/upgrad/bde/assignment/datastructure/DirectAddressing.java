package com.upgrad.bde.assignment.datastructure;

/**
 * The <b> DirectAddressing </b> class is an implementation of the Direct Addressing technique. 
 */


public class DirectAddressing {

	/**
	 * For <b> Direct Addressing </b> implementation, this is the data structure used for storing the <b> EVM </b> records. 
	 * <p>
	 * This data structure is used for the <b> ADD() </b> and <b> FIND() </b> operations.
	 */

	private static EVM [] directAddressingArr;
	
	/**
	 * For <b> Direct Addressing </b> implementation, this is the data structure used for storing the <b> VoteCount </b> records. 
	 * <p>
	 * This data structure is used for the <b> COUNT() </b> operation.
	 */
	
	private static VoteCount[] voteCount;
	
	/**
	 * For <b> Direct Addressing </b> implementation, this is the size of the data structure which is used for storing the <b> EVM </b> records. 
	 * <p>
	 * The size is calculated using the following formula:
	 * <p>
	 * <b> size =  (EVM_UPPER_BOUND - EVM_LOWER_BOUND)+1; </b>
	 */
	private static final int EVM_SIZE;
	
	/**
	 * For <b> Direct Addressing </b> implementation, this is the size of the data structure which is used for storing the <b> VoteCount </b> 
	 * records. 
	 * <p>
	 * The size is calculated using the following formula:
	 * <p>
	 * <b> size = (VOTE_COUNT_UPPER_BOUND - VOTE_COUNT_LOWER_BOUND)+1; </b>
	 */
	private static int voteCountSize;
		
	/**
	 * For <b> Direct Addressing </b> implementation, this is the lower bound of the range for <b> voterId </b>.
	 * <p>
	 * This constant is used for deriving the size of the <b> EVM </b> array used for storing the <b> EVM </b> records.
	 * <p>
	 * This constant is used in calculating the index position for storing an <b> EVM </b> record.
	 * <p>
	 * The value of this constant is set to <b> 100000 </b>.
	 */
	public static final int EVM_LOWER_BOUND             = 100000;
	
	/**
	 * For <b> Direct Addressing </b> implementation, this is the upper bound of the range for <b> voterId </b>.
	 * <p>
	 * This constant is used for deriving the size of the <b> EVM </b> array used for storing the <b> EVM </b> records. 
	 * <p>
	 * The value of this constant is set to <b> 999999 </b>.
	 */
	public static final int EVM_UPPER_BOUND             = 999999;
	
	/**
	 * For <b> Direct Addressing </b> implementation, this is the lower bound of the range for <b> candidateId </b>. 
	 * <p>
	 * This constant is used for deriving the size of the <b> VoteCount </b> array used for storing unique candidate IDs and their 
	 * corresponding voter counts.
	 * <p>
	 * This constant is used in calculating the index position for storing the <b> VoteCount </b> records.
	 * <p>
	 * The value of this constant is set to <b> 100 </b>.
	 */
	public static final short VOTE_COUNT_LOWER_BOUND    = 100;
	
	/**
	 * For <b> Direct Addressing </b> implementation, this is the upper bound of the range for <b> candidateId </b>. 
	 * <p>
	 * This constant is used for deriving the size of the <b> VoteCount </b> array used for storing unique candidate IDs and their 
	 * corresponding voter counts.
	 * <p>
	 * The value of this constant is set to <b> 999 </b>.
	 */
	public static final short VOTE_COUNT_UPPER_BOUND    = 999;

	static {
		
		EVM_SIZE              = (EVM_UPPER_BOUND - EVM_LOWER_BOUND)+1;
		voteCountSize         = (VOTE_COUNT_UPPER_BOUND - VOTE_COUNT_LOWER_BOUND)+1;
		
		directAddressingArr   = new EVM[EVM_SIZE];
		voteCount             = new VoteCount[voteCountSize];

	}
	
	private DirectAddressing() {
		
		// Not Defined...
		
	}
	
	public static EVM [] getDirectAddressingArr() {
		
		return directAddressingArr;
	
	}

	/**
	 * This method is an implementation of the <b> ADD() </b> operation for <b> Direct Addressing </b>.
	 * <p> 
	 * It adds a record of type <b> EVM </b> into a static array data structure of type <b> EVM </b> which stores the record using 
	 * the <b> Direct Addressing </b> implementation technique. 
	 * <p>
	 * The record consists of a <b> voterId </b> and a <b> candidateId </b>.
	 * <p>
	 * The record is added at a specific index position which is calculated using the following formula:
	 * <p>
	 * <b> index = voterId - EVM_LOWER_BOUND </b>
	 * <p>  
	 * The worst case run time complexity for this operation is <b> O(1) </b> i.e. irrespective of the number of records, the time taken for 
	 * adding a record is always constant.
	 * <p>
	 * This method accepts <b> Unique </b> records i.e. the <b> Key (voterId) </b> of each record is unique.
	 * 
	 * @param  voterId is a 6 digit number of type <b> int </b> which acts as the <b> Key </b> for the record.
	 * @param  candidateId is a 3 digit number of type <b> short </b> which acts as the <b> Value </b> of the record. 
	 */	
	public static void ADD(int voterId, short candidateId) {
		
		try {
		
			int index = voterId-EVM_LOWER_BOUND;
			
			directAddressingArr[index] = new EVM(voterId, candidateId);	
			
		}catch(Exception e) {
			
			e.printStackTrace();
			
		}
	}
	
	/**
	 * This method is an implementation of the <b> FIND() </b> operation for <b> Direct Addressing </b>.
	 * <p> 
	 * It finds a record of type <b> EVM </b> from a static array data structure of type <b> EVM </b>. 
	 * <p>
	 * It takes as input a <b> Key (voterId) </b> and returns the <b> Value (candidateId) </b> corresponding to the Key.
	 * <p>
	 * It returns 0 if the record containing the <b> Key (voterId) </b> does not exist.
	 * <p>
	 * The record available within the data structure is retrieved by calculating the specific index position (at which the record is available) 
	 * using the following formula:
	 * <p>
	 * <b> index = voterId - EVM_LOWER_BOUND </b>
	 * <p>  
	 * The worst case run time complexity for this operation is <b> O(1) </b> i.e. irrespective of the number of records, the time taken for 
	 * retrieving a particular record is always constant.
	 * <p>
	 * @param  voterId is a 6 digit number of type <b> int </b> which acts as the <b> Key </b> for finding the record.
	 * @return candidateId is a 3 digit number of type <b> short </b> which acts as the <b> Value </b> of the record. 
	 */	
	public static short FIND(int voterId) {
		
		try {
			
			int index = voterId-EVM_LOWER_BOUND;
			
			if (index>=0 && directAddressingArr[index]!=null) {
				
				return directAddressingArr[index].getCandidateID();
			
			}
			
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}
		
		return (short)0;
	
	}
	
	/**
	 * This method is an implementation of the <b> COUNT() </b> operation for <b> Direct Addressing </b>.
	 * <p> 
	 * It finds a record of type <b> VoteCount </b> from a static array data structure of type <b> VoteCount </b>. 
	 * <p>
	 * It takes as input a <b> Key (candidateId) </b> and returns the <b> Value (voterCount) </b> corresponding to the Key.
	 * <p>
	 * It returns 0 if the record containing the <b> Key (candidateId) </b> does not exist.
	 * <p>
	 * The record available within the data structure is retrieved by calculating the specific index position (at which it is available) 
	 * using the following formula:
	 * <p>
	 * <b> size = (VOTE_COUNT_UPPER_BOUND - VOTE_COUNT_LOWER_BOUND)+1; </b>
	 * <p>  
	 * The worst case run time complexity for this operation is <b> O(1) </b> i.e. irrespective of the number of records, the time taken for 
	 * retrieving a particular record is always constant.
	 * <p>
	 * @param  candidateId is a 3 digit number of type <b> short </b> which acts as the <b> Key </b> for finding the record.
	 * @return voterCount is an <b> int </b> which represents the total votes received by a particular candidate. 
	 */	
	public static int COUNT(short candidateId) {
		
		try {

			short index = (short) (candidateId - VOTE_COUNT_LOWER_BOUND);

			if (index>=0 && voteCount[index]!=null) {
				
				return voteCount[index].getVoterCount();
					
			}
			
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}
		
		return 0;
	}
	
	/**
	 * For <b> Direct Addressing </b> implementation, this is a helper method used for calculating total votes for all candidates.
	 * <p> 
	 * It adds a record of type <b> VoteCount </b> to a static array data structure of type <b> VoteCount </b>, if the record with a 
	 * particular Key (candidateId) does not exists. 
	 * <p>
	 * By default, the Value (voterCount) for the record is set to 1. 
	 * <p>
	 * If the record with a particular Key (candidateId) already exists then the Value (voterCount) is incremented by 1. 
	 * <p>
	 * The method takes as input a <b> Key (candidateId) </b>.
	 * <p>
	 * @param  candidateId is a 3 digit number of type <b> short </b> which acts as the <b> Key </b> for storing the record. 
	 */	
	public static void calculateVoterCount(short candidateId) {
		
		try {
			
			short index = (short) (candidateId - VOTE_COUNT_LOWER_BOUND);
			
			if (index>=0) {
				
				if (voteCount[index] != null) {

					voteCount[index].incrementVoterCount();

				}else{ 
									
					voteCount[index] = new VoteCount(candidateId);
		
				}
			}
			
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}
		
	}

}
