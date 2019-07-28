package com.upgrad.bde.assignment.datastructure;

/**
 * The <b> Hash Table </b> class is an implementation of the Hash Table technique. 
 */

public class HashTable {

	/**
	 * For <b> HashTable </b> implementation, this is the data structure which is used for storing the <b> EVM </b> records.
	 * <p>
	 * This data structure is used for the <b> ADD() </b> and <b> FIND() </b> operations.
	 */

	private static EVM[] hashTableArr;
	
	/**
	 * For <b> Hash Table </b> implementation, this is the data structure which is used for storing the <b> VoteCount </b> records.
	 * <p>
	 * This data structure is used for the <b> COUNT() </b> operation.
	 */
	private static VoteCount[] voteCountHashTable;
	
	/**
	 * For <b> Hash Table </b> implementation, this is the load factor used for rehashing the hash table data structure 
	 * i.e. the hash table is automatically rehashed when the load in the hash table data structure is about to equate the load factor. 
	 * <p>
	 * This constant is used in calculating the load in a hash table.
	 * <p>
	 * The value of this constant is set to <b> 0.75 </b>.
	 */
	private static final double LOAD_FACTOR              = 0.75;
	
	/**
	 * For <b> Hash Table </b> implementation, this is the total bucket (array) size for the hash table data structure  <b> hashTable </b> 
	 * which is an array of type <b> EVM </b>.
	 * <p>
	 * The initial size is set to 10 and the size is doubled to the next available prime number on every rehashing occasion. 
	 */
	private static int totalHTBucket                     = 10;
	
	/**
	 * For <b> Hash Table </b> implementation, this is the total bucket (array) size for the hash table data structure <b> voteCountHashTable </b> 
	 * which is an array of type <b> VoteCount </b>.
	 * <p>
	 * The initial size is set to 10 and the size is doubled to the next available prime number on every rehashing occasion. 
	 */
	 private static int totalVoteCountBucket             = 10;
		
	/**
	 * For <b> Hash Table </b> implementation, this is the total size for the hash table data structure <b> hashTable </b> 
	 * which is an array of type <b> EVM </b>.
	 * 
	 * The initial size is set to 0 and is incremented by 1 every time a new record of type <b> EVM </b> is added to the hash table. 
	 */
	private static int hashTableSize                    = 0;
	
	/**
	 * For <b> Hash Table </b> implementation, this the total size for the hash table data structure <b> voteCountHashTable </b> 
	 * which is an array of type <b> VoteCount </b>.
	 * 
	 * The initial size is set to 0 and is incremented by 1 every time a new record of type <b> VoteCount </b> is added to the hash table. 
	 */
	private static int voteCountHTSize                  = 0;

	static {
		
		hashTableArr            = new EVM [totalHTBucket];
		voteCountHashTable      = new VoteCount[totalVoteCountBucket];
    
	}
	
	private HashTable() {
		
		// Not Defined...
		
	}
	
	public static EVM[] gethashTableArr() {
		
		return hashTableArr;
		
	}

	/**
	 * This method is an implementation of the <b> ADD() </b> operation for <b> Hash Table. </b>.
	 * <p> 
	 * It adds a record of type <b> EVM </b> into a hash table data structure of type <b> EVM </b>. 
	 * <p>
	 * The record consists of a <b> voterId </b> and <b> candidateId </b>.
	 * <p>
	 * Using a hash function, an index location or bucket is identified where the record of type <b> EVM </b> is stored in a LinkedList.
	 * <p> 
	 * Here, the <b> Separate Chaining </b> technique for <b> Collision Resolution </b> is used. 
	 * <p>
	 * The new records of type <b> EVM </b> are added to the head of the LinkedList in order to avoid linear probing time while performing 
	 * the <b> ADD() </b> operation. 
	 * <p>
	 * The worst case run time complexity for this operation is <b> O(1) </b> i.e. irrespective of the number of records, the time taken 
	 * for adding a record is always constant.
	 * <p>
	 * The hash table data structure is automatically rehashed by doubling the size of the data structure to the next available prime number.
	 * <p> 
	 * The rehashing operation is performed when: 
	 * <p>
	 * the load in the data structure is about to reach to the <b> loadFactor </b> AND 
	 * <p>
	 * before adding the record in the hash table.
	 * <p>
	 * This method accepts <b> Unique </b> records i.e. the <b> Key (voterId) </b> of each record is unique.
	 * 
	 * @param  voterId is a 6 digit number of type <b> int </b> which acts as the <b> Key </b> for the record.
	 * @param  candidateId is a 3 digit number of type <b> short </b> which acts as the <b> Value </b> of the record. 
	 */		   
	public static void ADD(int voterId, short candidateId) {
        
        try {
        	
        	int index = getBucketIndex(voterId,totalHTBucket);

	        boolean isHashingRequired = (voteCountHTSize == (LOAD_FACTOR * totalVoteCountBucket) -1);
	        
	        if (isHashingRequired) {
	        	
	        	rehashVoteCount();
	
	        }
        	
	        hashTableSize++;
	        
	        EVM head = hashTableArr[index];
	        
	        EVM newEVM = new EVM(voterId, candidateId);
	        
	        newEVM.setNext(head);
	        
	        hashTableArr[index] = newEVM;	 
	        	            
        }catch (Exception e) {
	        	
        	e.printStackTrace();
	        	
        }
        
    }
	
	/**
	 * This method is an implementation of the <b> FIND() </b> operation for <b> Hash Table </b>.
	 * <p> 
	 * It finds a record of type <b> EVM </b> from a hash table data structure of type <b> EVM </b>. 
	 * <p>
	 * It takes as input a <b> Key (voterId) </b> and returns the <b> Value (candidateId) </b> corresponding to the Key.
	 * <p>
	 * It returns 0 if the record containing the <b> Key (voterId) </b> does not exist.
	 * <p>
	 * Using a hash function, an index location or bucket is identified where the record of type <b> EVM </b> is stored in a LinkedList. 
	 * <p>
	 * The LinkedList is linearly traversed to find the corresponding record.
	 * <p>  
	 * The worst case run time complexity for this operation is <b> O(N) </b>. 
	 * <p>
	 * The index or bucket storing the record is reached in constant time <b> O(1) </b> 
	 * <p>
	 * Now the LinkedList is linearly traversed to find the corresponding record. 
	 * <p>
	 * The worst case run time complexity is <b> O(N) </b> if the record to be found is at the last node of the LinkedList. 
	 * <p>
	 * Therefore, <b> O(1) </b> + <b> O(N) </b> = <b> O(N) </b> is the worst case time complexity for the <b> FIND() </b> operation.
	 * <p>
	 * @param  voterId is a 6 digit number of type <b> int </b> which acts as the <b> Key </b> for finding the record.
	 * @return candidateId is a 3 digit number of type <b> short </b> which acts as the <b> Value </b> of the record. 
	 */
	public static short FIND(int voterId){
		
		try {
		
			int index = getBucketIndex(voterId,totalHTBucket);
		    
			EVM head = hashTableArr[index];
		 
			while (head != null){
				
				if (head.getVoterID() == voterId) {
		            	
					return head.getCandidateID();
				}
		        
				head = head.getNext();
		    
			}
		
		}catch (Exception e) {
			
			e.printStackTrace();
			
		}
		
	    return 0;
	
	}
	
	
	/**
	 * This method is an implementation of the <b> COUNT() </b> operation for <b> Hash Table </b>.
	 * <p> 
	 * It finds a record of type <b> VoteCount </b> from a hash table data structure of type <b> VoteCount </b>. 
	 * <p>
	 * It takes as input a <b> Key (candidateId) </b> and returns the <b> Value (voterCount) </b> corresponding to the Key.
	 * <p>
	 * It returns 0 if the record containing the <b> Key (candidateId) </b> does not exist.
	 * <p>
	 * Using a hash function, an index location or bucket is identified where the record of type <b> VoteCount </b> is stored 
	 * in a LinkedList. 
	 * <p>
	 * If the LinkedList is empty then a Node of type <b> VoteCount </b> is added to the head node. 
	 * <p>
	 * By default, the <b> Value (voterCount) </b> is set to 1 when the node is added to the LinkedList.
	 * <p> 
	 * If the LinkedList is not empty then the LinkedList is linearly traversed to find the corresponding record. 
	 * <p>
	 * The <b> Value (voterCount) </b> is incremented by 1 for the existing record found during linear traversal.
	 * <p>
	 * The worst case run time complexity for this operation is <b> O(N) </b>. 
	 * <p>
	 * The index or bucket holding the record is reached in constant time <b> O(1) </b> 
	 * <p>
	 * The LinkedList is linearly traversed to find the record which is <b> O(N) </b> if the record to be found is 
	 * at the last node of the LinkedList. 
	 * <p>
	 * Therefore, <b> O(1) </b> + <b> O(N) </b> = <b> O(N) </b> is the worst case time complexity for the <b> FIND() </b> operation.
	 * <p>
	 * @param  candidateId is a 3 digit number of type <b> short </b> which acts as the <b> Key </b> for finding the record.
	 * @return voterCount is an <b> int </b> which represents the total votes received by a particular candidate.
	 */
	public static int COUNT(short candidateId) {
		
		try {

			int index = getBucketIndex(candidateId,totalVoteCountBucket);

			if (index>=0) {
				
				VoteCount head = voteCountHashTable[index];
		 
				while (head != null){
				
					if (head.getCandidateID() == candidateId) {
		            	
						return head.getVoterCount();
					}
		        
					head = head.getNext();
		    
				}
				
			}
			
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}
		
		return 0;

	}
	
	
	/**
	 * For <b> Hash Table </b> implementation, this is a helper method used for calculating total votes for all candidates.
	 * <p> 
	 * It adds a record of type <b> VoteCount </b> to a hash table data structure of type <b> VoteCount </b>, if the record with a 
	 * particular Key (candidateId) does not exists. 
	 * <p> 
	 * By default, the Value (voterCount) for the record is set to 1.
	 * <p> 
	 * If the record with a particular Key (candidateId) already exists in the data structure then the Value (voterCount) is incremented by 1. 
	 * <p>
	 * The method takes as input a <b> Key (candidateId) </b>.
	 * <p>
	 * @param  candidateId is a 3 digit number of type <b> short </b> which acts as the <b> Key </b> for storing the record. 
	 */
	static void calculateVoterCount(short candidateId) {
		
		try {
			
			int index = getBucketIndex(candidateId,totalVoteCountBucket);
	        
	                VoteCount head = voteCountHashTable[index];
	        
	                while (head != null) {
	        	
	                	if (head.getCandidateID() == candidateId) {
         	
	                		head.incrementVoterCount();

	                		return;
	                
	            		}
	            
	            		head = head.getNext();
	        
	                }
	        
	                boolean isHashingRequired = (voteCountHTSize == (LOAD_FACTOR * totalVoteCountBucket) -1);
	        
	        
	                if (isHashingRequired) {
	        	
	                	rehashVoteCount();
	
	                }
	 
	                voteCountHTSize++;
	        
	                head = voteCountHashTable[index];
	        
	                VoteCount newVoteCount = new VoteCount(candidateId);
	        
	                newVoteCount.setNext(head);
	        
	                voteCountHashTable[index] = newVoteCount;
      
	       	        			
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}
		
	}
	
	/**
	 * For <b> Hash Table </b> implementation, this is a helper method used for identifying the index or bucket location within the hash table 
	 * structure for storing or retrieving a record.
	 * <p> 
	 * The method takes as input a <b> Key </b> which is an ID.
	 * <p>
	 * This could be the voterId in case of <b> EVM </b> hash table data structure, OR 
	 * <p>
	 * This could be the candidateId in case of <b> VoteCount </b> hash table data structure.
	 * <p> 
	 * This method takes as input a bucketSize of a hash table data structure. 
	 * <p>
	 * It returns the computed index or bucket location within the hash table structure.
	 * <p>
	 * The index is computed using the following formula:
	 * <b> index = Key.hashCode % bucketSize </b>
	 * 
	 * @param  id is a 3 digit number of type <b> short </b> which acts as the <b> Key </b> for storing the record.
	 * @param  bucketSize is a number of type <b> int </b> which specifies the bucket size of the hash table, whose Key has to be hashed. 
	 * @return an int which is the index or bucket location within the hash table data structure.
	 */
	public static int getBucketIndex(int id, int bucketSize){
		
		int hashCode = Integer.valueOf(id).hashCode();
		
	    return Math.abs(hashCode) % bucketSize;
	
	}

	
	/**
	 * For <b> Hash Table </b> implementation, this is a helper method used for rehashing the hash table data structure
	 * i.e. increasing the size of the hash table when the load factor is about to be reached.
	 * <p>
	 * This method increases the size of the bucket by doubling the bucket size to the next available prime number and 
	 * rehashes all the existing elements and stores it in the hash table data structure.
	 * <p>
	 * This method is implemented for the hash table data structure <b> hashTable </b> which is of type <b> EVM </b>.
	 */
	public static void rehash() {
		
		EVM[] temp;
		
		try {

            		totalHTBucket  = 2 * totalHTBucket;
            		totalHTBucket  = getNextPrime(totalHTBucket);
            		hashTableSize  = 0;
            
            		temp           = hashTableArr;        	
            		hashTableArr   = new EVM[totalHTBucket];
            
            		for (EVM headNode : temp){
            	
                		while (headNode != null){
                	
                			ADD(headNode.getVoterID(), headNode.getCandidateID());
                    
                    		headNode = headNode.getNext();
                
                		}
            
            		}
			
		}catch(Exception e) {
			
			e.printStackTrace();
			
		}		
		
	}
	
		
	/**
	 * For <b> Hash Table </b> implementation, this is a helper method used for rehashing the hash table data structure 
	 * i.e. increasing the size of the hash table when the load factor is about to be reached.
	 * <p> 
	 * This method increases the size of the bucket by doubling the bucket size to the next available prime number and 
	 * rehashes all the existing elements and stores it in the hash table data structure.
	 * <p>
	 * This method is implemented for the hash table data structure <b> voteCountHashTable </b> which is of type <b> VoteCount </b>.
	 */
	public static void rehashVoteCount() {
		
		VoteCount[] temp;
		
		try {

			totalVoteCountBucket  = 2 * totalVoteCountBucket;
			totalVoteCountBucket  = getNextPrime(totalVoteCountBucket);
			voteCountHTSize       = 0;
            
        	temp                  = voteCountHashTable;
        	voteCountHashTable    = new VoteCount[totalVoteCountBucket];
            
            for (VoteCount headNode : temp){
            	
            	while (headNode != null){
                	
            		calculateVoterCount(headNode.getCandidateID());
                    
                    headNode = headNode.getNext();
                
            	}
            
            }
            			
		}catch(Exception e) {
			
			e.printStackTrace();
			
		}
		
	}
	
	
	private static int getNextPrime(int number) {
		
		try {
			
			while(true) {
			
				boolean isPrime = true;
				
				number = number + 1;
				
				int sqrt = (int) Math.sqrt(number);
				
				for (int i=2; i<=sqrt; i++) {
				
					if (number % i == 0) {
						
						isPrime = false;
						
						break;
					
					}
				
				}
				
				if (isPrime) {

					return number;
				
				}
				
			}					
		
		}catch (Exception e) {
		
			e.printStackTrace();
		
		}
		
		return 0;
		
	}

}
