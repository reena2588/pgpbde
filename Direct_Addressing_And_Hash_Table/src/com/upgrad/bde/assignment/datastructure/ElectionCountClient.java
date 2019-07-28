package com.upgrad.bde.assignment.datastructure;


import java.io.*;
import java.util.*;


/**
 * The <b> ElectionCountClient </b> class is the main class of the program. 
 * <p> 
 * It provides solution to the problem of Election Count using <b> Direct Addressing </b> and <b> Hash Table </b> implementation techniques.
 * <p>
 * It is a menu-based client which is used for operations like <b> ADD() </b>, <b> FIND() </b> and <b> COUNT() </b> for both implementations. 
 */

public class ElectionCountClient {		
	
	
	private static final String CONFIG_FILE             = "configuration.properties";
	private static final String INPUT_FILE_NAME_KEY     = "input.file.name";
	private static final String INPUT_FILE_LOCATION_KEY = "input.file.location";
	private static final String TAB_CHARACTER           = "\t";
	
	
	private static String inputfileNameValue;
	private static String inputfileLocationValue;
	private static String MSG1;
	private static String MSG2;
	private static String MSG3;
	private static String MSG4;
	private static String MSG5;
	private static String MSG6;
	private static String MSG7;
	private static String MSG8;
	private static String MSG9;
	private static String MSG10;
	private static String MSG11;
	private static String MSG12;
	private static boolean isInputLoaded;
		
	
	private static void loadConfigurationProperties() throws IOException {	
		
		
		Properties properties = new Properties();
		
		InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_FILE);
		 
		try{
		
			properties.load(inputStream);
			
			inputfileNameValue     = properties.getProperty(INPUT_FILE_NAME_KEY);
			inputfileLocationValue = properties.getProperty(INPUT_FILE_LOCATION_KEY);
			
			MSG1 = properties.getProperty("MSG_1");
			MSG2 = properties.getProperty("MSG_2");
			MSG3 = properties.getProperty("MSG_3");
			MSG4 = properties.getProperty("MSG_4");
			MSG5 = properties.getProperty("MSG_5");
			MSG6 = properties.getProperty("MSG_6");
			MSG7 = properties.getProperty("MSG_7");
			MSG8 = properties.getProperty("MSG_8");
			MSG9 = properties.getProperty("MSG_9");
			MSG10 = properties.getProperty("MSG_10");
			MSG11 = properties.getProperty("MSG_11");
			MSG12 = properties.getProperty("MSG_12");
			
		}catch(IOException e){
			
			e.printStackTrace();
		
		}finally{
			
			inputStream.close();
	
		}
		
	}
	
	
	private static void loadInputDataFromFile() throws IOException {
		
		BufferedReader br = null;
		
		try {
			
			String line      = null;
			String [] record = null;
			int voterId;
			short candidateId;
			
			br = new BufferedReader(new FileReader(inputfileLocationValue+inputfileNameValue));
			
			while((line = br.readLine()) != null) {
	            
				record = line.split(TAB_CHARACTER);	
				
				voterId     = Integer.parseInt(record[0].trim());
				candidateId = Short.parseShort(record[1].trim());
						
				DirectAddressing.ADD(voterId, candidateId);
				
				DirectAddressing.calculateVoterCount(candidateId);
				
				HashTable.ADD(voterId, candidateId);
				
				HashTable.calculateVoterCount(candidateId);
				
	       }
			
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}finally {
						
			if (br!=null) {
				
				try {
					
					br.close();	
				
				}catch(Exception e) {
					
					e.printStackTrace();
					
				}
			}
			
		}
		
	}
	
	
	private static boolean isInputDataLoaded() {
		
		try {
			
			if (DirectAddressing.getDirectAddressingArr().length>0 && HashTable.gethashTableArr().length>0) {
				
				return true;
			
			}
			
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}
		
		return false;
	
	}

		
	private ElectionCountClient() {
	
		// Not Defined...
	
	}
	
	private static void generalLoadInputData() {
		
		try {
		
			if (isInputLoaded) {
				
				System.out.println(MSG1+MSG4);
				
			}else {
			
				loadInputDataFromFile();
	
			}
									
			isInputLoaded = isInputDataLoaded();
			
			if (!isInputLoaded) {
				
				System.out.println(MSG1+MSG5);
				
			}
		
		}catch(Exception e) {
		
			e.printStackTrace();
		
		}
		
	}
	
	private static short getCandidateId(Scanner sc) {
		
		try {

			return (short) sc.nextInt();
		
		}catch(Exception e) {
			
			System.out.println(MSG12);
			
		}
		return 0;
		
	}
	
	private static int getVoterId(Scanner sc) {
		
		try {
			
			return sc.nextInt();
		
		}catch (Exception e) {
		
			System.out.println(MSG11);
		
		}
		
		return 0;
	
	}
	
	private static void directaddressingFindCandidate(Scanner sc) {
		
		try {
			
			if (isInputLoaded) {
				
				System.out.print(MSG2+MSG7);
				
				int voterId = getVoterId(sc);
				
				short candidateId = DirectAddressing.FIND(voterId);
			
				if (candidateId == 0) {
					
					System.out.println("Voter With ID "+voterId+" Does Not Exists.");
					
				}else {
				
					System.out.println("For Voter ID "+voterId+", The Candidate ID Is "+candidateId);
					
				}
			
			}else {
				
				System.out.println(MSG2+MSG6);
			
			}

			
		}catch(Exception e) {
			
			e.printStackTrace();
			
		}
		
	}
	
	private static void directaddressingCountVotes(Scanner sc) {
		
		try {
			
			if (isInputLoaded) {
				
				System.out.print(MSG2+MSG8);
				
				short candidateId = getCandidateId(sc);
								
				int voterCount    = DirectAddressing.COUNT(candidateId);
				
				if (voterCount == 0) {
					
					System.out.println("Candidate With ID "+candidateId+" Does Not Exists.");
					
				}else {
				
					System.out.println("For Candidate ID "+candidateId+", Total Number Of Vote(s) "+voterCount);
					
				}				
			
			}else {
				
				System.out.println(MSG2+MSG6);
			
			}
			
		}catch(Exception e) {
			
			e.printStackTrace();
		}
		
	}
	
	private static void hashtableFindCandidate(Scanner sc) {
		
		try {
			
			if (isInputLoaded) {
				
				System.out.print(MSG3+MSG7);
				
				int voterId = getVoterId(sc);
					
				short candidateId = HashTable.FIND(voterId);
			
				if (candidateId == 0) {
				
					System.out.println("Voter With ID "+voterId+" Does Not Exists.");
					
				}else {
				
					System.out.println("For Voter ID "+voterId+", The Candidate ID Is "+candidateId);
					
				}							
			
			}else {
				
				System.out.println(MSG3+MSG6);
				
			}
			
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}
	}
	
	
	private static void hashtableCountVotes(Scanner sc) {
		
		try {
			 
			if (isInputLoaded) {
				
				System.out.print(MSG3+MSG8);
				
				short candidateId = getCandidateId(sc);				
								
				int voterCount    = HashTable.COUNT(candidateId);
				
				System.out.println("For Candidate ID "+candidateId+", Total Number Of Vote(s) "+voterCount);
			
			}else {
				
				System.out.println(MSG3+MSG6);
			
			}
			
		}catch(Exception e) {
			
			e.printStackTrace();
			
		}
		
	}
	
	
	public static void main(String[] args) {
		
		char ch;
		Scanner sc = new Scanner(System.in);
		
		try	{
		
			loadConfigurationProperties();
			
			while(true) {
	
				System.out.println("Welcome To The Assignment On Data Structures - Direct Addressing And Hash Table");
				System.out.println("===============================================================================");
				System.out.println("");
				System.out.println("1. General:              Load The Input Data From File                    ADD()");
				System.out.println("2. Direct Addressing:    Find A Candidate                                FIND()");
				System.out.println("3. Direct Addressing:    Count Votes For A Candidate                    COUNT()");
				System.out.println("4. Hash Table:           Find A Candidate                                FIND()");
				System.out.println("5. Hash Table:           Count Votes For A Candidate                    COUNT()");			
				System.out.println("6. Exit");
				System.out.println("");
				
				System.out.print("Please Enter The Menu Number: ");
				System.out.print("");
				
				ch = (char) sc.nextInt();
				
				switch(ch) {
					
					case 1:{
		
						generalLoadInputData();		
						
						break;
					
					}
					
					case 2:{

						directaddressingFindCandidate(sc);						
					
						break;
						
					}
					
					case 3:{
					
						directaddressingCountVotes(sc);	
						
						break;
						
					}

					case 4:{
						
						hashtableFindCandidate(sc);
						
						break;
						
					}
					
					case 5:{
						
						hashtableCountVotes(sc);
						
						break;

					}

					case 6:{
					
						System.out.println(MSG9);
						
						System.exit(0);
						
						break;
					
					}
					
					default:{
						
						System.out.println(MSG1+MSG10);
						
						break;
					
					}
					
				}
				
			}
				
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}finally {
			
			sc.close();
		
		}
		
	}
	
}