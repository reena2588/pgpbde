package com.upgrad.sparkstreaming.project;

import java.util.Scanner;

public class StockDataAnalysisClient {
	
	
	public static void main(String[] args) {
		
		/* The following IF-ELSE construct will check if 02 command-line arguments for inputStreamFolderLocation and outputStreamFolderLocation are passed while executing the program. 
		 * If not provided, the program will end with a message */
		
		if (args[0]==null || args[1]==null) {
			
			System.out.println("Please Run The Progam With 02 Command-line Argument(s) - Input and Output Locations");
			System.exit(0);
		
		}else {
			
			char ch;
			Scanner sc = new Scanner(System.in);
			
			try	{
						
				while(true) {
		
					System.out.println("Welcome To The Project On Stock Data Analysis Using Spark Streaming");
					System.out.println("===================================================================");
					System.out.println("");
					System.out.println("1. Simple Moving Average - Close Price ");
					System.out.println("2. Stock With Maximum Profit");
					System.out.println("3. Stocks With RSI");
					System.out.println("4. Stock With Maximum Volume");
					System.out.println("5. Exit");
					System.out.println("");
					
					System.out.print("Please Enter The Menu Number: ");
					System.out.print("");
					
					ch = (char) sc.nextInt();
					
					switch(ch) {
						
						case 1:{
			
							StockDataAnalysis.simpleMovingAvgClosePrice(args);
							break;
						
						}
						
						case 2:{
	
							StockDataAnalysis.stockWithMaxProfit(args);
							break;
							
						}
						
						case 3:{
						
							StockDataAnalysis.stocksWithRSI(args);
							break;
							
						}
	
						case 4:{
							
							StockDataAnalysis.stockWithMaximumVolume(args);
							break;
							
						}
						
						case 5:{
							
							System.exit(0);
							break;
							
						}
											
						default:{
													
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

}