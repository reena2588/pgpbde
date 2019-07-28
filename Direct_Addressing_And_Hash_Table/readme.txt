Assignment On Data Structures - Direct Addressing And Hash Table
================================================================


Problem Statement:
------------------

The output of an Electronic Voting Machine(EVM) is a text file consisting of (voter_id, candidate_id) pairs. Suppose that voter_id is a 6-digit integer and candidate_id is a 3-digit integer. 

You are provided a file containing the consolidated output of EVMs from a constituency. Note that the exact values of voter_id and candidate_id are not revealed.

Implement the following methods for both direct addressing and hash table techniques:

ADD: Takes a (voter_id, candidate_id) pair from the given file as input and stores it
FIND: Takes a voter_id as input and outputs the candidate_id for whom the vote was cast
COUNT: Takes a candidate_id as input and outputs the total number of votes received by him/her

Evaluate the worst-case running time of the above methods.


Solution:
---------

The problem statement is resolved using both Direct Addressing and Hash Table implementations.

The main class ElectionCountClient is a solution to the problem of Election Count using Direct Addressing and Hash Table implementation techniques.
 
It is a menu-based client which is used for operations like ADD(), FIND() and COUNT() for both implementations.

Note:

Before executing the program, please refer the configuration.properties file available within the src folder available within the project folder. 

This is a configuration file which hosts a few properties which are required to be initialized to appropriate values before executing the program.

For detailed documentation pertaining to program logic and worst case run time complexities for various operations, please refer the javadoc available within the doc folder available within the project folder.