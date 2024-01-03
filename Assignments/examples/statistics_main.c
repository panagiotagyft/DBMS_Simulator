#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"
#include "sht_table.h"

#define RECORDS_NUM 300 // you can change it if you want
#define FILE_NAME "data.db"
#define INDEX_NAME "index.db"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }
  
int HashStatistics(char* filename){

	if( isSecondaryIndex(filename) == true ){ return HashStatistics_Secondary(filename); }
	if( isPrimaryIndex(filename) == true ){ return HashStatistics_Primary(filename); }
    
	return -1;		
}

int main(void){
	
	srand(12569874);
	// 	-- or -- use time(NULL) seed to initialize random number generator
	// 	srand(time(NULL));
	
	BF_Init(LRU);
	
// ---  Create data.db & index.db  ---

	// Αρχικοποιήσεις
	// create a file named data.db AKA FILE_NAME
	if( HT_CreateFile(FILE_NAME,10) == -1 ){
		printf("ERROR: HT_CreateFile() failed\n");
		exit(EXIT_FAILURE);
	}
	
	// create a file named index.db AKA INDEX_NAME
	if( SHT_CreateSecondaryIndex(INDEX_NAME, 10, FILE_NAME) == -1 ){
		printf("ERROR: SHT_CreateSecondaryIndex() failed\n");
		exit(EXIT_FAILURE);
	}

	printf("- Successfully created  a primary index file named %s and a secondary index file named %s! \n\n", FILE_NAME, INDEX_NAME);
	
	
// ---  Opens the data.db and index.db files -- and -- reads from the header blocks the information  ---

	HT_info* info;
 	if( (info = HT_OpenFile(FILE_NAME)) == NULL ){
		printf("ERROR: HT_OpenFile() failed\n");
		exit(EXIT_FAILURE);
	}
	
	SHT_info* index_info;
 	if( (index_info = SHT_OpenSecondaryIndex(INDEX_NAME)) == NULL ){
		printf("ERROR: SHT_OpenSecondaryIndex() failed\n");
		exit(EXIT_FAILURE);
	}
	
	printf("- The %s and %s files opened successfully!\n\n", FILE_NAME, INDEX_NAME);
	 

// ---  Select a random record  ---

	// Θα ψάξουμε στην συνέχεια το όνομα searchName
	Record record=randomRecord();
	char searchName[15];
	strcpy(searchName, record.name);


// ---  Insert Entries  ---

	// Κάνουμε εισαγωγή τυχαίων εγγραφών τόσο στο αρχείο κατακερματισμού τις οποίες προσθέτουμε και στο δευτερεύον ευρετήριο
	printf("Insert Entries\n\n");
	for (int id = 0; id < RECORDS_NUM; ++id) {
		record = randomRecord();
		// printRecord(record); 		// optional

		int block_id;
		if( (block_id = HT_InsertEntry(info, record)) == -1){
			printf("ERROR: HT_InsertEntry() failed\n");
			exit(EXIT_FAILURE);	
		}

		if( SHT_SecondaryInsertEntry(index_info, record, block_id) == -1){
			printf("ERROR: SHT_SecondaryInsertEntry() failed\n");
			exit(EXIT_FAILURE);	
		}		
	}
    
	printf("\nThe imports in both indexes completed successfully!\n\n");
// ---- optional prints!  ----
	// print_PrimaryIndex(info);
	//printf("\n\n");
	//print_SecondaryIndex(index_info);


// ---  Print all records with name -> searchName  ---

	// Τυπώνουμε όλες τις εγγραφές με όνομα searchName
	printf("\n\n-- RUN PrintAllEntries for name %s --\n\n",searchName);
	int blocks_num;
	if( (blocks_num =SHT_SecondaryGetAllEntries(info,index_info,searchName)) == -1 ){
			printf("ERROR: SHT_SecondaryGetAllEntries() failed\n");
			exit(EXIT_FAILURE);	
	}
	printf("\nThe number of blocks read is: %d\n", blocks_num);

	if( HashStatistics(FILE_NAME) == -1 ){
		printf("ERROR: HashStatistics() failed\n");
		exit(EXIT_FAILURE);		
	}
	if( HashStatistics(INDEX_NAME) == -1 ){
		printf("ERROR: HashStatistics() failed\n");
		exit(EXIT_FAILURE);		
	}

// ---  Close hash files  ---

	// Κλείνουμε το αρχείο κατακερματισμού και το δευτερεύον ευρετήριο
	
	if( SHT_CloseSecondaryIndex(index_info) == -1){
		printf("ERROR: SHT_CloseSecondaryIndex() failed\n");
		exit(EXIT_FAILURE);
	}
	
	if( HT_CloseFile(info) == -1){
		printf("ERROR: HT_CloseFile() failed\n");
		exit(EXIT_FAILURE);
	}
	
	CALL_OR_DIE(BF_Close());

	return (EXIT_SUCCESS);
}
