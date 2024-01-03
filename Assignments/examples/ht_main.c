#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>  //  time(NULL)
#include "bf.h"
#include "ht_table.h"

#define RECORDS_NUM 300 // you can change it if you want
#define FILE_NAME "data.db"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }


int main(void){
	
	CALL_OR_DIE(BF_Init(LRU));
	int numberOfBuckets = 10;

	// Create a new HashTable file on disk 
  	if( HT_CreateFile(FILE_NAME, numberOfBuckets) == -1 ){
		printf("ERROR: HT_CreateFile() failed\n");
		exit(EXIT_FAILURE);
	}
	printf("\n\n- Successfully created  a primary index file named %s! \n\n", FILE_NAME);

	// Create a new HashTable file on disk 
	HT_info* info; 
  	if( (info = HT_OpenFile(FILE_NAME)) == NULL ){
		printf("ERROR: HT_OpenFile() failed\n");
		exit(EXIT_FAILURE);
	}
	printf("- The %s file opened successfully!\n\n", FILE_NAME);
	

// ---  Insert Entries  ---

	Record record;
	
  	srand(12569874);
// 	-- or -- use time(NULL) seed to initialize random number generator
// 	srand(time(NULL));	
		 
	int r;
	printf("Insert Entries\n");
	for (int id = 0; id < RECORDS_NUM; ++id) {
		record = randomRecord();
		if( (r = HT_InsertEntry(info, record)) == -1){
			printf("ERROR: HT_InsertEntry() failed\n");
			exit(EXIT_FAILURE);	
		}
	}
	
	printf("\nThe imports in primary index completed successfully!\n\n");
	// print_PrimaryIndex(info);   // optional!

//---  Get Entries  ---
	
	printf("\n\n-- Run GetAllEntries --\n\n");
	
	int id = rand() % RECORDS_NUM;
	printf("Id of Random Record: %d\n", id);
	int blocks_num;
	if( (blocks_num = HT_GetAllEntries(info, id)) == -1){
		printf("ERROR: HP_GetAllEntries() failed\n");
		exit(EXIT_FAILURE);	
	}
	printf("The number of blocks read until the record is found is: %d\n", blocks_num);

// ---  Close file  ---

	// close the file and deallocate the memory
	if( HT_CloseFile(info) == -1){
		printf("ERROR: HT_CloseFile() failed\n");
		exit(EXIT_FAILURE);
	}
	CALL_OR_DIE(BF_Close());

	return (EXIT_SUCCESS);

}
