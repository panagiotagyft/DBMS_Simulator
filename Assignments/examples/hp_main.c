#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>  //  time(NULL)
#include "bf.h"
#include "hp_file.h"

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
	
  	CALL_OR_DIE(BF_Init(LRU));		// initializing BF level & set replacement policy Block
	
	// create a file named data.db AKA FILE_NAME
	if( HP_CreateFile(FILE_NAME) == -1 ){
		printf("ERROR: HP_CreateFile() failed\n");
		exit(EXIT_FAILURE);
	}
	printf("\n\n- Successfully created  a heap file named %s! \n\n", FILE_NAME);

  	// opens the data.db file -- and -- reads from the header block the information relating to the heap file
	HP_info* info;
	if( (info = HP_OpenFile(FILE_NAME)) == NULL ){
		printf("ERROR: HP_OpenFile() failed\n");
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
    	// create a record
		record = randomRecord();
		
		// insert record in heap file
    	if( (r = HP_InsertEntry(info, record)) == -1){
			printf("ERROR: HP_InsertEntry() failed\n");
			exit(EXIT_FAILURE);	
		}
		//printf("record entered in blockId:%d\n",r);  // optional
    }

	printf("\nThe imports in heap file completed successfully!\n\n");

	//print_HeapFile(info);   // optional!

//---  Get Entries  ---

  	printf("RUN PrintAllEntries\n");
  	
  	// get a random record
  	int id = rand() % RECORDS_NUM; 	
  	
  	// search for the record.id
  	printf("\nSearching for: %d\n",id);
  	int blocks_num;
    if( (blocks_num = HP_GetAllEntries(info, id)) == -1){
		printf("ERROR: HP_GetAllEntries() failed\n");
		exit(EXIT_FAILURE);	
	}
	printf("The number of blocks read until the record is found is: %d\n", blocks_num);

// ---  Close file  ---

    // close the file and deallocate the memory
  	if( HP_CloseFile(info) == -1){
		printf("ERROR: HP_GetAllEntries() failed\n");
		exit(EXIT_FAILURE);
	}
  	
	CALL_OR_DIE(BF_Close());
	
	exit(EXIT_SUCCESS);
}
