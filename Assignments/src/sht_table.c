#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "bf.h"
#include "sht_table.h"
#include "ht_table.h"
#include "record.h"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

// --- POS_BLOCKINFO & POS_BLOCKINFO_HT -->> position of the information in the block. --- !
#define POS_BLOCKINFO_HT  (BF_BLOCK_SIZE - sizeof(HT_block_info))		// primary index storage block !
#define POS_BLOCKINFO  (BF_BLOCK_SIZE - sizeof(SHT_block_info))			// secondary index storage block !

char* fileName_SHT;  // index.db

int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName){

	// Initialize variables 
	int fd_SHT;
	void* data;
	int** arr;

	// Initialize file-Structure
	CALL_OR_DIE(BF_CreateFile(sfileName));			// Create a new file on disk 
	CALL_OR_DIE(BF_OpenFile(sfileName, &fd_SHT));	// Open the file 

	// Initialize Block-Structure
	BF_Block *block;  				// A block
	BF_Block_Init(&block);			// Create a block without any info.

	// Allocate the first block with information about the HashTable
	CALL_OR_DIE(BF_AllocateBlock(fd_SHT, block));
	data = BF_Block_GetData(block);			// Get the 1st block's (for now empty) data

// ---  Update the info of first block (metadata). ---!

	SHT_info ShashTable_info;            
	ShashTable_info.fileDesc = fd_SHT;
	
	// Allocate space for 2d array 
	arr = (int**)malloc(buckets * sizeof(int*));
	for (int i = 1; i < buckets+1; i++){
		arr[i-1] = (int*)malloc(sizeof(int)*2);
		arr[i-1][0] = i;
		arr[i-1][1] = i; 	// overflow
	}
	
	ShashTable_info.InfoAboutHTable = block;
	ShashTable_info.HashTable = arr;
	ShashTable_info.recordSize = sizeof(Record_Secondary);
	ShashTable_info.numberOfRecordsInBlock = (BF_BLOCK_SIZE - sizeof(SHT_block_info))/ShashTable_info.recordSize;
	ShashTable_info.numOfBuckets = buckets;

	// Initialize Block-Structure
	BF_Block *blockBucket;
	BF_Block_Init(&blockBucket);

	// Copy hashTable_info into the 1st block's data
	memcpy(data, &ShashTable_info, sizeof(ShashTable_info));
	
	
// --- Creating buckets block --- !

	// For each bucket, allocate one block
	for (int i = 0; i < buckets; i++){
		
		// Allocate a block
		CALL_OR_DIE(BF_AllocateBlock(fd_SHT, blockBucket));
		void* data = BF_Block_GetData(blockBucket);			// Get the block's(for now empty) data

		// Write Meta-Data in each block
		SHT_block_info shtBlock;
		shtBlock.numberOfRecords = 0;             
   		shtBlock.nextBlockId = -1;                

		// Copy htBlock into data
		memcpy((data + POS_BLOCKINFO), &shtBlock, sizeof(shtBlock)); 
		
		// Closing-Functions
		BF_Block_SetDirty(blockBucket);
		CALL_OR_DIE(BF_UnpinBlock(blockBucket));
	}

	// Closing-Functions
	BF_Block_Destroy(&blockBucket);		 //Free mem
	
	// save changes and deallocate 
	BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
	BF_Block_Destroy(&block);		 //Free mem
	
	CALL_OR_DIE(BF_CloseFile(fd_SHT));
	
    return 0;
}

SHT_info* SHT_OpenSecondaryIndex(char *indexName){

	// Initialize variables 
	int fd1;
	int blocks_num;
	void* data;
	SHT_info* ShashTable_info;

	// Open the file 
	if( BF_OpenFile(indexName, &fd1) < 0){
    	printf("ERROR: BF_OpenFile() failed");
    	return NULL;
	} 

	fileName_SHT = (char*)malloc(strlen(indexName)+1);
	strcpy(fileName_SHT, indexName);
	
	// Initialize Block-Structure
	BF_Block *block;
	BF_Block_Init(&block);

	// Get Block 0 (MetaData)
	CALL_OR_DIE(BF_GetBlock(fd1, 0, block)); 
	data = BF_Block_GetData(block); 		// Get Block's Data
	ShashTable_info = data;

	// Calculate the number of blocks in file
	CALL_OR_DIE(BF_GetBlockCounter(fd1, &blocks_num));

	// Update its block
	ShashTable_info->fileDesc = fd1;
	ShashTable_info->InfoAboutHTable = block;

	// Closing-Functions
	BF_Block_SetDirty(block);
	
	
// ------------------- optional -----------------------

	printf("\n\n\t\t----------  Print initial %s file state  ----------\n\n\n", indexName);
	
	printf("(SHT) blocks_num: %d \n", blocks_num);
	
	print_Metadata_SecondaryIndex(ShashTable_info);

// -----------------------------------------------------
    return ShashTable_info;
}

int SHT_CloseSecondaryIndex(SHT_info* SHT_info){

	free(fileName_SHT);  // free name!
	
	// Free memory from malloc 
	for (int i=0; i<SHT_info->numOfBuckets; i++){
		free(SHT_info->HashTable[i]);
	}
	free(SHT_info->HashTable);

	// Unpin meta-data & deallocate memory
	CALL_OR_DIE(BF_UnpinBlock(SHT_info->InfoAboutHTable));
	BF_Block_Destroy(&(SHT_info->InfoAboutHTable));
	
	// Close file
	CALL_OR_DIE(BF_CloseFile(SHT_info->fileDesc));

    return 0;
}

int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id){

	// Initialize variables 
	int blockId;
	// Get Info From ht_info
 	int fd1 = sht_info->fileDesc;		// file desc
	int numberOfBuckets = sht_info->numOfBuckets; 		// number of buckets
	int** hashTable = sht_info->HashTable;				// hash table
	
	// call charHashFunction() to get bucket!
	int sHashNumber = charHashFunction(record.name, sht_info->numOfBuckets);
	int bucketId = hashTable[sHashNumber-1][1]; 		// get correct bucketId

	
	// Initialize Block-Structure
	BF_Block* block;
	BF_Block_Init(&block); 

	// Get data of hash-bucket
	CALL_OR_DIE(BF_GetBlock(sht_info->fileDesc, bucketId, block)); 
	void* data_current_examBlock = BF_Block_GetData(block);
	SHT_block_info *shtBlock_info = (data_current_examBlock + POS_BLOCKINFO);	// get the metadata of block
	
	// Make our new record-struct
	Record_Secondary recordSec;
    memcpy(recordSec.name, record.name, strlen(record.name)+1);
	recordSec.blockId = block_id;
	
	// If the space of current block is not sufficient, allocate a new block
	if(shtBlock_info->numberOfRecords == sht_info->numberOfRecordsInBlock){	
	
		// Initialize variables 
		BF_Block* new_block;
		BF_Block_Init(&new_block);
		
		// Allocate a new block 
	    CALL_OR_DIE( BF_AllocateBlock(sht_info->fileDesc, new_block));

		// Calculate the number of blocks
		int blocks_num;
		CALL_OR_DIE( BF_GetBlockCounter(sht_info->fileDesc, &blocks_num) );
		
		// Update the metadata of the block 
		shtBlock_info->nextBlockId = blocks_num - 1;
		
		// Update the metadata of the file (hashTable)
		hashTable[sHashNumber-1][1] = blocks_num - 1;
		sht_info->HashTable= hashTable;
		
	    // Closing-Functions
		BF_Block_SetDirty(sht_info->InfoAboutHTable);
		BF_Block_SetDirty(block);
		CALL_OR_DIE(BF_UnpinBlock(block));
		BF_Block_Destroy(&block);

		// Add the new record to the new block
		void* data_newBlock = BF_Block_GetData(new_block);		// Get Data of the new block
		memcpy(data_newBlock, &recordSec, sht_info->recordSize);
			
		// Initialize the metadata of the new block
		SHT_block_info info_newBlock;
		info_newBlock.numberOfRecords = 1;   
		info_newBlock.nextBlockId = -1;
		memcpy(data_newBlock + POS_BLOCKINFO, &info_newBlock, sizeof(SHT_block_info));
			
		// Closing-Functions
		BF_Block_SetDirty(sht_info->InfoAboutHTable);	
		BF_Block_SetDirty(new_block);
		CALL_OR_DIE(BF_UnpinBlock(new_block));
		BF_Block_Destroy(&new_block);

		blockId = blocks_num - 1;
		return blockId;
		
	}else{
		
		// Add the new record to the new block
		memcpy( (data_current_examBlock + ( (shtBlock_info->numberOfRecords) * (sht_info->recordSize) )), &recordSec, (sht_info->recordSize));

		// Update the metadata of the block 
		shtBlock_info->numberOfRecords++;
			
		// Closing-Functions
		BF_Block_SetDirty(sht_info->InfoAboutHTable);	
		BF_Block_SetDirty(block);
		CALL_OR_DIE(BF_UnpinBlock(block));
		BF_Block_Destroy(&block);
		
		blockId = bucketId;
		
		return blockId;
	}
}

int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* name){

	// Initialize variables 
	int returnValue, id, blocks_num;
	returnValue = 0;		// initialise the counter of the number of blocks

	// Find the number of blocks
	CALL_OR_DIE( BF_GetBlockCounter(sht_info->fileDesc, &blocks_num) );
	
	// If the number of blocks isn't one it means that there are blocks with records (the first block is only for metadata info without records),
	// -- then -->>  set id -> 1 
	// -- else -- return -1, witch means the record was not found!
	if(blocks_num == 1){ return -1; }

	id = 1; 
	
	// Initialize important variables 
	int* alreadyCheckedBlock = (int*)malloc(sizeof(int));		// Array of already visited blocks
	int allocateCount = 1;			// Number of elements in array

	// Initialize Block-Structure
	BF_Block* block;
	BF_Block_Init(&block); 
	
	// Get hash number of the key:name
	int hashBlockId = charHashFunction(name, sht_info->numOfBuckets);

	// Get the first block with the correct id.
	CALL_OR_DIE(BF_GetBlock(sht_info->fileDesc, hashBlockId, block)); // Get 1st block

	// Get the selected block's data
	void* data = BF_Block_GetData(block);			// Get block's data
	Record_Secondary* rec = data;
	SHT_block_info* shtBlock = (data + POS_BLOCKINFO);
	
	returnValue++;
	
	// Get the records of the bucket with the correct id
	for(int j=0; j<shtBlock->numberOfRecords; j++){ // For every stored record
	
		// If we found the correct name
		if(strcmp(name, rec[j].name) == 0){
			
			// Get the id of the block in data.db that the rest of the record is stored
			int id = rec[j].blockId;

			// If the id is not in the array
			if(!checkIfExists(alreadyCheckedBlock, id, allocateCount)){
				returnValue++;
				
				alreadyCheckedBlock[allocateCount-1] = id;
				allocateCount++;
				alreadyCheckedBlock = (int*)realloc(alreadyCheckedBlock, sizeof(int)*allocateCount);
				
			}else{ // If it is, get next record
				continue;
			}

			// Initialize Block-Structure
			BF_Block* block1;
			BF_Block_Init(&block1); 

			// Get data.db data
			CALL_OR_DIE(BF_GetBlock(ht_info->fileDesc, id, block1)); // Get 1st block
			void* data1 = BF_Block_GetData(block1);
			HT_block_info* htBlock1 = (data1 + POS_BLOCKINFO_HT);
			Record* rec1 = data1;

			// For every stored record
			for(int j=0; j<htBlock1->numberOfRecords; j++){

				// If we found the correct name, print it
				if(strcmp(name, rec1[j].name) == 0){
					printRecord(rec1[j]);
				}
			}

			// Closing-Functions
			CALL_OR_DIE(BF_UnpinBlock(block1));
			BF_Block_Destroy(&block1);
		}
	}

	// For each overflow-bucket of the initial block-bucket
	int bucketId = shtBlock->nextBlockId;
	CALL_OR_DIE(BF_UnpinBlock(block));
	BF_Block_Destroy(&block);

	printf("----------------- OverFlow BLocks -----------------\n");

	// Initialize Block-Structure
	BF_Block* bucket_block;
	BF_Block_Init(&bucket_block); // Init block

	// While we do have overflow-buckets
	while(bucketId != -1){
		
		returnValue++;
		
		// Get overflow-bucket data
		CALL_OR_DIE(BF_GetBlock(sht_info->fileDesc, bucketId, bucket_block));
		void* bucket_data = BF_Block_GetData(bucket_block);// Get block's data
		Record_Secondary* bucket_rec = bucket_data;
		SHT_block_info* bucket_htBlock = bucket_data + POS_BLOCKINFO;

		// Get records in the overflow-bucket
		for(int j=0; j<bucket_htBlock->numberOfRecords; j++){ // For every stored record

			// If we found the correct name
			if(strcmp(name, bucket_rec[j].name) == 0){

				// Get the id of the block in data.db that the rest of the record is stored
				int id = bucket_rec[j].blockId;

				// If the id is not in the array
				if(!checkIfExists(alreadyCheckedBlock, id, allocateCount)){
					returnValue++;
				
					alreadyCheckedBlock[allocateCount-1] = id;
					allocateCount++;
					alreadyCheckedBlock = (int*)realloc(alreadyCheckedBlock, sizeof(int)*allocateCount);
					
				}else{ // If it is, get next record
					continue;
				}
				
				// Initialize Block-Structure
				BF_Block* block1;
				BF_Block_Init(&block1); 

				// Get data.db data
				CALL_OR_DIE(BF_GetBlock(ht_info->fileDesc, id, block1)); // Get 1st block
				void* data1 = BF_Block_GetData(block1);
				HT_block_info* htBlock1 = (data1 + POS_BLOCKINFO_HT);
				Record* rec1 = data1;

				// For every stored record
				for(int j=0; j<htBlock1->numberOfRecords; j++){
					
					// If we found the correct name
					if(strcmp(name, rec1[j].name) == 0){
						printRecord(rec1[j]);
					}
				}

				// Closing-Functions
				CALL_OR_DIE(BF_UnpinBlock(block1));
				BF_Block_Destroy(&block1);
			}
		}
		
		// Get next overflow-bucket, if it exists.
		bucketId = bucket_htBlock->nextBlockId;

		// Closing-Functions
		CALL_OR_DIE(BF_UnpinBlock(bucket_block));
	}
	
	// Closing-Functions
	free(alreadyCheckedBlock);
	BF_Block_Destroy(&bucket_block);
	
	return returnValue;
}

// Our hash function for string
int charHashFunction(char* name, int numberOfBuckets){
	long sum = 0;
	for (int i = 0; i < strlen(name); i++) {
		sum += name[i] - 'A';
    }
	return (sum % numberOfBuckets) + 1;
}

// Checks if id is in the array.
bool checkIfExists(int* array, int id, int allocateCount){
	for(int i=0; i < allocateCount; i++){
		if(array[i] == id){
			return true;
		}
	}
	return false;
}

int HashStatistics_Secondary(char* filename){
	
	int fd;
	CALL_OR_DIE(BF_OpenFile(filename, &fd));		// Open the file 
	
	printf("\n-- Statistics for: %s file --\n\n", filename);
	
//  --- 1. Number of blocks in file. ---
	
	int blocks_num;
	CALL_OR_DIE(BF_GetBlockCounter(fd, &blocks_num));			// calculate the number of blocks 
	
	printf("\t1. Number of blocks in file %s are %d \n\n", filename, blocks_num);			// print the number of blocks
	
	// auxiliary block
	BF_Block *block;
	BF_Block_Init(&block);

	
	CALL_OR_DIE(BF_GetBlock(fd, 0, block));			// get the first block (metadata)
	void* metadata = BF_Block_GetData(block);         // get block's data
	SHT_info* shashTable_info = metadata;  
	
	// definition of important variables
	int min, mid, max, overflowBuckets_num, total_records, overflow_blocks;
	
	min = shashTable_info->numberOfRecordsInBlock;
	max = 0; 
	overflowBuckets_num = 0;
	total_records = 0;


// --- 4. The number of buckets that have overflow blocks. ---
		
	printf("\t4. The buckets that have overflow blocks:\n");

	// check one bucket at a time
	for(int i = 0; i < shashTable_info->numOfBuckets; i++){
		
		if( shashTable_info->HashTable[i][0] != shashTable_info->HashTable[i][1]){ overflowBuckets_num++; }
		
		CALL_OR_DIE(BF_GetBlock(fd, i+1, block));		// get the block that was initially set to correspond to the current bucket

		//  get block's data
		void* data = BF_Block_GetData(block); 
		SHT_block_info* block_info = data + POS_BLOCKINFO;
		
		// update 'max' and 'min' variables
		if(min > block_info->numberOfRecords){ min = block_info->numberOfRecords; }
		if(max < block_info->numberOfRecords){ max = block_info->numberOfRecords; }
		
		total_records += block_info->numberOfRecords;
		
		overflow_blocks = 0;
		
		// check overflow blocks if they exist
		while(block_info->nextBlockId != - 1){

			BF_Block* bucket_block;
			BF_Block_Init(&bucket_block); 	// Init block

			CALL_OR_DIE(BF_GetBlock(fd, block_info->nextBlockId, bucket_block));		// get overflow block
			
			//  get overflow-bucket data
			void* bucket_data = BF_Block_GetData(bucket_block); 
			block_info = bucket_data + POS_BLOCKINFO;
			
			overflow_blocks++;
			
			// update 'min' and 'max' variables
			if(min > block_info->numberOfRecords){ min = block_info->numberOfRecords; }
			if(max < block_info->numberOfRecords){ max = block_info->numberOfRecords; }
			
			total_records += block_info->numberOfRecords;
			
			CALL_OR_DIE(BF_UnpinBlock(bucket_block));
			BF_Block_Destroy(&bucket_block);
		} 
		
		if(overflow_blocks != 0){ printf("\t\tBucket %d. -->> there are [ %d ] overflow blocks\n", i+1, overflow_blocks);	}
		
		CALL_OR_DIE(BF_UnpinBlock(block));
		
	}
	
	printf("\t The number of buckets that have overflow blocks is %d.\n\n", overflowBuckets_num);
	

//  --- 2. The minimum, medium, and maximum number of records each bucket of a file has. ---
	
	printf("\t2. The minimum, medium, and maximum number of records each bucket of a file has\n");
	
	printf("\t\t min number of records: %2d, max number of records: %2d, mean number of records: %.1f \n",
			min, 
			max, 
			(double)total_records/(double)shashTable_info->numOfBuckets
	);
	
// --- 3. The average number of blocks each bucket has. ---

	double average_numberBlocks = (double)(blocks_num-1)/(double)shashTable_info->numOfBuckets;
	
	printf("\n\t3. The average number of blocks each bucket has is %.1f\n\n", average_numberBlocks);
	

	CALL_OR_DIE(BF_UnpinBlock(block));
	BF_Block_Destroy(&block);			//Free mem
	
	return 0;
	
}

// check if the examining string is the same as the secondary index name string
bool isSecondaryIndex(char* filename){
	
	if( strcmp(filename, fileName_SHT) == 0 ){ return true; }
	
	return false;	
}

void print_Metadata_SecondaryIndex(SHT_info* sht_info){

	// optional: print SHT_info (metadata)
	printf("\n\n--  optional: print SHT_info (metadata)  --\n\n");
	printf(" fileDesc: %d, \n\n InfoAboutHTable: %p, \n\n numOfBuckets: %ld, \n\n recordSize: %d, \n\n numberOfRecordsInBlock: %d \n\n",
			sht_info->fileDesc, 
			sht_info->InfoAboutHTable, 
			sht_info->numOfBuckets, 
			sht_info->recordSize,
			sht_info->numberOfRecordsInBlock
		);
	
	printf(" HashTable:\n");
	for (int i = 0; i < sht_info->numOfBuckets; i++){
		printf("\t\t: %d %d\n", 
				sht_info->HashTable[i][0],
				sht_info->HashTable[i][1]
		);
	}
	printf("\n\n------------------------------------------------\n\n");
	
	return;
}

int print_SecondaryIndex(SHT_info* sht_info){

	printf("\t\t---   SecondaryIndex   ---");
	
	// Initialize variables 
	int blocks_num, fd;
	
	// Find the number of blocks
	CALL_OR_DIE( BF_GetBlockCounter(sht_info->fileDesc, &blocks_num) );
	
	// If the number of blocks isn't one it means that there are blocks with records (the first block is only for metadata info without records),
	if(blocks_num == 1){ 
		printf("\n\nThe number of blocks is one it means that there are not blocks with records\n\n");
		return 0;
	}
	
	fd = sht_info->fileDesc;
	
	BF_Block *block;
	BF_Block_Init(&block);

	
	CALL_OR_DIE(BF_GetBlock(fd, 0, block));		       // get the first block (metadata)
	void* metadata = BF_Block_GetData(block);         // get block's data
	SHT_info* shashTable_info = metadata;               

	// check one bucket at a time
	for(int i = 0; i < shashTable_info->numOfBuckets; i++){
		
		printf("\n\n\n--------------------   Bucket %d.   --------------------", i+1);
	
		CALL_OR_DIE(BF_GetBlock(fd, i+1, block));		// get the block that was initially set to correspond to the current bucket

		printf("\n\n[ Block %d ]\n\n", i+1);

		//  get block's data
		void* data = BF_Block_GetData(block); 
		Record_Secondary* rec = data;
		SHT_block_info* block_info = data + POS_BLOCKINFO;

		for(int j = 0; j < block_info->numberOfRecords; j++){
			printf("(%s, Block %d)\n", rec[j].name, rec[j].blockId); 
		}
	
		CALL_OR_DIE(BF_UnpinBlock(block));
		
		// check overflow blocks if they exist
		for(;;){
			
			if(block_info->nextBlockId == - 1){ break;}
			
			CALL_OR_DIE(BF_GetBlock(fd, block_info->nextBlockId, block));		// get overflow block

			printf("\n\n[ Block %d ]\n\n", block_info->nextBlockId);
			
			//  get block's data
			data = BF_Block_GetData(block); 
			rec = data;
			block_info = data + POS_BLOCKINFO;
			
			for(int j = 0; j < block_info->numberOfRecords; j++){ 
				printf("(%s, Block %d)\n", rec[j].name, rec[j].blockId); 
			}
			
			CALL_OR_DIE(BF_UnpinBlock(block));
		} 
	}

	CALL_OR_DIE(BF_UnpinBlock(block));
	BF_Block_Destroy(&block);			//Free mem
	
	return 0;

}
