#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h> // floor()
#include <stdbool.h>
#include "bf.h"
#include "ht_table.h"
#include "record.h"


#define CALL_OR_DIE(call)      \
{                        	   \
	BF_ErrorCode code = call; \
	if (code != BF_OK) {      \
		BF_PrintError(code);   \
		exit(code);            \
	}                          \
}

#define POS_BLOCKINFO  (BF_BLOCK_SIZE - sizeof(HT_block_info))		// POS_BLOCKINFO -->> position of the information in the block.

char* fileName_HT;

int HT_CreateFile(char *fileName, int buckets){
	// Initialize variables 
	int fd1;
	void* data;
	int** arr;

	// Initialize file-Structure
	CALL_OR_DIE(BF_CreateFile(fileName));		// Create a new file on disk 
	CALL_OR_DIE(BF_OpenFile(fileName, &fd1));	// Open the file 

	// Initialize Block-Structure
	BF_Block *block;  		// A block
	BF_Block_Init(&block);  // Create a block without any info.

	// Allocate the first block with information about the HashTable
	CALL_OR_DIE(BF_AllocateBlock(fd1, block));
	data = BF_Block_GetData(block); 	// Get the 1st block's(for now empty) data

	// Update the info of first block (metadata).
	HT_info hashTable_info;            
	hashTable_info.fileDesc = fd1;
	
	// Allocate space for 2d array 
	arr = (int**)malloc(buckets * sizeof(int*));
	for (int i = 1; i < buckets+1; i++){
		arr[i-1] = (int*)malloc(sizeof(int)*2);
		arr[i-1][0] = i;
		arr[i-1][1] = i; 	// overflow
	}
	hashTable_info.InfoAboutHTable = block;
	hashTable_info.HashTable = arr;
	hashTable_info.recordSize = sizeof(Record);
	hashTable_info.numberOfRecordsInBlock = floor((double)(BF_BLOCK_SIZE - sizeof(HT_block_info))/(double)sizeof(Record));
	hashTable_info.numOfBuckets = buckets;
	char* type = "Hash File";
	hashTable_info.fileType = (char*)malloc(strlen(type)+1);
	strcpy(hashTable_info.fileType, type);

	// Initialize Block-Structure
	BF_Block *blockBucket;
	BF_Block_Init(&blockBucket);
	
	// For each bucket, allocate 1 block
	for (int i = 0; i < buckets; i++){
		
		// Allocate a block
		CALL_OR_DIE(BF_AllocateBlock(fd1, blockBucket));
		void* data = BF_Block_GetData(blockBucket);		// Get the block's(for now empty) data

		// Write Meta-Data in each block
		HT_block_info htBlock;
		htBlock.numberOfRecords = 0;             
   		htBlock.nextBlockId = -1;                

		// Copy htBlock into data
		memcpy((data + POS_BLOCKINFO), &htBlock, sizeof(htBlock)); 
		
		// Closing-Functions
		BF_Block_SetDirty(blockBucket);
		CALL_OR_DIE(BF_UnpinBlock(blockBucket));
		
	}
	
	BF_Block_Destroy(&blockBucket);		 //Free mem
	
	// Copy hashTable_info into the 1st block's data
	memcpy(data, &hashTable_info, sizeof(hashTable_info));

	// Closing-Functions
	BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
	BF_Block_Destroy(&block);		 //Free mem
	
	CALL_OR_DIE(BF_CloseFile(fd1));
	
    return 0;
}

HT_info* HT_OpenFile(char *fileName){

	// Initialize variables 
	int fd1;
	int blocks_num;
	void* data;
	HT_info* hashTable_info;

	// Open the file 
	if( BF_OpenFile(fileName, &fd1) < 0){
    	printf("ERROR: BF_OpenFile() failed");
    	return NULL;
	} 
	
	fileName_HT = (char*)malloc(strlen(fileName)+1);
	strcpy(fileName_HT, fileName);
	
	// Initialize Block-Structure
	BF_Block *block;
	BF_Block_Init(&block);

	// Get Block 0 (MetaData)
	CALL_OR_DIE(BF_GetBlock(fd1, 0, block)); 
	data = BF_Block_GetData(block); // Get Block's Data
	hashTable_info = data;

	// Check if file is a HashTable file
	if(strcmp(hashTable_info->fileType, "Hash File")){
		if(BF_CloseFile(fd1) != 0){
			printf("ERROR: BF_CloseFile() failed");
			return NULL;
		}
		printf("File: %d is not a Hash File\n", hashTable_info->fileDesc);
		return NULL;
	}

	// Print Number Of Blocks in file
	CALL_OR_DIE(BF_GetBlockCounter(fd1, &blocks_num));

	// Update its block
	hashTable_info->fileDesc = fd1;
	hashTable_info->InfoAboutHTable = block;
	
	// Closing-Functions
	BF_Block_SetDirty(block);
	
// ------------------- optional -----------------------

	printf("\n\n\t\t----------  Print initial %s file state  ----------\n\n\n", fileName);
	
	printf("(HT) blocks_num: %d \n", blocks_num);
	
	print_Metadata_PrimaryIndex(hashTable_info);

// -----------------------------------------------------

    return hashTable_info;
}

int HT_CloseFile(HT_info* header_info){
	
	free(fileName_HT);  // free name!
	
	// Free memory from malloc 
	free(header_info->fileType);
	for (int i=0; i<header_info->numOfBuckets; i++){
		free(header_info->HashTable[i]);
	}
	free(header_info->HashTable);

	// Unpin meta-data & deallocate memory
	CALL_OR_DIE(BF_UnpinBlock(header_info->InfoAboutHTable));
	BF_Block_Destroy(&(header_info->InfoAboutHTable));
	
	// Close file
	CALL_OR_DIE(BF_CloseFile(header_info->fileDesc));

    return 0;
}

int HT_InsertEntry(HT_info* ht_info, Record record){
	int blockId;
	
	// Get Info From ht_info
 	int fd1 = ht_info->fileDesc; // file desc
	int numberOfBuckets = ht_info->numOfBuckets; // number of buckets
	int** hashTable = ht_info->HashTable;			// has table
	int hashNumber = hashFunction(record.id, numberOfBuckets);
	int bucketId = hashTable[hashNumber-1][1]; // get correct bucket

	// Get data of hash-bucket
	BF_Block* block;
	BF_Block_Init(&block); // Init block
	CALL_OR_DIE(BF_GetBlock(ht_info->fileDesc, bucketId, block)); 
	void* data_current_examBlock = BF_Block_GetData(block);
	
	// get the metadata of block
	HT_block_info *htBlock_info = (data_current_examBlock + POS_BLOCKINFO);
	
	// If the space of current block is not sufficient, allocate a new block
	if(htBlock_info->numberOfRecords == ht_info->numberOfRecordsInBlock){	
		
		// Initialize variables 
		BF_Block* new_block;
		BF_Block_Init(&new_block);
		
		// Allocate a new block 
	    CALL_OR_DIE( BF_AllocateBlock(ht_info->fileDesc, new_block));

		// calculate the number of blocks
		int blocks_num;
		CALL_OR_DIE( BF_GetBlockCounter(ht_info->fileDesc, &blocks_num) );
		
		// update metadata in the block 
		htBlock_info->nextBlockId = blocks_num - 1;
		
		// update metadata (update hashTable)
		hashTable[hashNumber-1][1] = blocks_num - 1;
		ht_info->HashTable= hashTable;
		
	    // Closing-Functions
		BF_Block_SetDirty(ht_info->InfoAboutHTable);
		BF_Block_SetDirty(block);
		CALL_OR_DIE(BF_UnpinBlock(block));
		BF_Block_Destroy(&block);
			
		void* data_newBlock = BF_Block_GetData(new_block);

		// add the new record to the new block
		memcpy(data_newBlock, &record, ht_info->recordSize);
			
		// initialize the metadata of new block
		HT_block_info info_newBlock;
		info_newBlock.numberOfRecords = 1;   
		info_newBlock.nextBlockId = -1;
		
		memcpy(data_newBlock + POS_BLOCKINFO, &info_newBlock, sizeof(HT_block_info));
			
		blockId = blocks_num - 1;
		
		// Closing-Functions
		BF_Block_SetDirty(ht_info->InfoAboutHTable);	
		BF_Block_SetDirty(new_block);
		CALL_OR_DIE(BF_UnpinBlock(new_block));
		BF_Block_Destroy(&new_block);
		
		return blockId;
		
	}
	else{
		
		memcpy( (data_current_examBlock + ( (htBlock_info->numberOfRecords) * (ht_info->recordSize) )), &record, (ht_info->recordSize));
	
		// update metadata of the block 0
		htBlock_info->numberOfRecords++;
		
		// calculate the number of blocks
		int blocks_num;
		CALL_OR_DIE( BF_GetBlockCounter(ht_info->fileDesc, &blocks_num) );
		
		blockId = bucketId;
		
		// Closing-Functions
		BF_Block_SetDirty(ht_info->InfoAboutHTable);	
		BF_Block_SetDirty(block);
		CALL_OR_DIE(BF_UnpinBlock(block));
		BF_Block_Destroy(&block);
	
		return blockId;
	}
	

    return -1;
}

int HT_GetAllEntries(HT_info* ht_info, int correctId){
	
	// Initialize variables 
	int numBlocks_read, id, blocks_num;
	numBlocks_read = 0;		// initialise the counter of the number of records read 
	
	// Find the number of blocks
	CALL_OR_DIE( BF_GetBlockCounter(ht_info->fileDesc, &blocks_num) );
	
	// If the number of blocks isn't one it means that there are blocks with records (the first block is only for metadata info without records),
	// -- then -->>  set id -> 1 
	// -- else -- return -1, witch means the record was not found!
	if(blocks_num == 1){ return -1; }
	
	id = 1;

	// Initialize Block-Structure
	BF_Block* block;
	BF_Block_Init(&block); 
	
	// Get hash number of the key:correctId
	int hashBlockId = hashFunction(correctId, ht_info->numOfBuckets);
	printf("hashBlockId: %d\n", hashBlockId);

	// Get the first block with the correct id.
	CALL_OR_DIE(BF_GetBlock(ht_info->fileDesc, hashBlockId, block)); // Get 1st block

	// Get the selected block's data
	void* data = BF_Block_GetData(block);// Get block's data
	Record* rec = data;
	HT_block_info* htBlock = (data + POS_BLOCKINFO);
	
	// Get the records of the bucket with the correct id
	numBlocks_read++;
	for(int j=0; j<htBlock->numberOfRecords; j++){
		if(correctId == rec[j].id){
			printRecord(rec[j]);

			// Closing-Functions
			CALL_OR_DIE(BF_UnpinBlock(block));
			BF_Block_Destroy(&block);
			return numBlocks_read;
		}
	}

	// For each overflow-bucket of the initial block-bucket
	int bucketId = htBlock->nextBlockId;
	CALL_OR_DIE(BF_UnpinBlock(block));
	BF_Block_Destroy(&block);
	
	// Initialize Block-Structure
	BF_Block* bucket_block;
	BF_Block_Init(&bucket_block); // Init block

	// While we do have overflow-buckets
	while(bucketId != -1){
		numBlocks_read++;

		// Get overflow-bucket data
		CALL_OR_DIE(BF_GetBlock(ht_info->fileDesc, bucketId, bucket_block));
		void* bucket_data = BF_Block_GetData(bucket_block);// Get block's data
		Record* bucket_rec = bucket_data;
		HT_block_info* bucket_htBlock = bucket_data + POS_BLOCKINFO;

		// Get records in the overflow-bucket
		for(int j=0; j<bucket_htBlock->numberOfRecords; j++){
			
			if(correctId == bucket_rec[j].id){
				printRecord(bucket_rec[j]);

				// Closing-Functions
				CALL_OR_DIE(BF_UnpinBlock(bucket_block));
				BF_Block_Destroy(&bucket_block);
				return numBlocks_read;
			}
		}
		
		// Get next overflow-bucket, if it exists.
		bucketId = bucket_htBlock->nextBlockId;

		// Closing-Functions
		CALL_OR_DIE(BF_UnpinBlock(bucket_block));
	}

	// Closing-Functions
	BF_Block_Destroy(&bucket_block);

    return -1;
}

// Our hash function
int hashFunction(int number, int numberOfBuckets){
	return (number % numberOfBuckets) + 1;
}

// Ststistics function
int HashStatistics_Primary(char* filename){
	
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
	HT_info* hashTable_info = metadata;  
	
	// definition of important variables
	int min, mid, max, overflowBuckets_num, total_records, overflow_blocks;
	
	min = hashTable_info->numberOfRecordsInBlock;
	max = 0; 
	overflowBuckets_num = 0;
	total_records = 0;


// --- 4. The number of buckets that have overflow blocks. ---
		
	printf("\t4. The buckets that have overflow blocks:\n");

	// check one bucket at a time
	for(int i = 0; i < hashTable_info->numOfBuckets; i++){
		
		if( hashTable_info->HashTable[i][0] != hashTable_info->HashTable[i][1]){ overflowBuckets_num++; }
		
		CALL_OR_DIE(BF_GetBlock(fd, i+1, block));		// get the block that was initially set to correspond to the current bucket

		//  get block's data
		void* data = BF_Block_GetData(block); 
		HT_block_info* block_info = data + POS_BLOCKINFO;
		
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
			(double)total_records/(double)hashTable_info->numOfBuckets
	);
	
// --- 3. The average number of blocks each bucket has. ---

	double average_numberBlocks = (double)(blocks_num-1)/(double)hashTable_info->numOfBuckets;
	
	printf("\n\t3. The average number of blocks each bucket has is %.1f\n\n", average_numberBlocks);
	

	CALL_OR_DIE(BF_UnpinBlock(block));
	BF_Block_Destroy(&block);			//Free mem
	
	return 0;
	
}

// check if the examining string is the same as the primary index name string
bool isPrimaryIndex(char* filename){
	
	if( strcmp(filename, fileName_HT) == 0 ){ return true; }
	
	return false;	
}

void print_Metadata_PrimaryIndex(HT_info* ht_info){

	// optional: print HT_info (metadata)
	printf("\n\n--  optional: print HT_info (metadata)  --\n\n");
	printf(" fileDesc: %d, \n\n InfoAboutHTable: %p, \n\n numOfBuckets: %ld, \n\n recordSize: %d, \n\n numberOfRecordsInBlock: %d \n\n",
			ht_info->fileDesc, 
			ht_info->InfoAboutHTable, 
			ht_info->numOfBuckets, 
			ht_info->recordSize,
			ht_info->numberOfRecordsInBlock
		);
	
	printf(" HashTable:\n");
	for (int i = 0; i < ht_info->numOfBuckets; i++){
		printf("\t\t: %d %d\n", 
				ht_info->HashTable[i][0],
				ht_info->HashTable[i][1]
		);
	}
	printf("\n\n------------------------------------------------\n\n");
	
	return;
}

int print_PrimaryIndex(HT_info* ht_info){

	printf("\t\t---   PrimaryIndex   ---");
	
	// Initialize variables 
	int blocks_num, fd;
	
	// Find the number of blocks
	CALL_OR_DIE( BF_GetBlockCounter(ht_info->fileDesc, &blocks_num) );
	
	// If the number of blocks isn't one it means that there are blocks with records (the first block is only for metadata info without records),
	if(blocks_num == 1){ 
		printf("\n\nThe number of blocks is one it means that there are not blocks with records\n\n");
		return 0;
	}
	
	fd = ht_info->fileDesc;
	
	BF_Block *block;
	BF_Block_Init(&block);

	
	CALL_OR_DIE(BF_GetBlock(fd, 0, block));		      // get the first block (metadata)
	void* metadata = BF_Block_GetData(block);         // get block's data
	HT_info* hashTable_info = metadata;               

	// check one bucket at a time
	for(int i = 0; i < hashTable_info->numOfBuckets; i++){
		
		printf("\n\n\n--------------------   Bucket %d.   --------------------", i+1);
	
		CALL_OR_DIE(BF_GetBlock(fd, i+1, block));		// get the block that was initially set to correspond to the current bucket

		printf("\n\n[ Block %d ]\n\n", i+1);

		//  get block's data
		void* data = BF_Block_GetData(block); 
		Record* rec = data;
		HT_block_info* block_info = data + POS_BLOCKINFO;

		for(int j = 0; j < block_info->numberOfRecords; j++){ printRecord(rec[j]); }
	
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
			
			for(int j = 0; j < block_info->numberOfRecords; j++){ printRecord(rec[j]); }
			
			CALL_OR_DIE(BF_UnpinBlock(block));
		} 
	}

	CALL_OR_DIE(BF_UnpinBlock(block));
	BF_Block_Destroy(&block);			//Free mem
	
	return 0;

}

