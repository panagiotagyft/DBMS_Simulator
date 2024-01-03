#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "bf.h"
#include "hp_file.h"
#include "record.h"

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

#define POS_BLOCKINFO  (BF_BLOCK_SIZE - sizeof(HP_block_info))		// POS_BLOCKINFO -->> position of the information in the block.


int HP_CreateFile(char *fileName){
	int fd;
	void* data;
	
	// Create a new file on disk 
	CALL_BF( BF_CreateFile(fileName) );
	
	// Open the file (data.db) to get the file descriptor
	CALL_BF( BF_OpenFile(fileName, &fd) );
	
	// Initialize and bind a block from cache
	BF_Block* block;
	BF_Block_Init(&block);  
	
	// Allocate the first block 
	// The first block contains all the important information (metadata) about the heap file
	CALL_BF( BF_AllocateBlock(fd, block) );
	
	// data (-->> index) points to beginning of first block   
	data = BF_Block_GetData(block);
	
	HP_info* heapFile_info = data;
	

// ----------     Initializing metadata     ----------
	
	// File Descriptor
	heapFile_info->fileDesc = fd;
	
	// File Type --->> Heap File
	char type[] = "Heap File";
	heapFile_info->fileType = (char*)malloc(strlen(type)+1);
	strcpy(heapFile_info->fileType, type);

	// The size of Record
	heapFile_info->recordSize = sizeof(Record);
    
    // The maximum number of records per block
	heapFile_info->maxRecords_per_Block =( (BF_BLOCK_SIZE - sizeof(HP_block_info)) / sizeof(Record) );
	
	// The last block id
	heapFile_info->lastBlock_id = 0;
	
	// The header block contains the metadata
	heapFile_info->header_block = block;

	BF_Block_SetDirty(block);
	CALL_BF( BF_UnpinBlock(block) );
	BF_Block_Destroy(&block);
	
	CALL_BF( BF_CloseFile(fd) );

    return 0;
}

HP_info* HP_OpenFile(char *fileName){
	int fd;
	
	// Open the file (data.db) to get the file descriptor
    if( BF_OpenFile(fileName, &fd) < 0){
    	printf("ERROR: BF_OpenFile() failed");
    	return NULL;
	}

	BF_Block* block;
	BF_Block_Init(&block);
	
	// Get the first block
	if( BF_GetBlock(fd, 0, block) != 0 ){
		printf("ERROR: BF_GetBlock() failed");
		return NULL;
	}

	void* data = BF_Block_GetData(block);
	

	HP_info *information_head = data;
	
	// check if the file to be examined is a heap file
	if(strcmp(information_head->fileType, "Heap File") > 0){
		
		if(BF_CloseFile(fd) != 0){
			printf("ERROR: BF_CloseFile() failed");
			return NULL;
		}
		
		printf("ERROR: strcmp() failed");
		return NULL;
	}
	// update metada (header block)
	information_head->fileDesc = fd;
	information_head->header_block = block;
	
	BF_Block_SetDirty(block);
	
// ------------------- optional -----------------------

	printf("\n\n\t\t----------  Print initial %s file state  ----------\n", fileName);
	
	print_Metadata_HeapFile(information_head);

// -----------------------------------------------------
	
    return information_head;
}


int HP_CloseFile( HP_info* hp_info ){
	
    free(hp_info->fileType);	// Free memory from malloc 

	// Unpin meta-data & deallocate memory
    CALL_BF(BF_UnpinBlock(hp_info->header_block));
    BF_Block_Destroy(&hp_info->header_block);

	CALL_BF(BF_CloseFile(hp_info->fileDesc)); 	// Close file
	
    return 0;
}

int HP_InsertEntry(HP_info* hp_info, Record record){
	int blocks_num, blockId;
	bool  allocate_new_block = false;
	
	// calculate the number of blocks
	CALL_BF( BF_GetBlockCounter(hp_info->fileDesc, &blocks_num) );
	
	// --------------------     There's a block to examine (last block)    --------------------
	if(blocks_num > 1){ 

		BF_Block* block;
		BF_Block_Init(&block); 
     
		CALL_BF(BF_GetBlock(hp_info->fileDesc, hp_info->lastBlock_id, block));
		void* data_currentBlock = BF_Block_GetData(block);
        
		// get the metadata of block
		HP_block_info *info = (data_currentBlock + POS_BLOCKINFO);
		
		// If the space of current block is not sufficient, allocate a new block
		if(info->numberOfRecords == hp_info->maxRecords_per_Block){ 
	
			// release the previous block
			CALL_BF( BF_UnpinBlock(block) );
			BF_Block_Destroy(&block);
			
			// set true to the allocate flag to enable the allocate condition
			allocate_new_block = true;

		}
		else{
			// If the space of current block is not sufficient  -- then -->>  add the new record.
			memcpy( (data_currentBlock + ( (info->numberOfRecords) * (hp_info->recordSize) )), &record, (hp_info->recordSize));
	
			// update metadata of the block
			info->numberOfRecords++;
			blockId = hp_info->lastBlock_id;
			
			BF_Block_SetDirty(block);
			CALL_BF( BF_UnpinBlock(block) );
			BF_Block_Destroy(&block);
			
			return blockId;
		}		
	}
	
    // -----------------   zero blocks   - or -   allocate a new block    -----------------
	if( blocks_num == 1 || allocate_new_block == true){

		BF_Block* new_block;
		BF_Block_Init(&new_block);
			
		// allocate a new block 
	    CALL_BF( BF_AllocateBlock(hp_info->fileDesc, new_block) );
			
		void* data_newBlock = BF_Block_GetData(new_block);

		// add the new record to the new block
		memcpy(data_newBlock, &record, hp_info->recordSize);
			
		// initialize the metadata of new block
		HP_block_info info_newBlock;
		info_newBlock.numberOfRecords = 1;   
	
		memcpy((data_newBlock + POS_BLOCKINFO), &info_newBlock, sizeof(HP_block_info));
		
		// update metadata of header	
		(hp_info->lastBlock_id)++;		
		blockId = hp_info->lastBlock_id;
		
		BF_Block_SetDirty(hp_info->header_block);
			
		BF_Block_SetDirty(new_block);
		CALL_BF(BF_UnpinBlock(new_block));
		BF_Block_Destroy(&new_block);
		
		return blockId;
		
	}
	
    return -1;
}

int HP_GetAllEntries(HP_info* hp_info, int value){
	int numBlocks_read, id, blocks_num;
	
	numBlocks_read = 0;		// initialise the counter of the number of blocks read 
	
	// calculate the number of blocks
	CALL_BF( BF_GetBlockCounter(hp_info->fileDesc, &blocks_num) );
	
	// If the number of blocks isn't one it means that there are blocks with records (the first block is only for metadata info without records),
	// -- then -->>  set id -> 1 
	// -- else -- return -1, witch means the record was not found!
	if(blocks_num == 1){  return -1; }	
	
	id = 1;
	
	// auxiliary block
	BF_Block* block;
	BF_Block_Init(&block); 
	
	// while the index is smaller than the number of blocks search for the record with id == value
	while(id < blocks_num){
		numBlocks_read++;

		CALL_BF(BF_GetBlock(hp_info->fileDesc, id, block));    // get block from heap file
		
		void* data = BF_Block_GetData(block);    

		Record* rec = data;

		HP_block_info* block_info = data + POS_BLOCKINFO;
		
		int i=0;
		// check one record at a time! 
		// if the requested record is found print its contents and -- return --> the number of blocks read
		while(i < block_info->numberOfRecords){
			// check if the current record is the requested one
			// if it is -- then -->> return the number of blocks read
			// -- else -- continue!
			if(rec[i].id == value){  
				printRecord(rec[i]);   // print record
				
				CALL_BF(BF_UnpinBlock(block));
				BF_Block_Destroy(&block);
				
				return numBlocks_read;
			}
			
			i++;
		}
		
		id++;

		CALL_BF(BF_UnpinBlock(block));
	}
	
	BF_Block_Destroy(&block);
	
    return -1;
}

void print_Metadata_HeapFile(HP_info* hp_info){
	
	// optional: print HP_info (metadata)
	printf("\n\n--  optional: print HP_info (metadata)  --\n\n");
	printf(" fileDesc: %d, \n\n fileType: %s, \n\n recordSize: %d, \n\n maxRecords_per_Block: %d, \n\n lastBlock_id: %d, \n\n header_block: %p \n\n",
			hp_info->fileDesc, 
			hp_info->fileType,
			hp_info->recordSize, 
			hp_info->maxRecords_per_Block, 
			hp_info->lastBlock_id,
			hp_info->header_block
		);
	printf("\n\n------------------------------------------------\n\n");
	
	return;
}

int print_HeapFile(HP_info* hp_info){

	printf("\t\t---   HeapFile   ---\n\n");
	
	// Initialize variables 
	int blocks_num, fd;
	
	// Find the number of blocks
	CALL_BF( BF_GetBlockCounter(hp_info->fileDesc, &blocks_num) );
	
	// If the number of blocks isn't one it means that there are blocks with records (the first block is only for metadata info without records),
	if(blocks_num == 1){ 
		printf("The number of blocks is one it means that there are not blocks with records\n\n");
		return 0;
	}
	
	fd = hp_info->fileDesc;
	
	// auxiliary block
	BF_Block* block;
	BF_Block_Init(&block); 
	
	// print all records of the file storage blocks
	for(int id = 1; id < blocks_num; id++){
		
		printf("\n\n\n--------------------   Block %d.   --------------------\n\n", id);

		CALL_BF(BF_GetBlock(fd, id, block));    // get block from heap file
		
		void* data = BF_Block_GetData(block);    

		Record* rec = data;

		HP_block_info* block_info = data + POS_BLOCKINFO;
		
		int i=0;
		while(i < block_info->numberOfRecords){ 
			printRecord(rec[i]);    // print record
			i++;
		}

		CALL_BF(BF_UnpinBlock(block));
	}
	
	BF_Block_Destroy(&block);
	
	return 0;
}
