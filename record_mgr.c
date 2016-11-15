#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "buffer_mgr.h"
#include "record_mgr.h"
#include "storage_mgr.h"
#include "tables.h"
#include "rm_serializer.c"

typedef struct fileInfo{
	BM_BufferPool *bm;
	int data_start_page;
	int data_end_page;
	int free_space;

}fileInfo;

//table and manager
RC initRecordManager(void *mgmtData){
	initBufferPool();
	return 0;
}

RC shutdownRecordManager(){
	return 0;
}

//need to store information about free space and schema on information page
RC createTable(char *name, Schema *schema){

	SM_FileHandle *fileHandle=(SM_FileHandle *)malloc(sizeof(SM_FileHandle));

	createPageFile(name);
	openPageFile(name, fileHandle);

	char schema_metadata = serializeSchema(*schema);

	//writes the table information to the first page
	writeBlock(0, fileHandle, schema_metadata);
	return 0;
}

RC openTable(RM_TableData *rel, char *name){

	SM_FileHandle *fHandle=(SM_FileHandle *)malloc(sizeof(SM_FileHandle));
	fileInfo *fInfo=(fileInfo *)malloc(sizeof(fileInfo));
	BM_PageHandle *pHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	openPageFile(name, fHandle);

	readBlock(0, fHandle, pHandle);

	rel->name = name;
	//Not sure if we need to, set rel->schema by reading the first page
	//set rel->schema equal to serialized contents in page 0

	return 0;
}

RC closeTable (RM_TableData *rel){
	return 0;
}

RC deleteTable (char *name){
	return 0;
}
int getNumTuples (RM_TableData *rel){
	return 0;
}

//handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){

	return 0;
}
RC deleteRecord (RM_TableData *rel, RID id){
	return 0;
}
RC updateRecord (RM_TableData *rel, Record *record){
	return 0;
}
RC getRecord (RM_TableData *rel, RID id, Record *record){
	return 0;
}

//scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
	return 0;
}
RC next (RM_ScanHandle *scan, Record *record){
	return 0;
}
RC closeScan (RM_ScanHandle *scan){
	return 0;
}

// dealing with schemas
int getRecordSize (Schema *schema){
	return 0;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
	return 0;
}

RC freeSchema (Schema *schema){
	return 0;
}

// dealing with records and attribute values
createRecord (Record **record, Schema *schema){
	return 0;
}

RC freeRecord (Record *record){
	return 0;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
	return 0;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
	return 0;
}


