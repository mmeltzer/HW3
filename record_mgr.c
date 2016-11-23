#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "buffer_mgr.h"
#include "record_mgr.h"
#include "storage_mgr.h"
#include "tables.h"
#include "rm_serializer.c"

typedef struct RM_RecordHashKey{
	int next;
	void *mgmtData;
}RM_RecordHashKey

typedef struct RM_MgmtInfo{
	int recordSize;
	int pageSlots;
	int tupleNum;
	RM_RecordHashKey *keyTable;
	int keyTableSize;
	char *indexFileName;
	BM_BufferPool *bm;
}RM_MgmtInfo

int TUPLE_NUM_OFFSET = 0;
int PAGE_BUFFER_SIZE = 4096;

//table and manager
RC initRecordManager(void *mgmtData){
	initBufferPool();
	return 0;
}

RC shutdownRecordManager(){
	return 0;
}

//need to store information about free space and schema on information page
RC createTable (char *name, Schema *schema) {

	FILE *file = fopen(name, "r");
	BM_PageHandle *ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	BM_BufferPool *bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
	RC rc;
	rc = createPageFile(name);
	rc = initBufferPool(bm, name, 3, RS_LRU, NULL);
	rc = pinPage(bm, ph, 0);

	writeSchema(schema, ph->data);

	int tupleNum = 0;

	memcpy(ph->data + TUPLE_NUM_OFFSET, &tupleNum, sizeof(int));

	rc = markDirty(bm, ph);
	rc = unpinPage(bm, ph);
	rc = forcePage(bm, ph);
	rc = shutdownBufferPool(bm);

	return rc;
}

RC openTable (RM_TableData *rel, char *name) {

	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	RC rc;

	rc = initBufferPool(bm, name, PAGE_BUFFER_SIZE, RS_LRU, NULL);
	rel->name = name;

	RM_MgmtInfo *mgmtData = (RM_MgmtInfo *)malloc(sizeof(RM_MgmtInfo));
	mgmtData-> bm = bm;

	rel->mgmtData = mgmtData;
	rc = pinPage(bm, ph, 0);
	rel->schema = readSchema(ph->data);

	mgmtData->recordSize = getRecordSize(rel->schema);
	mgmtData->pageSlots = PAGE_SIZE / mgmtData->recordSize;

	memcpy(&mgmtData->tupleNum, ph->data + TUPLE_NUM_OFFSET, sizeof(int));

	rc = unpinPage(bm, ph);

	bool keyConstrainEnabled = 0;

	if (keyConstrainEnabled) {

		mgmtData->keyTableSize = 20000;
		mgmtData->keyTable = (RM_RecordHashKey **)malloc(sizeof(RM_RecordHashKey *) * mgmtData->keyTableSize);
		int i;

		for (i = 0; i < mgmtData->keyTableSize; i++) {
			mgmtData->keyTable[i] = NULL;
		}

		char *tmp = "._index_";
		int len = strlen(name) + strlen(tmp) + 1;
		mgmtData->indexFileName = (char*)calloc(len, sizeof(char));
		strcpy(mgmtData->indexFileName, name);
		strcat(mgmtData->indexFileName, tmp);
	}

	return rc;
}

RC closeTable (RM_TableData *rel) {

	RM_MgmtInfo *rmMgmtData = (RM_MgmtInfo *)rel->mgmtData;
	BM_BufferPool *bm = (BM_BufferPool *)rmMgmtData->bm;

	RC rc;

	bool keyConstrainEnabled = 0;

	if (keyConstrainEnabled) {
		writeIndex(rel);
		int i;
		RM_RecordHashKey *p1, *p2;;

		for (i = 0; i < rmMgmtData->keyTableSize; i++) {
			p1 = rmMgmtData->keyTable[i];
			while (p1 != NULL) {
				p2 = p1->next;
				free(p1);
				p1 = p2;
			}
		}

	}

	rc = shutdownBufferPool(bm);
	return rc;
}

RC deleteTable (char *name) {

	return destroyPageFile(name);
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

	int i, size = 0;

	for (i = 0; i < schema->numAttr; i++) {
		switch (schema->dataTypes[i]) {
		case DT_INT:
			size += sizeof(int);
			break;
		case DT_FLOAT:
			size += sizeof(float);
			break;
		case DT_BOOL:
			size += sizeof(bool);
			break;
		case DT_STRING:
			size += schema->typeLength[i];
			break;
		default:
			break;
		}
	}

		return size;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
	Schema *schema = (Schema *) malloc(sizeof(Schema));

	schema->numAttr = numAttr;
	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	schema->typeLength = typeLength;
	schema->keySize = keySize;
	schema->keyAttrs = keys;

	return schema;
}

RC freeSchema (Schema *schema){
	free(schema->attrNames);
	free(schema->dataTypes);
	free(schema->typeLength);
	free(schema->keyAttrs);
	free(schema);

	return 0;
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema){
	Record *rec = (Record *)malloc(sizeof(Record));
	char *rec_data = (char *) malloc(sizeof(char) * getRecordSize(schema));

	rec->data = rec_data;
	*record = rec;

	return 0;
}

RC freeRecord (Record *record){
	free(record->data);
	free(record);

	return 0;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
	Value *v = (Value *) malloc(sizeof(Value));
	int i; int offset = 0;

	for(i=0; i<attrNum; i++){
		if(schema->dataTypes[i] == DT_STRING){
			offset += (schema->typeLength[i] * sizeof(char));
		}
		else{
			offset += sizeof(schema->dataTypes[i]);
		}
	}

	switch(schema->dataTypes[attrNum]){
		case DT_INT:{
			memcpy(&(v->v.intV), &(record->data[offset]), sizeof(int));
			break;
		}
		case DT_FLOAT:{
			memcpy(&(v->v.floatV), &(record->data[offset]), sizeof(float));
			break;
		}
		case DT_BOOL:{
			memcpy(&(v->v.boolV), &(record->data[offset]), sizeof(bool));
			break;
		}
		case DT_STRING:{
			//Not sure how to do this case
			break;
		}
	}
	return 0;
}


RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
	int i; int offset = 0;

	for(i=0; i<attrNum; i++){
		offset += sizeof(schema->dataTypes[i]);
	}

	switch(schema->dataTypes[attrNum]){
		case DT_INT:{
			memcpy(&(record->data[offset]), &value->v.intV, sizeof(int));
			break;
		}
		case DT_FLOAT:{
			memcpy(&(record->data[offset]), &value->v.floatV, sizeof(float));
			break;
		}
		case DT_BOOL:{
			memcpy(&(record->data[offset]), &value->v.boolV, sizeof(bool));
			break;
		}
		case DT_STRING:{
			//Not sure how to do this case
			break;
		}
	}
	return 0;
}


