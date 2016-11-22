#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "buffer_mgr.h"
#include "record_mgr.h"
#include "storage_mgr.h"
#include "tables.h"
#include "rm_serializer.c"

typedef struct scanInfo{
	int curPage;
	int curSlot;
	Expr *search_cond;

}scanInfo;


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

	rc = createPageFile(name);

	rc = initBufferPool(bm, name, 3, RS_LRU, NULL);

	rc = pinPage(bm, pageHandler, 0);

	writeSchema(schema, pageHandler->data);

	int tupleNum = 0;

	memcpy(pageHandler->data + TUPLE_NUM_OFFSET, &tupleNum, sizeof(int));

	rc = markDirty(bm, pageHandler);

	rc = unpinPage(bm, pageHandler);

	rc = forcePage(bm, pageHandler);

	rc = shutdownBufferPool(bm);

	return rc;
}

RC openTable (RM_TableData *rel, char *name) {

	BM_BufferPool *bm = MAKE_POOL();

	rc = initBufferPool(bm, name, PAGE_BUFFER_SIZE, RS_LRU, NULL);

	rel->name = name;

	RM_MgmtInfo *mgmtData = (RM_MgmtInfo *)malloc(sizeof(RM_MgmtInfo));

	mgmtData->bm = bm;

	rel->mgmtData = mgmtData;

	rc = pinPage(bm, pageHandler, 0);

	rel->schema = readSchema(pageHandler->data);

	mgmtData->recordSize = getRecordSize(rel->schema);

	mgmtData->pageSlots = PAGE_SIZE / mgmtData->recordSize;

	memcpy(&mgmtData->tupleNum, pageHandler->data + TUPLE_NUM_OFFSET, sizeof(int));

	rc = unpinPage(bm, pageHandler);

	if (keyConstrainEnabled) {

		mgmtData->keyTableSize = 20000;

		mgmtData->keyTable = (RM_ReordHashKey **)malloc(sizeof(RM_ReordHashKey *) * mgmtData->keyTableSize);
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

	if (keyConstrainEnabled) {
		writeIndex(rel);

		int i;

		RM_ReordHashKey *p1, *p2;;

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

	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RM_MgmtInfo *mgmtData = (RM_MgmtInfo *)rel->mgmtData;

	FILE *file = fopen(mgmtData->indexFileName, "r");

	mgmtData->bm = bm;
	rel->mgmtData = mgmtData;

	int pageSlots = mgmtData->pageSlots;
	int recordSize = mgmtData->recordSize;

	strcpy(page, 0);
	readBlock(1, *file, *page);

	mgmtData->tupleNum++;


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
	scanInfo *sc = (scanInfo *)malloc(sizeof(scanInfo*));
	scan->rel = rel;

	sc->curPage = 1; //Adjust according to directory
	sc->curSlot = 0;
	sc->search_cond = cond;

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
			case DT_FLOAT:
				size += sizeof(float);
				break;
			case DT_BOOL:
				size += sizeof(bool);
				break;
			case DT_INT:
				size += sizeof(int);
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

	for(i=0: i<attrNum; i++){
		offset += schema->typeLength[i];
	}

}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
	return 0;
}


