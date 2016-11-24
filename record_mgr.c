#include "dberror.h"
#include "expr.h"
#include "tables.h"

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"

// table and manager


// 1, create a page file in hard disk, with certain number of pages. 
// 2, write the schema info to the 0 page.
// 3, create the bit map at first N pages after page 0.
RC initRecordManager (void *mgmtData) {
	return RC_OK;
}
RC shutdownRecordManager () {
	return RC_OK;	
}

//the purpose of this method is to create an table and save it into a pagefile on disk.
//in this method, we create a buffer pool and a BM_Pagehandle, but they are just temporary, no need to keep them after the
//process is done
//a few key info:
//1, length of each attribute is 10 bytes.
//2, each dataType is also 10 bytes.
RC createTable (char *name, Schema *schema) {

	//initialize the pagefile
	openoagefile(name);

	//create a page with PAGE_SIZE
	char *page_data=(char *)malloc(sizeof(PAGE_SIZE));

	//initialize a buffer manager
	BM_BufferPool *bm=(BM_BufferPool *)malloc(sizeof(BM_BufferPool));

	initBufferPool(bm, name, 100, LRU, NULL);
	
	//create a page in memory to hold the information, this page will be write back to disk later
	BM_PageHandle *newPage=(BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	//read page 0 of the page file.
	bm.pinPage(bm, newPage, 0);

    int offeet = 0;

    //the idea is to manually write a schema to the page in memory, and then write it back to the page file on disk

    //set numAttr
	mempcy(page_data+offset, &schecma->num_attr, sizeof(int));
	offset += sizeof(int);

	//set attriNames
	int i;
	for (i = 0; i < schema->numAttr; i++) {

		//we assume the length of an attribute name is less than or equal to 10, these are strings, so no need to use &
		mempcy(page_data+offset, schecma->attrNames[i], 10);
		offset += 10;
	}
	
	//set dataTypes
	//assume there are only two possible data types for attributes
	for (i = 0; i < schema->numAttr; i++) {

		memcpy(page_data+offset, &schema->dataTypes[i], sizeof(int));

		offset += sizeof(int);

	}

	//set typeLength -- not sure about this one, but my understanding for typeLength is: there is a length for each attribute
	for (i = 0; i < schema->schema->numAttr; i++) {
		memcpy(page_Data + offset, &schema->typeLength[i], sizeof(int));
		offset += sizeof(int);
	}

	//set the keyAttrs
	for (i = 0; i < schema->keySize; i++) {
		memcpy(page_Data + offset, &schema->keyAttrs[i], sizeof(int));
		offset += sizeof(int);
	}

	//set the keySize
	memcpy(page_Data + offset, &schema->keySize, sizeof(int));
	offset += sizeof(int);

	//assign this page to BM_PageHandle
	newPage->data=page_data;

	//write back to disk
	markDirty(bm, newPage);
	unpinPage(bm, newPage);
	forceFlushPool(bm);
	shutdownBufferPool(bm);
	free(bm);
	free(newPage);

	return RC_OK;



	/*
	typedef struct Schema
{
  int numAttr;
  char **attrNames;
  DataType *dataTypes;
  int *typeLength;
  int *keyAttrs;
  int keySize;
} Schema;
	*/

}

//when initialize the buffer pool, I chose the number of buffer to be 100
RC openTable (RM_TableData *rel, char *name) {
	
	//initialize a buffer manager
	BM_BufferPool *bm=(BM_BufferPool *)malloc(sizeof(BM_BufferPool));

	initBufferPool(bm, name, 100, RS_LRU, NULL);

	//set the name of Record Manager
	rel->name = name;

	//initialize RM_Mgmtdata
	RM_MgmtData *mgmtData = (RM_MgmtData *)malloc(sizeof(RM_MgmtData));

	mgmtData->bm = bm;

	//assign mgmtData to RM_TableData
	rel->mgmtData = mgmtData;

	//create a page in memory to hold the information, this page will be write back to disk later
	BM_PageHandle *newPage=(BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	pinPage(bm, newPage, 0);

	//read the schema into RM_TableData
	rel->schema = readSchema(newPage->data);

	//calculate and assign recordSize
	mgmtData->recordSize = getRecordSize(rel->schema);

	//calculate and assign pageSlots
	mgmtData->numPageSlot = PAGE_SIZE/mgmtData->recordSize;

	//what does tuple number do?
	memcpy(&mgmtData->tupleNum, newPage->data + TUPLE_NUM_OFFSET, sizeof(int));

	unpinPage(bm, newPage);

	if (keyConstrainEnabled) {
		mgmtData->keyTableSize = 20089;
		getHashKeySize(rel->schema, &mgmtData->hashKeySize);
		calcKeyDataOffset(rel);
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
		readIndex(rel);
	}
	free(pageHandler);
	return rc;

/*
typedef struct RM_TableData
{
  char *name;
  Schema *schema;
  void *mgmtData;
} RM_TableData;
*/

}

RC closeTable (RM_TableData *rel) {
	
	RM_MgmtData *mgmtData = (RM_MgmtData *)rel->mgmtData;
	BM_BufferPool *bm = (BM_BufferPool *)mgmtData->bm;
	freeSchema(rel->schema);
	if (keyConstrainEnabled) {
		writeIndex(rel);
		free(mgmtData->keyDataOffset);
		free(mgmtData->keyDataSize);
		int i;
		RM_ReordHashKey *p1, *p2;;
		for (i = 0; i < mgmtData->keyTableSize; i++) {
			p1 = mgmtData->keyTable[i];
			while (p1 != NULL) {
				p2 = p1->next;
				free(p1);
				p1 = p2;
			}
		}
		free(mgmtData->keyTable);
		free(mgmtData->indexFileName);
	}
	free(mgmtData);
	rc = shutdownBufferPool(bm);
	return rc;
}

RC deleteTable (char *name) {

	return destroyPageFile(name);
}

int getNumTuples (RM_TableData *rel) {

	//read the mgmtData
	RM_MgmtData *mgmtData = (RM_MgmtData *)rel->mgmtData;

	//return the tuple numbers
	return mgmtData->tupleNum;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){


	//get mgmtData of RM_MgmtData
	RM_MgmtData *mgmtData = (RM_MgmtData *)rel->mgmtData;

	//we also need a buffer manager to do two things, one is update hash table, the other is updaate tuple number
	BM_BufferPool *bm = (BM_BufferPool *)mgmtData->bm;


	//BM_PoolMgmtData *bmMgmtData = (BM_PoolMgmtData *)bm->mgmtData;
	//first, initialize a pageHandler
	BM_PageHandle *newPage = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	int freePagePosition = -1;
	int freeSlotPosition = -1;

	//get some free space
	//rc = getFreeSpace(rel, &freePageNo, &freeSlotNo);

	int i, j;
	int pageNumber;
	BM_PageHandle *pageForSpace = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	bool found = false;

	//what is SPACE_META_PAGE_NUM used for?
	for (i = 0; i < SPACE_META_PAGE_NUM; i++) {

		pageNumber = SPACE_META_START_PAGE + i;

		pinPage(bm, pageForSpace, pageNumber);

		for (j = 0; j < PAGE_SIZE; j++) {
			if (pageHandler->data[j] != '1') {
				int slotIndex = i * PAGE_SIZE + j;
				*freePagePosition = RECORD_START_PAGE + slotIndex / mgmtData->numPageSlot;
				*freeSlotPosition = slotIndex % mgmtData->numPageSlot;
				found = true;
				break;
			}
		}
		unpinPage(bm, pageForSpace);

		if (found) {
			break;
		}
	}

	free(pageForSpace);

	if (found==false)
	{
		THROW(-1, "Meta pages are full!!!");
	}
	

	pinPage(bm, newPage, freePageNo);

	record->id.page = freePageNo;
	record->id.slot = freeSlotNo;

	char *slotPosition = freeSlotNo * mgmtData->recordSize + newPage->data;

	memcpy(slotPosition, record->data, mgmtData->recordSize);

	//update space use
	//updateSpaceUse(rel, record->id.page, record->id.slot, '1');
	//update hash table
	int slotPosition, metaPage, offset;

	slotPosition = mgmtData->numPageSlot*(record->id.page - RECORD_START_PAGE) + record->id.slot;

	metaPage = slotPosition / PAGE_SIZE + SCAN_START_PAGE;

	offset = slotPosition % PAGE_SIZE;

	//first, initialize a pageHandler
	BM_PageHandle *newPage2 = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	//read the hash table in, change it to '0', and write back
	pinPage(bm, newPage2, metaPage);

	newPage2->data[offset] = '1';

	markDirty(bm, newPage2);
	unpinPage(bm, newPage2);

	free(newPage2);


	rc = markDirty(bm, newPage);
	rc = unpinPage(bm, newPage);
	
	//update the number of tuples
	//rc = incTupleNum(rel, 1);

	//first, initialize a pageHandler
	BM_PageHandle *newPage3 = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	pinPage(bm, newPage3, 0);

	int tupleNumber = 0;

	memcpy(&tupleNumber, newPage3->data+TUPLE_NUM_OFFSET, sizeof(int));

	//decrease the number
	tupleNumber++;

	memcpy(newPage3->data+TUPLE_NUM_OFFSET, &tupleNumber, sizeof(int));

	//assign the tuple number
	mgmtData->tupleNumber = tupleNumber;

	markDirty(bm, newPage3);
	unpinPage(bm, newPage3);
	
	free(newPage3);

	free(newPage);

	return rc;
}


RC deleteRecord (RM_TableData *rel, RID id) {

	//get mgmtData of RM_MgmtData
	RM_MgmtData *mgmtData = (RM_MgmtData *)rel->mgmtData;

	//we also need a buffer manager to do two things, one is update hash table, the other is updaate tuple number
	BM_BufferPool *bm = (BM_BufferPool *)mgmtData->bm;

	//initialize a record
	Record *record=(Record *)malloc(sizeof(Record));

	createRecord(&record, rel->schema);

	getRecord (rel, id, record);

	//free this record
	freeRecord(record);

	//update hash table
	int slotPosition = mgmtData->numPageSlot*(id.page - RECORD_START_PAGE) + id.slot;

	int metaPage = slotPosition / PAGE_SIZE + SCAN_START_PAGE;

	int offset = slotPosition % PAGE_SIZE;

	//first, initialize a pageHandler
	BM_PageHandle *newPage = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	//read the hash table in, change it to '0', and write back
	pinPage(bm, newPage, metaPage);

	newPage->data[offset] = '0';

	markDirty(bm, newPage);
	unpinPage(bm, newPage);

	free(newPage);
	//update the number of tuples

	//first, initialize a pageHandler
	BM_PageHandle *newPage2 = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	pinPage(bm, newPage2, 0);

	int tupleNumber = 0;

	memcpy(&tupleNumber, newPage2->data+TUPLE_NUM_OFFSET, sizeof(int));

	//decrease the number
	tupleNumber--;

	memcpy(newPage2->data+TUPLE_NUM_OFFSET, &tupleNumber, sizeof(int));

	//assign the tuple number
	mgmtData->tupleNumber = tupleNumber;

	markDirty(bm, newPage2);
	unpinPage(bm, newPage2);
	
	free(newPage2);

	return RC_OK;
}


RC updateRecord (RM_TableData *rel, Record *record) {

	//initialize buffer pool
	RM_MgmtData *mgmtData = (RM_MgmtData *)rel->mgmtData;

	BM_BufferPool *bm = (BM_BufferPool *)mgmtData->bm;

	int recordSize;

	recordSize = mgmtData->recordSize;

	//first, initialize a pageHandler
	BM_PageHandle *newPage = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	pinPage(bm, newPage, record->id.page);

	//find its position
	char* slotPosition = recordSize * record->id.slot + newPage->data;

	//update it
	memcpy(slotPosition, record->data, recordSize);

	markDirty(bm, pageHandler);
	unpinPage(bm, pageHandler);
	free(newPage);

	return RC_OK;
}


RC getRecord (RM_TableData *rel, RID id, Record *record) {
	
	//initialize buffer pool
	RM_MgmtData *mgmtData = (RM_MgmtData *)rel->mgmtData;

	BM_BufferPool *bm = (BM_BufferPool *)mgmtData->bm;

	int recordSize;

	recordSize = mgmtData->recordSize;

	//first, initialize a pageHandler
	BM_PageHandle *newPage = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	
	pinPage(bm, newPage, id.page);

	char* slotPosition = id.slot * recordSize + newPage->data;

	record->id = id;

	memcpy(record->data, slotPosition, recordSize);

	unpinPage(bm, pageHandler);

	free(newPage);

	return RC_OK;
}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {

	//assign the scan object the table object
	scan->rel = rel;

	//intitalize scan mamagement data
	RM_ScanMgmtData *mgmtData = (RM_ScanMgmtData *)malloc(sizeof(RM_ScanMgmtData));

	//assign all related values
	mgmtData->cond = cond;
	mgmtData->nextPage = SCAN_START_PAGE;
	mgmtData->nextPosition = 0;
	mgmtData->numScanned = 0;

	//assign the mgmtData to Scan object
	scan->mgmtData = mgmtData;

	return RC_OK;
}


RC next (RM_ScanHandle *scan, Record *record) {
	RC rc;
	RM_TableData *rel = scan->rel;
	RM_MgmtInfo *rmMgmtData = (RM_MgmtInfo *)rel->mgmtData;
	if (rmMgmtData->tupleNum == 0) {
		return RC_RM_NO_MORE_TUPLES;
	}

	RM_ScanContext *context = (RM_ScanContext *)scan->mgmtData;
	Expr *cond = context->cond;

	int nextPage = context->nextPage;
	int nextIndex = context->nextIndex;
	
	BM_BufferPool *bm = (BM_BufferPool *)rmMgmtData->bm;	
	BM_PoolMgmtData *bmMgmtData = (BM_PoolMgmtData *)bm->mgmtData;

	int j;
	int metaPageUpper = SPACE_META_START_PAGE + SPACE_META_PAGE_NUM;
	int metaPageNo;
	bool found = false;
	bool allScanned = false;
	Record *rd;
	rc = createRecord(&rd, rel->schema);
	BM_PageHandle *pageHandler = MAKE_PAGE_HANDLE();
	BM_PageHandle *rdPageHandler = MAKE_PAGE_HANDLE();
	for (metaPageNo = nextPage; metaPageNo < metaPageUpper; metaPageNo++) {
		rc = pinPage(bm, pageHandler, metaPageNo);
		char *pageData = pageHandler->data;
		for (j = nextIndex; j < PAGE_SIZE; j++) {
			if (pageData[j] == '1') {
				context->tuplesScanned++;
				int slotIndex = (metaPageNo - SPACE_META_START_PAGE) * PAGE_SIZE + j;
				int rdPageNo = RECORD_START_PAGE + slotIndex / rmMgmtData->pageSlots;
				int rdSlotNo = slotIndex % rmMgmtData->pageSlots;
				rd->id.page = rdPageNo;
				rd->id.slot = rdSlotNo;
				
				rc = pinPage(bm, rdPageHandler, rdPageNo);
				char* rdData = rdPageHandler->data + rdSlotNo * rmMgmtData->recordSize;
				memcpy(rd->data, rdData, rmMgmtData->recordSize);
				Value * result;
				evalExpr (rd, rel->schema, cond, &result);
				if (result->v.boolV) {
					record->id = rd->id;
					memcpy(record->data, rd->data, rmMgmtData->recordSize);
					found = true;
					context->nextPage = metaPageNo;
					context->nextIndex = j + 1;
				}
				rc = unpinPage(bm, rdPageHandler);
				if (found)
					break;
				if (context->tuplesScanned == rmMgmtData->tupleNum) {
					allScanned = true;
					break;
				}
			}
		}
		rc = unpinPage(bm, pageHandler);
		if (found || allScanned)
			break;
	}
	freeRecord(rd);
	free(rd);
	free(pageHandler);
	free(rdPageHandler);
	if (found)
		return RC_OK;
	else
		return RC_RM_NO_MORE_TUPLES;
}


RC closeScan (RM_ScanHandle *scan) {
	free(scan->mgmtData);
	return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema) {
	int numAttr=schema->numAttr;
	int i;
	int dataSize=0;

	for (i = 0; i < numAttr; i++) {

		dataSize += schema->typeLength[i];

	}
	
	return dataSize;
}

//simply assign different value to corresponding attribute
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {

	//create a schema object in memory
	Schema *schema = (Schema *)malloc(sizeof(Schema));

	//assign values
	schema->numAttr = numAttr;

	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	schema->typeLength = typeLength;
	schema->keySize = keySize;
	schema->keyAttrs = keys;

	//return the object
	return schema; 
}

//free the memory of schema
RC freeSchema (Schema *schema) {

	//the tricky part is that **attriNames is an array of pointers, so we have to free the pointers one by one
	int i;
	for (i = 0; i < schema->numAttr; i++) {
		free(schema->attrNames[i]);
	}

	//free the pointer to the pointers
	free(schema->attrNames);

	//free other pointers
	free(schema->dataTypes);
	free(schema->typeLength);
	free(schema->keyAttrs);

	return RC_OK;


/*
typedef struct Schema
{
  int numAttr;
  char **attrNames;
  DataType *dataTypes;
  int *typeLength;
  int *keyAttrs;
  int keySize;
} Schema;

*/

}

// dealing with records and attribute values

//the idea of create record is to generate an empty record
RC createRecord (Record **record, Schema *schema) {

	//initialize an record object
	Record *new_record=(Record *)malloc(sizeof(Record));

	int dataSize = getRecordSize(schema);

	//initialize the data in this record
	new_record->data=(char *)malloc(sizeof(char) * dataSize);

	//set other parameters
	new_record->id.page=-1;
	new_record->id.slot=-1;

	//assign it to the input Record object
	*record=new_record;

	return RC_OK;

/*
typedef struct Record
{
  RID id;
  char *data;
} Record;

typedef struct RID {
  int page;
  int slot;
} RID;

*/

}



RC freeRecord (Record *record) {
	free(record->data);
	return RC_OK;
}

//this method read the value from Record *record and assign it to Value **value.
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) {

	int offset;
	int i;

	//calculate the correct offsdet
	for (i=0;i<attrNum;i++)
	{
		offest += schema->typeLength[i];

	}

	char *attribute_data = record->data + offset;

	if(schema->dataTypes[attrNum]==DT_INT)
	{
		int a=-1;
		memcpy(&a, attribute_data, sizeof(int));
		MAKE_VALUE(*value, DT_INT, a);
	}
	else if (schema->dataTypes[attrNum]==DT_FLOAT)
	{
		float a=-1;
		memcpy(&a,attribute_data, sizeof(float));
		MAKE_VALUE(*value, DT_FLOAT, val);
	}
	else if (schema->dataTypes[attrNum]==DT_BOOL)
	{
		bool a;
		memcpy(&a,attribute_data, sizeof(bool));
		MAKE_VALUE(*value, DT_BOOL, a);
	}
	else
	{
		//read the value at schema->typeLength[attrNum] to an integer
		int string_length = schema->typeLength[attrNum];

		//the length has to plus 1 because, in MAKE_STRING_VALUE(), v.stringV = (char *) malloc(strlen(value) + 1)
		char *string_temp = (char *) malloc(string_length + 1);

		strncpy(string_temp, attribute_data, string_length);

		//make the last position to be 0
		string_temp[string_length] = '\0';

		MAKE_STRING_VALUE(*value, string_temp);

		free(string_temp);
	}


	return RC_OK;


/*
typedef struct Value {
  DataType dt;
  union v {
    int intV;
    char *stringV;
    float floatV;
    bool boolV;
  } v;
} Value;
*/

}


RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {

	int offset;
	int i;

	//calculate the correct offsdet
	for (i=0;i<attrNum;i++)
	{
		offest += schema->typeLength[i];

	}

	//set the value depending on the offset
	//assuming only four kinds of data types
	if (schema->dataTypes[attrNum]==DT_INT)
	{
		memcpy(record->data+offset, &value->v.intV, sizeof(int));
	}

	else if (schema->dataTypes[attrNum]==DT_STRING)
	{
		strcpy(record->data+offset, value->v.stringV);
	}

	else if (schema->dataTypes[attrNum]==DT_FLOAT)
	{
		memcpy(record->data+offset, &value->v.floatV, sizeof(float));
	}

	else
	{
		memcpy(record->data+offset, &value->v.boolV, sizeof(bool));
	}

	return RC_OK;

/*
typedef struct Value {
  DataType dt;
  union v {
    int intV;
    char *stringV;
    float floatV;
    bool boolV;
  } v;
} Value;


typedef enum DataType {
  DT_INT = 0,
  DT_STRING = 1,
  DT_FLOAT = 2,
  DT_BOOL = 3
} DataType;

*/


}


#endif // RECORD_MGR_H
