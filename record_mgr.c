#include "dberror.h"
#include "expr.h"
#include "tables.h"

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"


typedef struct RM_MgmtData {
  BM_BufferPool *bm;
  int recordSize;
  int numPageSlot;
  int tupleNumber;
  int *keyDataOffset;
  int *keyDataSize;
  int keyTableSize;
  
} RM_MgmtData;

typedef struct RM_ScanMgmtData {
  Expr *cond;
  int nextPage;
  int nextPosition;
  int numScanned;
} RM_ScanMgmtData;
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
	createPageFile(name);

	
	//initialize a buffer manager
	BM_BufferPool *bm=(BM_BufferPool *)malloc(sizeof(BM_BufferPool));

	initBufferPool(bm, name, 100, RS_LRU, NULL);
	
	//create a page in memory to hold the information, this page will be write back to disk later
	BM_PageHandle *newPage=(BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	//read page 0 of the page file.
	pinPage(bm, newPage, 0);

	char *page_data=newPage->data;

    int offset = 0;

    //the idea is to manually write a schema to the page in memory, and then write it back to the page file on disk

    //set numAttr
	memcpy(page_data+offset, &schema->numAttr, sizeof(int));
	offset += sizeof(int);

	//set attriNames
	int i;
	for (i = 0; i < schema->numAttr; i++) {

		//we assume the length of an attribute name is less than or equal to 10, these are strings, so no need to use &
		memcpy(page_data+offset, schema->attrNames[i], 10);
		offset += 10;
	}
	
	//set dataTypes
	//assume there are only two possible data types for attributes
	for (i = 0; i < schema->numAttr; i++) {

		memcpy(page_data+offset, &schema->dataTypes[i], sizeof(int));

		offset += sizeof(int);

	}

	//set typeLength -- not sure about this one, but my understanding for typeLength is: there is a length for each attribute
	for (i = 0; i < schema->numAttr; i++) {
		memcpy(page_data + offset, &schema->typeLength[i], sizeof(int));
		offset += sizeof(int);
	}

	//set the keyAttrs
	for (i = 0; i < schema->keySize; i++) {
		memcpy(page_data + offset, &schema->keyAttrs[i], sizeof(int));
		offset += sizeof(int);
	}

	//set the keySize
	memcpy(page_data + offset, &schema->keySize, sizeof(int));
	offset += sizeof(int);

	
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

	initBufferPool(bm, name, PAGE_BUFFER_SIZE, RS_LRU, NULL);

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
	//initialize the pagefile

	//create a page with PAGE_SIZE
	char *page_data=newPage->data;


	int numAttr;
	char **attrNames; // -> 0 ->
	//						1 ->
	//						2 ->
	DataType *dataTypes;
	int *typeLength;
	int keySize;
	int *keys;


	//

	//initialize a buffer manager

    int offset = 0;

    //the idea is to manually write a schema to the page in memory, and then write it back to the page file on disk

    //set numAttr
	memcpy(&numAttr, page_data+offset, sizeof(int));
	offset += sizeof(int);

	attrNames = (char*)malloc(sizeof(char*) * numAttr);
	dataTypes = (DataType*)malloc(sizeof(DataType) * numAttr);
	typeLength = (int *)malloc(sizeof(int)*numAttr);


	//set attriNames
	int i;
	for (i = 0; i < numAttr; i++) {

		attrNames[i] = (char*)malloc(10);
		//we assume the length of an attribute name is less than or equal to 10, these are strings, so no need to use &
		memcpy(attrNames[i], page_data+offset, 10);
		offset += 10;
	}
	
	//set dataTypes
	//assume there are only two possible data types for attributes
	for (i = 0; i < numAttr; i++) {

		memcpy(&dataTypes[i], page_data+offset, sizeof(int));

		offset += sizeof(int);

	}

	//set typeLength -- not sure about this one, but my understanding for typeLength is: there is a length for each attribute
	for (i = 0; i < numAttr; i++) {
		memcpy(&typeLength[i], page_data + offset, sizeof(int));
		offset += sizeof(int);
	}

	//set the keySize
	memcpy(&keySize, page_data + offset, sizeof(int));
	offset += sizeof(int);

	keys = (int*)malloc(sizeof(int)*keySize);

	//set the keyAttrs
	for (i = 0; i < keySize; i++) {
		memcpy(&keys[i], page_data + offset, sizeof(int));
		offset += sizeof(int);
	}


	//write back to disk

	Schema *schema=createSchema (numAttr, attrNames, dataTypes, typeLength, keySize, keys);

	rel->schema=schema;
	//read schema ends

	//calculate and assign recordSize
	mgmtData->recordSize = getRecordSize(rel->schema);

	//calculate and assign pageSlots
	mgmtData->numPageSlot = PAGE_SIZE/mgmtData->recordSize;

	//what does tuple number do?
	memcpy(&mgmtData->tupleNumber, newPage->data + TUPLE_NUM_OFFSET, sizeof(int));

	unpinPage(bm, newPage);

	free(newPage);

	return RC_OK;

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

	BM_PageHandle *newPage2 = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	pinPage(bm, newPage2, 0);

	unpinPage(bm, newPage2);

	freeSchema(rel->schema);
	
	shutdownBufferPool(bm);

	free(mgmtData);
	
	return RC_OK;
}

RC deleteTable (char *name) {

	return destroyPageFile(name);
}

int getNumTuples (RM_TableData *rel) {

	//read the mgmtData
	RM_MgmtData *mgmtData = (RM_MgmtData *)rel->mgmtData;

	//return the tuple numbers
	return mgmtData->tupleNumber;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){


	//get mgmtData of RM_MgmtData
	RM_MgmtData *mgmtData = (RM_MgmtData *)rel->mgmtData;

	//we also need a buffer manager to do two things, one is update hash table, the other is updaate tuple number
	BM_BufferPool *bm = (BM_BufferPool *)mgmtData->bm;

	//first, initialize pageHandler
	BM_PageHandle *newPage = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	BM_PageHandle *pageForSpace = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	int freePagePosition = -1;
	int freeSlotPosition = -1;

	//get some free space
	int i;
	int j;
	int slot;
	int pageNumber;

	bool found = false;
	int record_position=1+BITMAP_NUMBER_PAGE;

	//what is BITMAP_END_PAGE used for?
	for (i = 0; i < BITMAP_NUMBER_PAGE; i++) {

		pageNumber = BITMAP_START_PAGE + i;

		pinPage(bm, pageForSpace, pageNumber);

		for (j = 0; j < PAGE_SIZE; j++) {

			if (pageForSpace->data[j] != '1') {

				slot = i * PAGE_SIZE + j;

				freePagePosition = slot/mgmtData->numPageSlot + record_position;

				freeSlotPosition = slot%mgmtData->numPageSlot;

				found = true;

				break;

			}
		}
		unpinPage(bm, pageForSpace);


	}

	free(pageForSpace);

	pinPage(bm, newPage, freePagePosition);

	record->id.page = freePagePosition;
	record->id.slot = freeSlotPosition;

	char *slPosition = freeSlotPosition * mgmtData->recordSize + newPage->data;

	memcpy(slPosition, record->data, mgmtData->recordSize);

	//update hash table
	int slotPosition, metaPage, offset;

	slotPosition = mgmtData->numPageSlot*(record->id.page - record_position) + record->id.slot;

	metaPage = slotPosition/PAGE_SIZE + SCAN_START_PAGE;

	offset = slotPosition%PAGE_SIZE;

	//first, initialize a pageHandler
	BM_PageHandle *newPage2 = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	//read the hash table in, change it to '0', and write back
	pinPage(bm, newPage2, metaPage);

	newPage2->data[offset] = '1';

	markDirty(bm, newPage2);
	unpinPage(bm, newPage2);

	free(newPage2);

	markDirty(bm, newPage);
	unpinPage(bm, newPage);
	
	//update the number of tuples

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

	return RC_OK;
}


RC deleteRecord (RM_TableData *rel, RID id) {

	int record_position=1+BITMAP_NUMBER_PAGE;

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
	int slotPosition;
	int metaPage;
	int offset;

	slotPosition = mgmtData->numPageSlot*(id.page - record_position) + id.slot;
	metaPage = slotPosition / PAGE_SIZE + SCAN_START_PAGE;
	offset = slotPosition % PAGE_SIZE;

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

	markDirty(bm, newPage);
	unpinPage(bm, newPage);
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

	unpinPage(bm, newPage);

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

	//get the mgmtData for RM and ScanMgmtData
	RM_TableData *rel = scan->rel;
	RM_MgmtData *mgmtData = (RM_MgmtData *)rel->mgmtData;
	RM_ScanMgmtData *scan_mgmtData = (RM_ScanMgmtData *)scan->mgmtData;
	Expr *cond = scan_mgmtData->cond;

	BM_PageHandle *newPage = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
	BM_PageHandle *newPage2 = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

	//initialize a record
	Record *rd;
	createRecord(&rd, rel->schema);

	//initialize parameters
	int record_position;
	int nextPage;
	int nextPosition;
	int i, j;
	int scan_range;
	bool found;
	bool scaned_OK;
	char *data;

	int slotPosition;

	//assign values
	record_position=1+BITMAP_NUMBER_PAGE;
	nextPage = scan_mgmtData->nextPage;
	nextPosition = scan_mgmtData->nextPosition;
	scan_range = BITMAP_START_PAGE + BITMAP_NUMBER_PAGE;
	found = false;
	scaned_OK = false;
	i = nextPage;


	//initialize buffer pool
	BM_BufferPool *bm = (BM_BufferPool *)mgmtData->bm;	

	while(!found && !scaned_OK && i < scan_range)
	{

		pinPage(bm, newPage, i);

		char *data = newPage->data;

		for (j = nextPosition; j < PAGE_SIZE; j++) {

			if (data[j] == '1') {

				//locate the record position in terms of page and slot
				slotPosition = (i - BITMAP_START_PAGE) * PAGE_SIZE + j;

				rd->id.page = record_position+slotPosition/mgmtData->numPageSlot;

				rd->id.slot  = slotPosition % mgmtData->numPageSlot;
				
				//read it
				pinPage(bm, newPage2, rd->id.page);

				int record_offset=rd->id.slot * mgmtData->recordSize;

				char* record_Data = newPage2->data + record_offset;

				//get the content of the record
				memcpy(rd->data, record_Data, mgmtData->recordSize);

				//check the content
				Value * result;

				evalExpr (rd, rel->schema, cond, &result);

				if (result->v.boolV==true) {

					record->id = rd->id;

					memcpy(record->data, rd->data, mgmtData->recordSize);

					scan_mgmtData->nextPage = i;

					scan_mgmtData->nextPosition = j + 1;

					found = true;

				}

				unpinPage(bm, newPage2);

				scan_mgmtData->numScanned++;

				if (found)
					break;

				if (scan_mgmtData->numScanned == mgmtData->tupleNumber) {

					scaned_OK = true;
					break;
				}
			}
		}
		unpinPage(bm, newPage);

		i++;
	}

	free(newPage);
	free(newPage2);
	freeRecord(rd);
	free(rd);
	

	if (found)
	{
		return RC_OK;
	}
	else
	{
		return RC_RM_NO_MORE_TUPLES;
	}

/*
typedef struct RM_ScanMgmtData {
  Expr *cond;
  int nextPage;
  int nextPosition;
  int numScanned;
} RM_ScanMgmtData;


typedef struct RM_MgmtData {
  BM_BufferPool *bm;
  int recordSize;
  int numPageSlot;
  int tupleNumber;
  int *keyDataOffset;
  int *keyDataSize;
  int keyTableSize;
  
} RM_MgmtData;

*/


}


RC closeScan (RM_ScanHandle *scan) {
	free(scan->mgmtData);
	return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema) {

	int numAttr;
	int i;
	int dataSize;

	numAttr=schema->numAttr;
	dataSize=0;

	for (i = 0; i < numAttr; i++) {

		if (schema->dataTypes[i] == DT_INT) {
			dataSize += sizeof(int);	
		} else {
			dataSize += schema->typeLength[i];	
		}
	
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

	int offset=0;
	int i;
	
	//calculate the correct offsdet
	for (i=0;i<attrNum;i++)
	{
		if (schema->dataTypes[i] == DT_INT) {
			offset += sizeof(int);	
		} else {
			offset += schema->typeLength[i];	
		}
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
		MAKE_VALUE(*value, DT_FLOAT, a);
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

	int offset=0;
	int i;

	//calculate the correct offsdet
	for (i=0;i<attrNum;i++)
	{
		if (schema->dataTypes[i] == DT_INT) {
			offset += sizeof(int);	
		} else {
			offset += schema->typeLength[i];	
		}
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

