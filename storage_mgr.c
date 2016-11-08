#ifndef STORAGE_MGR_H
#define STORAGE_MGR_H

#include <stdio.h>
#include <stdlib.h>
#include "dberror.h"
#include <string.h>

/* module wide constants */
#define META_SIZE 4096

/************************************************************
 *                    handle data structures                *
 ************************************************************/
typedef struct SM_FileHandle {
  char *fileName;
  int totalNumPages;
  int curPagePos;
  void *mgmtInfo;
} SM_FileHandle;

/* create a new structure to hold the pointer to a FILE object*/
typedef struct SM_mgmtInfo {

	FILE *fp;

} SM_mgmtInfo;

typedef char* SM_PageHandle;

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
void initStorageManager (void) {
	return;
}

	/* 1, create a block in memory, containing the size of meta data and 1 page. In this case
	we only put the information of number of pages into meta data section.

	   2, set the content of this area in memory to be 0.

	   3, set the first 50 bytes to be the area exclusively containing the information of number of pages. The
	number of pages in this case is 1. So we store "1" as a string in this 50 bytes area.

	   4, we write this memory block into harddirve with fwrite(), with target area fp, where fp points to the 
	file that we've created just now.

	   5, at last, we free the memory and the memory we've used and close the FILE pointer.
	*/

RC createPageFile (char *fileName) {
	
	//declare a File object pointer
	FILE *fp;
	
	//create a binary File object
	fp=fopen(fileName, "ab+"); //when manipulating passing string by pointer, use the name directly

	//malloc memory
	char *multipages=(char *)malloc(META_SIZE+PAGE_SIZE); // malloc returns void *
	memset(multipages, '\0', META_SIZE+PAGE_SIZE);
	
	//create a string, give it a value, and copy this string into memory
	char str[50] = {'\0'};
	strcpy(str, "1");
	memcpy(multipages, str, 50);
	
	//fwrite() 
	fwrite(multipages, 1, META_SIZE+PAGE_SIZE, fp);
	
	//free memory
	free(multipages);
	
	//close FILE pointer
	fclose(fp);
	
	//return code
	return RC_OK;

}


	/* 1, When openning a file, we only need to know the meta data secton from the area that stores the file.

	   2, we open the file first, by fopen().

	   3, then malloc a piece of memory for the meta data stored in harddrive.

	   4, then we read the meta data from hard drive to this piece of memory, with fread().

	   5, because we've stored the number information in string format in harddrive,  we need to create a temperory string 
	to hold the information when reading it back. So, we use memcpy() to copy this information from memory to this string. 
	That is because at this point the informatio has already been read from HardDrive to memory.

	   6, Then, we have to convert this information from str to integer.

	   7, With this information, coping with filename, current page(default 0), we fill up the passed filehandle.

	   8, however, we also need to fill the 'void *mgmtInfo'. To do that, we first create an object with SM_mgmtInfo. Then let
	the item *fp inside this SM_mgmtInfo object point to fp which is the pointer to the file stored in harddrive.

	   9, at last, we let SM_mgmtInfo point to 'void *mgmtInfo' in the filehandle (struct SM_FileHandle).
	*/



/*we assume the object for this method is the result from createPageFile*/
RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
	
	//declare a File object
	FILE *fp;
	
	//open a File object
	fp=fopen(fileName, "r+");
	if(fp==NULL)
	{
		return RC_FILE_NOT_FOUND;
	}
	
	//malloc memory 
	char *metapage=(char *)malloc(META_SIZE);
	
	//read meta data to the malloced memory
	fread(metapage, 1, META_SIZE, fp);
	
	//create a string, read the information from memory to this string
	char str[50] = {'\0'};
	
	memcpy(str, metapage, 50);

	//convert it into int
	int total = atoi(str);
	
	//fill up the filehandle
	fHandle->fileName=fileName;
	fHandle->totalNumPages=total;
	fHandle->curPagePos=0;

	//fill up the void *mgmtInfo
    SM_mgmtInfo *mgmtInfomation=(SM_mgmtInfo *)malloc(sizeof(SM_mgmtInfo));
    	
    mgmtInfomation->fp=fp;

	fHandle->mgmtInfo=mgmtInfomation;
	
	return RC_OK;

}

/* 
	Simply close the file:
	1, Get the information from fHandle. Be careful, the fHandle object contains an SM_mgmInfo object, and the 
	SM_mgmInfo object contains fp, which points to the FILE object that we need.
	2, use fclose() function to close the File object.

*/
RC closePageFile (SM_FileHandle *fHandle) {

	//get the information from fHandle.
	FILE *fp;

	SM_mgmtInfo *recieveInfo;
	recieveInfo=fHandle->mgmtInfo;

	fp=recieveInfo->fp;

	//close the FILE object.
	fclose(fp);

	return RC_OK;

}

/* 
	Simply remove the file:
	1, Because the file might not exist at all, we have to verify the failure/success informtion returned by remove()
	if the return value is -1, the file doesn't exist.

*/

RC destroyPageFile (char *fileName) {

	if (remove(fileName)==-1) {
		return RC_FILE_NOT_FOUND;
	}

	return RC_OK;
}

/* reading blocks from disc 

	1, To read a file, we have to get the pointer that points to the FILE object first, which is fp. We get it from fHandle.
	2, We get the total number of pages from fHandle for this file.
	3, Compare pageNum with total number of pages. If the pageNum input is not in the correct range, we will return an eror info.
	4, If the pageNum is correct, we then set the start position for reading by fseek().
	5, Based on the start position set by step 4, we read the specific page from file into the memory address that has been passed
	by memPage.
*/
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

	FILE *fp;

	SM_mgmtInfo *recieveInfo;
	recieveInfo=fHandle->mgmtInfo;

	fp=recieveInfo->fp;

	//compare pagesNum and totalpages
	int filePages=fHandle->totalNumPages;

	if(pageNum >= filePages || pageNum < 0) {

		return RC_READ_NON_EXISTING_PAGE;
	}

	//set the offset before reading
	fseek(fp, META_SIZE+pageNum*PAGE_SIZE, SEEK_SET);

	fHandle->curPagePos=pageNum;

	//read the content into memory
	fread(memPage, 1, PAGE_SIZE, fp);

	close(fp);

	return RC_OK;	

}

/*
	read the current page info from fHandle
	
*/

int getBlockPos (SM_FileHandle *fHandle){

	return fHandle->curPagePos;
}

/*
	Since the first page we read always starts with 0 position, we simply set the page position 0 and read it to memory. We can re-use
	the readblock() method we created.
	
*/

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

	return readBlock(0, fHandle, memPage);

}

/*
	1, Before reading the previous block, we have to check if the previous block exists. If it doesn't exist, we return an error message.
	2, If the previous block exists, we decrease the position by 1, and read it into memory.
	
*/

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

	//validate the position
	if (fHandle->curPagePos==0){
		return RC_READ_NON_EXISTING_PAGE;
	}

	//set the position
	int pageNumber=fHandle->curPagePos-1;

	//read the file into memory
	return readBlock(pageNumber, fHandle, memPage);

}

/*
	simply read the current block.
*/

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

	return readBlock(fHandle->curPagePos, fHandle, memPage);

}

/*
	similiar to the readPreviousBLock() method, we have to validate the position before reading.

	if the position that will be read is not valid, we return an error. If the position is valid, we increase the position by 1, and
	read the info to memory.
*/


RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

	//validate the position
	if(fHandle->curPagePos==fHandle->totalNumPages-1){
		return RC_READ_NON_EXISTING_PAGE;
	}

	//increase the position
	int pageNumber=fHandle->curPagePos+1;

	//read the info
	return readBlock(pageNumber, fHandle, memPage);

}

/*
	The last page is always in the position of totalNumpages minus 1. So we can simply read it from there.
*/

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

	return readBlock(fHandle->totalNumPages-1, fHandle, memPage);

}

/* writing blocks to a page file 
	1, Get the pointer that points to FILE object from fHandle.
	2, Get the total number of page from fHandle.
	3, Varify if pageNum is in the correct range corresponding to the file object.
	4, Set the start position for writing the file.
	5, write the file.

*/
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){

	//Get the pointer that points to FILE object from fHandle.
	FILE *fp;

	SM_mgmtInfo *recieveInfo;

	recieveInfo=fHandle->mgmtInfo;

	fp=recieveInfo->fp;

	//compare pagesNum and totalpages
	int filePages=fHandle->totalNumPages;

	if(pageNum >= filePages || pageNum < 0) {

		return RC_READ_NON_EXISTING_PAGE;
	}

	//Set the start position for reading the file.
	int offset=META_SIZE+pageNum*PAGE_SIZE;

	fseek(fp, offset, SEEK_SET);

	//write the file.
	fwrite(memPage, 1, PAGE_SIZE, fp);

	return RC_OK;	
}

/*
	simply write to the current page.
*/

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

	return writeBlock(fHandle->curPagePos, fHandle, memPage);

}

/*
	Append an empty block to the file object.
	1, Create an empty block in memory with the size of a page.
	2, Get the pointer that points to the file object.
	3, Set the offset, which is the size of the meta data plus all the pages.
	4, Write the empty block at the end of the file object.
*/

RC appendEmptyBlock (SM_FileHandle *fHandle){

	//malloc memory
	char *newpage=(char *)malloc(PAGE_SIZE);

	memset(newpage, '\0', PAGE_SIZE);

	//get the pointer to the file
	FILE *fp;

	SM_mgmtInfo *recieveInfo;

	recieveInfo=fHandle->mgmtInfo;

	fp=recieveInfo->fp;

	//set offset
	int offset=META_SIZE+fHandle->totalNumPages*PAGE_SIZE;

	fseek(fp, offset, SEEK_SET);

	//write the empty block into the file.
	fwrite(newpage, 1, PAGE_SIZE, fp);

	fHandle->totalNumPages++;

	//update menta data page
	char str[50] = {'\0'};
	sprintf(str, "%d", fHandle->totalNumPages);

	fseek(fp, 0, SEEK_SET);
	int c = fwrite(str, 1, 50, fp);

	free(newpage);

	return RC_OK;
}

/*
	This method is to ensure if there is enough room for a file object.
	1, Get the pointer that points to the file object.
	2, Get the total number of pages.
	3, Compare the input number with the current number of pages in the file. If the difference is less than or equal to 0, it is ok.
	4, If the difference is larger than 0. Then repeatedly add an empty block to the end of the file object, by reusing the a
	ppendEmptyBlock() method.
*/

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){

	//Get the pointer that points to the file object.
	FILE *fp;

	SM_mgmtInfo *recieveInfo;

	recieveInfo=fHandle->mgmtInfo;

	fp=recieveInfo->fp;

	//Get the total number of pages.
	int totalPage=fHandle->totalNumPages;

	//Compare difference
	int diff=numberOfPages-totalPage;

	if(diff<=0)
	{
		return RC_OK;
	}

	//Repeatedly adding an empty block.
	int i;

	for (i=0;i<diff;i++) {
		appendEmptyBlock(fHandle);
	}

	return RC_OK;

}

#endif
