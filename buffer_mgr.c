// Include return codes and methods for logging errors
#include <stdio.h>
#include <stdlib.h>
#include "dberror.h"
#include <string.h>

// Include bool DT
#include "dt.h"
#include "buffer_mgr_stat.h"

/*
enum flag { const1, const2, ..., constN };
Here, name of the enumeration is flag.
And, const1, const2,...., constN are values of type flag.
By default, const1 is 0, const2  is 1 and so on. You can change default values of enum elements during 
declaration (if necessary).
*/


// Replacement Strategies

// Data Types and Structures


int g_tick = 0;


//the BM_mgmtData structure comprises:
//1, the pointer to the memory space that contains the pages.
//2, a pointer to a SM_FileHandle object that contains all the info about a file on disk.
typedef struct BM_mgmtData {

  BM_PageHandle *pages; //buffer initial address
  SM_FileHandle *fileHandle;

  //add two features, the number of pages in the pool that is occupied.
  int page_count;

  //add head and tail for FIFO queue
	int read_count;
	int write_count;

  int *LRU_Order; //hold the accumulative number of LRU, use pointer to point to an array.

} BM_mgmtData;

// convenience macros

/*
char * const a;
means that the pointer is constant and immutable but the pointed data is not.

const char * a;
means that the pointed data cannot be written to using the pointer a.

const char * const
which is a pointer that you cannot change, to a char (or multiple chars) that you cannot change. 
*/



// Buffer Manager Interface Pool Handling

// a little confused about the stratData, what does it do?
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData){

    //g_tick = 0;
    //initialize the BM_mgmtData
    //1, create an BM_mgmtData object
    BM_mgmtData *mgmtDataPool=(BM_mgmtData *)malloc(sizeof(BM_mgmtData));

    //create BM_PageHandle array
    BM_PageHandle *BM_pages=(BM_PageHandle *)malloc(sizeof(BM_PageHandle) * numPages);

    int i, k;

    for (i=0;i<numPages;i++){

        //repeatedly create new memory space, and assign the space to BM_PageHandle's.
        SM_PageHandle page=(SM_PageHandle)malloc(PAGE_SIZE);

        //when using [], the pointer turns to object, so we use '.'
        BM_pages[i].pageNum=-1;   //at beginning, set all page number to be -1.
        BM_pages[i].data=page;
        BM_pages[i].pin_fix_count=0;
        BM_pages[i].dirty=0;

    }

    //assign the BM_PageHandle array to BM_mgmtData
    mgmtDataPool->pages = BM_pages;



    //2, initialize a SM_FileHandle, and save it into BM_mgmtData
    SM_FileHandle *fileHandle=(SM_FileHandle *)malloc(sizeof(SM_FileHandle));

    openPageFile(pageFileName, fileHandle);

    mgmtDataPool->fileHandle=fileHandle;

    //3, initialize the int array that contains the dirty information, make them all 0.
    int *LRU_Order=(int *)malloc(sizeof(int)*numPages);

    memset(LRU_Order, 0, sizeof(int) * numPages);

    mgmtDataPool->LRU_Order=LRU_Order;

    //4, page count
    mgmtDataPool->page_count=0;

    //5, head and tail for FIFO queue structure

    //6, update the read_count and write_count
    mgmtDataPool->read_count = 0;
    mgmtDataPool->write_count = 0;

    //by now, the BM_mgmtData object has the memory space as well as all info about the file on disk.

    //at last, save all info into BM_BufferPool, including the BM_mgmtData just created.
    bm->pageFile=pageFileName;
    bm->numPages=numPages;
    bm->strategy=strategy;
    bm->mgmtData=mgmtDataPool;


    return RC_OK;


}


//how to completely shut down a buffer pool? modify it later
RC shutdownBufferPool(BM_BufferPool *const bm){

    int i, num_page;

    BM_mgmtData *mgmtData=(BM_mgmtData *)(bm->mgmtData);

    forceFlushPool(bm);

    //free all of the pages
    num_page=mgmtData->page_count;

    for(i=0;i<num_page;i++)
    {
      free(mgmtData->pages[i].data);
    }


    //free mgmtData
    free(mgmtData->pages);
    free(mgmtData->fileHandle);
    free(mgmtData);


    closePageFile(mgmtData->fileHandle);

    return RC_OK;

}

//
RC forceFlushPool(BM_BufferPool *const bm){

  //write back after checking the dirty attribute and pin_fix_count attribute
  int i, page_count;

  //because mgmtData is void type in struct, so have to coerce it into BM_mgmtData it is not working if using 'page_count=bm->mgmtData->page_count;'
  BM_mgmtData *mgmtData=(BM_mgmtData *)(bm->mgmtData);

  page_count=mgmtData->page_count;

  for(i=0;i<page_count;i++){

    if(mgmtData->pages[i].pin_fix_count==0 && mgmtData->pages[i].dirty==1){

      writeBlock(mgmtData->pages[i].pageNum, mgmtData->fileHandle, mgmtData->pages[i].data);
      mgmtData->write_count++;

      //change the dirty into 0
      mgmtData->pages[i].dirty=0;

    }
  }

  return RC_OK;

}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){

  //find the page in the buffer pool
  int position, k, numPages, page_count;

  position=0;

  BM_mgmtData *mgmtData=(BM_mgmtData *)(bm->mgmtData);

  numPages=bm->numPages;
  
  page_count=mgmtData->page_count;

  int exist=0;

  while(exist==0&&position<page_count){

    k=mgmtData->pages[position].pageNum;

    if(page->pageNum==k){
      
      exist=1;

    }

    position++; //the position will be one more than real position when the loop ends

  }

  if(exist==1){
    position--;

    mgmtData->pages[position].dirty=1;

  }

  return RC_OK;

}


RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){

  //find the page in the buffer pool
  int position, k, numPages, page_count;

  position=0;

  BM_mgmtData *mgmtData=(BM_mgmtData *)(bm->mgmtData);

  numPages=bm->numPages;
  
  page_count=mgmtData->page_count;

  int exist=0;

  while(exist==0&&position<page_count){

    k=mgmtData->pages[position].pageNum;

    if(page->pageNum==k){
      
      exist=1;

    }

    position++; //the position will be one more than real position when the loop ends

  }

  if(exist==1){

    position--;

    mgmtData->pages[position].pin_fix_count--;

  }

  
  return RC_OK;

}


//assume that input BM_PageHandle has the page number and the data.
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
  

  BM_mgmtData *mgmtData=(BM_mgmtData *)(bm->mgmtData);

  writeBlock(page->pageNum, mgmtData->fileHandle, page->data);
  mgmtData->write_count++;

  //locate the position of the desired page in the buffer pool

  //find the page in the buffer pool
  int position, k, numPages, page_count;

  position=0;

  numPages=bm->numPages;

  page_count=mgmtData->page_count;

  int exist=0;

  while(exist==0&&position<page_count){

    k=mgmtData->pages[position].pageNum;

    if(page->pageNum==k){
      
      exist=1;

    }

    position++; //the position will be one more than real position when the loop ends

  }

  position--;

  //change the dirty to 0
  mgmtData->pages[position].dirty=0;

  return RC_OK;

}

//different strategies is implemented here.
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum){

  
  //check if the page is already in the buffer

  //get the position of this page
  int position, k, numPages, page_count;

  position=0;

  BM_mgmtData *mgmtData=(BM_mgmtData *)(bm->mgmtData);

  if (pageNum  >= mgmtData->fileHandle->totalNumPages) {
    ensureCapacity(pageNum + 1, mgmtData->fileHandle);
  }

  numPages=bm->numPages;

  page_count=mgmtData->page_count;

  int exist=0;

  while(exist==0&&position<numPages){

    k=mgmtData->pages[position].pageNum;

    if(pageNum==k){ 
      
      exist=1;

    }

    position++; //the position will be one more than real position when the loop ends

  }


  if(exist){

    position--;

    //if it exists in buffer pool, then increase the fix count, and set the passed PageHandle
    mgmtData->pages[position].pin_fix_count++;

    //set the page number, content, and other info
    page->pageNum=pageNum;
    page->data=mgmtData->pages[position].data;
    page->pin_fix_count=mgmtData->pages[position].pin_fix_count;
    page->dirty=mgmtData->pages[position].dirty;

    if (bm->strategy == RS_LRU) {
      mgmtData->LRU_Order[position] = g_tick++;
    }

    return RC_OK;

  }

  //here, will be implementing different strategies
  else{

    //if the queue is not full, add the element into the last position
      if(bm->numPages > page_count)
      {

        SM_PageHandle memPage;

        //read the page and store it at the position of tail
        memPage=mgmtData->pages[page_count].data; 

        RC ret = readBlock(pageNum, mgmtData->fileHandle, memPage); //read a page from disk to this position 
                                                              //in buffer pool
        if (ret != RC_OK) {
          return ret;
        }
        mgmtData->read_count++;


        //update other info, including the page number of this page, pin_fix_count, and dirty or not.
        mgmtData->pages[page_count].pageNum=pageNum;
        mgmtData->pages[page_count].pin_fix_count++;
        mgmtData->pages[page_count].dirty=0;


        //set the features of PageHandle that has been passed in the method
        page->pageNum=pageNum;
        page->data=mgmtData->pages[page_count].data;
        page->pin_fix_count=mgmtData->pages[page_count].pin_fix_count;
        page->dirty=mgmtData->pages[page_count].dirty;

        mgmtData->LRU_Order[page_count] = g_tick++;

        //increase the page_count in the mgmtData
        page_count++;

        mgmtData->page_count=page_count;


        return RC_OK;
      }
      //if the queue is full, use FIFO or LRU strategy
      else
      {
        //find the element with largest LRU_order number, and replace it with new element

        //find the position of this element
        int g, position, min_value, max_value;

        position = -1;
        for (g=0;g<page_count;g++){
          if (mgmtData->pages[g].pin_fix_count == 0) {
            position = g;
            break;
          }
        }

        if (position == -1) {
          return -1;
        }

        //find position to replace
        min_value=mgmtData->LRU_Order[0];

        for (g=0;g<page_count;g++){

          if(mgmtData->pages[g].pin_fix_count == 0 && mgmtData->LRU_Order[g]<=min_value)
          {
            min_value=mgmtData->LRU_Order[g];
            position=g;
          }
        }

        //increment the LRU_Order number for each element
        mgmtData->LRU_Order[position] = g_tick++;
        

        //rewrite later
        if(mgmtData->pages[position].dirty==1){
          writeBlock(mgmtData->pages[position].pageNum, mgmtData->fileHandle, mgmtData->pages[position].data);
          mgmtData->write_count++;
        }


        //pin_fix_count = 0

        //replace the element at the position we got
        SM_PageHandle memPage;

        //read the page and store it at the position of tail
        memPage=mgmtData->pages[position].data; 

        readBlock(pageNum, mgmtData->fileHandle, memPage); //read a page from disk to this position 
                                                              //in buffer pool

        mgmtData->read_count++;


        //update other info, including the page number of this page, pin_fix_count, and dirty or not.
        mgmtData->pages[position].pageNum=pageNum;
        mgmtData->pages[position].pin_fix_count = 1;  
        mgmtData->pages[position].dirty=0;


        //set the features of PageHandle that has been passed in the method
        page->pageNum=pageNum;
        page->data=mgmtData->pages[position].data;
        page->pin_fix_count=mgmtData->pages[position].pin_fix_count;
        page->dirty=mgmtData->pages[position].dirty;

        
        //since the queue is full, no need to update the page_count

        return RC_OK;
      }

  }

}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm){

  int i;
  BM_mgmtData *mgmtData=(BM_mgmtData *)(bm->mgmtData);


  PageNumber *fcontents = malloc(sizeof(PageNumber) * bm->numPages);


  for(i=0;i<bm->numPages;i++){
      fcontents[i] = mgmtData->pages[i].pageNum;
  }

  return fcontents;

}

bool *getDirtyFlags (BM_BufferPool *const bm){

  int i;

  BM_mgmtData *mgmtData=(BM_mgmtData *)(bm->mgmtData);

  bool *flags = malloc(sizeof(bool) * bm->numPages);

  for(i=0; i<bm->numPages; i++){
    if(mgmtData->pages[i].dirty == 1){
      flags[i] = true;
    }
    else{
      flags[i] = false;
    }

  }

  return flags;
}

int *getFixCounts (BM_BufferPool *const bm){

  int i;
  
  BM_mgmtData *mgmtData=(BM_mgmtData *)(bm->mgmtData);

  int *fcounts = malloc(sizeof(int) * bm->numPages);

  for(i=0; i<bm->numPages; i++)
  {
    fcounts[i]= mgmtData->pages[i].pin_fix_count;
  }

  return fcounts;

}

int getNumReadIO (BM_BufferPool *const bm){

  BM_mgmtData *mgmtData=(BM_mgmtData *)(bm->mgmtData);

  return mgmtData->read_count;
}

int getNumWriteIO (BM_BufferPool *const bm){
    BM_mgmtData *mgmtData=(BM_mgmtData *)(bm->mgmtData);

  return mgmtData->write_count;
}


