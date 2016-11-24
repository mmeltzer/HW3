Group Members: *Manojkumar Gaddam (mgaddam), Guoxuan Hao (ghao2), Martin Meltzer (mmeltze3), Sachet Misra (smisra8)

*group leader

No additional installations are required to execute the program. 

We did not create any additional files, error codes or methods.

The main data structure that we used is RM_MgmtInfo. Here is the structure: 

typedef struct RM_MgmtData {
  BM_BufferPool *bm;
  int recordSize;
  int numPageSlot;
  int tupleNumber;
  int *keyDataOffset;
  int *keyDataSize;
  int keyTableSize;
  
} RM_MgmtData;

This data structure stores information specific to individual records, in particular, a page slot number, a tuple number, 
and the size of the record.

Extra credit: 

