#include "BlockBuffer.h"
#include "StaticBuffer.h"

#include "Disk_Class/Disk.h"
#include "define/constants.h" 
#include <cstring>


BlockBuffer::BlockBuffer(int blockNum) {
    this->blockNum = blockNum;
}
RecBuffer::RecBuffer(int blockNum) : BlockBuffer(blockNum) {}

int BlockBuffer::getHeader(struct HeadInfo *head) {

    unsigned char *bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS) {
        return ret;
    }

    memcpy(&head->numEntries, bufferPtr + 16, 4);
    memcpy(&head->numAttrs,   bufferPtr + 20, 4);
    memcpy(&head->lblock,     bufferPtr + 8, 4);
    memcpy(&head->rblock,     bufferPtr + 12, 4);
    memcpy(&head->numSlots,   bufferPtr + 24, 4);

    return SUCCESS;
}


int RecBuffer::getRecord(union Attribute *rec, int slotNum) {

    struct HeadInfo head;
    getHeader(&head);

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS) {
        return ret;
    }

    if (slotNum < 0 || slotNum >= slotCount) {
        return E_OUTOFBOUND;
    }
    int recordSize = attrCount * sizeof(Attribute);
    int slotMapSize = slotCount;

    unsigned char *slotPointer =
        bufferPtr + HEADER_SIZE + slotMapSize + (slotNum * recordSize);

    memcpy(rec, slotPointer, recordSize);

    return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {

    unsigned char *bufferPtr;

    /* Step 1: Bring block into buffer */
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS) {
        return ret;
    }

    /* Step 2: Read header */
    HeadInfo head;
    getHeader(&head);

    int numAttrs = head.numAttrs;
    int numSlots = head.numSlots;

    /* Step 3: Slot range check */
    if (slotNum < 0 || slotNum >= numSlots) {
        return E_OUTOFBOUND;
    }

    /* Step 4: Compute record size */
    int recordSize = numAttrs * sizeof(Attribute);

    /* Step 5: Compute offset to correct slot */
    unsigned char *recordPtr =
        bufferPtr + HEADER_SIZE + (numSlots) + (slotNum * recordSize);

    /* Step 6: Copy record into buffer */
    memcpy(recordPtr, rec, recordSize);

    /* Step 7: Mark dirty bit */
    StaticBuffer::setDirtyBit(this->blockNum);

    return SUCCESS;
}


int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {

    /* Step 1: Check if block already present in buffer */
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    /* ---------------------------------------------
       CASE 1: Block is already in buffer
       --------------------------------------------- */
    if (bufferNum != E_BLOCKNOTINBUFFER) {

        /* Reset timestamp of this buffer to 0
           and increment timestamp of all others */
        for (int i = 0; i < BUFFER_CAPACITY; i++) {
            if (StaticBuffer::metainfo[i].free == false) {

                if (i == bufferNum)
                    StaticBuffer::metainfo[i].timeStamp = 0;
                else
                    StaticBuffer::metainfo[i].timeStamp++;
            }
        }
    }

    /* ---------------------------------------------
       CASE 2: Block is NOT in buffer → bring it in
       --------------------------------------------- */
    else {

        /* Get a free buffer slot using LRU */
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        /* If invalid block number */
        if (bufferNum == E_OUTOFBOUND) {
            return E_OUTOFBOUND;
        }

        /* Load block from disk into buffer */
        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
    }

    /* Step 3: Return pointer to buffer block */
    *buffPtr = StaticBuffer::blocks[bufferNum];

    return SUCCESS;
}

int RecBuffer::getSlotMap(unsigned char *slotMap) {

    unsigned char *bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS) {
        return ret;
    }

    HeadInfo head;
    getHeader(&head);

    int slotCount = head.numSlots;

   
    unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

    memcpy(slotMap, slotMapInBuffer, slotCount);

    return SUCCESS;
}
int compareAttrs(union Attribute attr1,
                 union Attribute attr2,
                 int attrType) {

    double diff;


    if (attrType == STRING) {
        diff = strcmp(attr1.sVal, attr2.sVal);
    }

    else {
        diff = attr1.nVal - attr2.nVal;
    }

    if (diff > 0)
        return 1;
    if (diff < 0)
        return -1;
    return 0;
}

int BlockBuffer::setHeader(struct HeadInfo *head){

    unsigned char *bufferPtr;
    int ret =  loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret!=SUCCESS)
        return ret;

    struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;


    bufferHeader->blockType = head->blockType;
    bufferHeader->pblock     = head->pblock;
    bufferHeader->lblock     = head->lblock;
    bufferHeader->rblock     = head->rblock;
    bufferHeader->numEntries = head->numEntries;
    bufferHeader->numAttrs   = head->numAttrs;
    bufferHeader->numSlots   = head->numSlots;

    ret = StaticBuffer::setDirtyBit(this->blockNum);
    if(ret!=SUCCESS)
        return ret;

    return SUCCESS;
}
int BlockBuffer::setBlockType(int blockType){

    unsigned char *bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;


    *((int32_t *)bufferPtr) = blockType;


     StaticBuffer::blockAllocMap[this->blockNum] = blockType;

    ret = StaticBuffer::setDirtyBit(this->blockNum);
    if (ret != SUCCESS)
        return ret;

    return SUCCESS;
}
int BlockBuffer::getFreeBlock(int blockType) {

    int freeBlock = -1;

    for (int i = 4; i < DISK_BLOCKS; i++) {  
        if (StaticBuffer::blockAllocMap[i] == UNUSED_BLK) {
            freeBlock = i;
            break;
        }
    }

    if (freeBlock == -1)
        return E_DISKFULL;


    this->blockNum = freeBlock;

    int bufferNum = StaticBuffer::getFreeBuffer(freeBlock);
    if (bufferNum < 0)
        return bufferNum;
    
    struct HeadInfo head;
    head.pblock     = -1;
    head.lblock     = -1;
    head.rblock     = -1;
    head.numEntries = 0;
    head.numAttrs   = 0;
    head.numSlots   = 0;

    setHeader(&head);


    setBlockType(blockType);

    return freeBlock;
}
RecBuffer::RecBuffer() : BlockBuffer('R'){}

BlockBuffer::BlockBuffer(char blockType) {


    int ret = getFreeBlock(blockType);

    if (ret >= 0) {
        this->blockNum = ret;
    }
    else {
        this->blockNum = ret;
    }
}
int RecBuffer::setSlotMap(unsigned char *slotMap) {

    unsigned char *bufferPtr;

    int status = loadBlockAndGetBufferPtr(&bufferPtr);
    if(status != SUCCESS)
        return status;

    HeadInfo head;
    status = getHeader(&head);
    if(status != SUCCESS)
        return status;

    int numSlots = head.numSlots;

    
    memcpy(bufferPtr + HEADER_SIZE,slotMap,numSlots);

    status = StaticBuffer::setDirtyBit(blockNum);
    if(status != SUCCESS)
        return status;

    return SUCCESS;
}
int BlockBuffer::getBlockNum(){

    return blockNum;
}