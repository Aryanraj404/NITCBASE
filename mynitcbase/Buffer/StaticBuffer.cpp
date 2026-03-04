#include "StaticBuffer.h"
#include "define/constants.h"
#include "Disk_Class/Disk.h"
#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Cache/RelCacheTable.h"
#include "Cache/AttrCacheTable.h"
#include "define/constants.h"
#include "FrontendInterface/FrontendInterface.h"
#include "FrontendInterface/RegexHandler.h"
#include <string.h>
#include <cstdio>

unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];
unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

StaticBuffer::StaticBuffer() {


int bytesCopied = 0;

for (int i = 0; bytesCopied < DISK_BLOCKS; i++) {
    unsigned char tempBlock[BLOCK_SIZE];
    Disk::readBlock(tempBlock, i);

    int bytesToCopy = BLOCK_SIZE;
    if (bytesCopied + BLOCK_SIZE > DISK_BLOCKS)
        bytesToCopy = DISK_BLOCKS - bytesCopied;

    memcpy(blockAllocMap + bytesCopied, tempBlock, bytesToCopy);
    bytesCopied += bytesToCopy;
}


    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        metainfo[i].free = true;
        metainfo[i].blockNum = -1;
        metainfo[i].dirty = false;
        metainfo[i].timeStamp = -1;
    }
}

StaticBuffer::~StaticBuffer() {


int bytesCopied = 0;

for (int i = 0; bytesCopied < DISK_BLOCKS; i++) {

    unsigned char tempBlock[BLOCK_SIZE];

    int bytesToCopy = BLOCK_SIZE;
    if (bytesCopied + BLOCK_SIZE > DISK_BLOCKS)
        bytesToCopy = DISK_BLOCKS - bytesCopied;

    memcpy(tempBlock, blockAllocMap + bytesCopied, bytesToCopy);
    Disk::writeBlock(tempBlock, i);

    bytesCopied += bytesToCopy;
}

    for (int i = 0; i < BUFFER_CAPACITY; i++){
        if(metainfo[i].free == false){
            if(metainfo[i].dirty == true){
                Disk::writeBlock(blocks[i], metainfo[i].blockNum);

                metainfo[i].dirty = false;
            }
        }
    }
}




int StaticBuffer::getBufferNum(int blockNum) {

    if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }

    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        if (!metainfo[i].free && metainfo[i].blockNum == blockNum) {
            return i;
        }
    }

    return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::getFreeBuffer(int blockNum) {

   
    if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }


    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        if (metainfo[i].free == false) {
            metainfo[i].timeStamp++;
        }
    }

    int bufferNum = -1;


    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        if (metainfo[i].free == true) {
            bufferNum = i;
            break;
        }
    }


    if (bufferNum == -1) {

        int maxTime = -1;
        int lruIndex = -1;

     
        for (int i = 0; i < BUFFER_CAPACITY; i++) {
            if (metainfo[i].timeStamp > maxTime) {
                maxTime = metainfo[i].timeStamp;
                lruIndex = i;
            }
        }

       
        if (metainfo[lruIndex].dirty == true) {
            Disk::writeBlock(blocks[lruIndex], metainfo[lruIndex].blockNum);
        }

        bufferNum = lruIndex;
    }


    metainfo[bufferNum].free = false;
    metainfo[bufferNum].dirty = false;
    metainfo[bufferNum].blockNum = blockNum;
    metainfo[bufferNum].timeStamp = 0;

    return bufferNum;
}

int StaticBuffer::setDirtyBit(int blockNum) {

    /* Find buffer slot containing this block */
    int bufferNum = getBufferNum(blockNum);

    /* Block not present */
    if (bufferNum == E_BLOCKNOTINBUFFER) {
        return E_BLOCKNOTINBUFFER;
    }

    /* Out of bound */
    if (bufferNum == E_OUTOFBOUND) {
        return E_OUTOFBOUND;
    }

    /* Mark dirty */
    metainfo[bufferNum].dirty = true;

    return SUCCESS;
}


