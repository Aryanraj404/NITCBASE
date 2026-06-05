#include "BPlusTree.h"

#include <cstring>
#include <cstdio>
RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
    IndexId searchIndex;
    AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    int block, index;

    if (searchIndex.block == -1 && searchIndex.index == -1)
    {
        block = attrCatEntry.rootBlock;
        index = 0;

        if (block == -1)
            return RecId{-1, -1};
    }
    else
    {

        block = searchIndex.block;
        index = searchIndex.index + 1;

        IndLeaf leaf(block);

        HeadInfo leafHead;
        leaf.BlockBuffer::getHeader(&leafHead);

        if (index >= leafHead.numEntries)
        {
            block = leafHead.rblock;
            index = 0;
            if (block == -1)
            {
                return RecId{-1, -1};
            }
        }
    }

    while (StaticBuffer::getStaticBlockType(block) == IND_INTERNAL)
    {
        IndInternal internalBlk(block);

        HeadInfo intHead;
        internalBlk.BlockBuffer::getHeader(&intHead);

        InternalEntry intEntry;

        if (op == NE || op == LT || op == LE)
        {
            internalBlk.IndInternal::getEntry(&intEntry, 0);
            block = intEntry.lChild;
        }
        else
        {
            bool flag = true;
            for (int i = 0; i < intHead.numEntries; i++)
            {
                internalBlk.IndInternal::getEntry(&intEntry, i);
                int cmpVal = compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType);
                if (((op == EQ || op == GE) && cmpVal >= 0) || (op == GT && cmpVal > 0))
                {
                    block = intEntry.lChild;
                    flag = false;
                    break;
                }
            }
            if (flag)
            {
                internalBlk.getEntry(&intEntry, intHead.numEntries - 1);
                block = intEntry.rChild;
            }
        }
    }

    while (block != -1)
    {
        IndLeaf leafBlk(block);
        HeadInfo leafHead;
        leafBlk.getHeader(&leafHead);

        Index leafEntry;

        while (index < leafHead.numEntries)
        {
            leafBlk.getEntry(&leafEntry, index);
            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType);

            if (
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0))
            {
                IndexId newIndex{block, index};
                AttrCacheTable::setSearchIndex(relId, attrName, &newIndex);
                return RecId{leafEntry.block, leafEntry.slot};
            }
            else if ((op == EQ || op == LE || op == LT) && cmpVal > 0)
            {
                return RecId{-1, -1};
            }

            ++index;
        }

        if (op != NE)
        {
            break;
        }

        block = leafHead.rblock;
        index = 0;
    }

    return RecId{-1, -1};
}

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE])
{
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
        return E_NOTPERMITTED;

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret != SUCCESS)
        return ret;

    if (attrCatEntry.rootBlock != -1)
    {
        return SUCCESS;
    }

    IndLeaf rootBlockBuf;
    int rootBlock = rootBlockBuf.getBlockNum();

    if (rootBlock == E_DISKFULL)
    {
        return E_DISKFULL;
    }

    attrCatEntry.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int block = relCatEntry.firstBlk;
    while (block != -1)
    {
        RecBuffer recBuffer(block);

        unsigned char slotMap[relCatEntry.numSlotsPerBlk];
        recBuffer.getSlotMap(slotMap);

        for (int i = 0; i < relCatEntry.numSlotsPerBlk; i++)
            if (slotMap[i] == SLOT_OCCUPIED)
            {
                Attribute record[relCatEntry.numAttrs];
                recBuffer.getRecord(record, i);

                RecId recId{block, i};
                int ret = bPlusInsert(relId, attrName, record[attrCatEntry.offset], recId);
                if (ret != SUCCESS)
                {
                    return ret;
                }
            }

        struct HeadInfo head;
        recBuffer.getHeader(&head);
        block = head.rblock;
    }

    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum)
{
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS)
    {
        return E_OUTOFBOUND;
    }

    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    if (type == IND_LEAF)
    {
        IndLeaf leafBlk(rootBlockNum);
        leafBlk.releaseBlock();

        return SUCCESS;
    }
    else if (type == IND_INTERNAL)
    {
        IndInternal internalBlk(rootBlockNum);
        struct HeadInfo head;
        internalBlk.getHeader(&head);

        InternalEntry internalEntry;
        internalBlk.IndInternal::getEntry(&internalEntry, 0);
        if (internalEntry.lChild != -1)
            bPlusDestroy(internalEntry.lChild);
        for (int i = 0; i < head.numEntries; i++)
        {
            internalBlk.IndInternal::getEntry(&internalEntry, i);
            if (internalEntry.rChild != -1)
                bPlusDestroy(internalEntry.rChild);
        }
        internalBlk.releaseBlock();

        return SUCCESS;
    }
    else
    {
        return E_INVALIDBLOCK;
    }
}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId)
{
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret != SUCCESS)
        return ret;

    int rootBlk = attrCatEntry.rootBlock;
    if (rootBlk == -1)
        return E_NOINDEX;
    
    int leafBlkNum = findLeafToInsert(rootBlk, attrVal, attrCatEntry.attrType);

    Index entry{
        .attrVal = attrVal,
        .block = recId.block,
        .slot = recId.slot};
    ret = insertIntoLeaf(relId, attrName, leafBlkNum, entry);
    if (ret == E_DISKFULL)
    {
        bPlusDestroy(rootBlk);

        attrCatEntry.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

        return E_DISKFULL;
    }

    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType)
{
    int blockNum = rootBlock;

    while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF)
    {
        IndInternal internalBlk(blockNum);
        HeadInfo head;
        internalBlk.BlockBuffer::getHeader(&head);
        
        InternalEntry internalEntry;
        bool flag = true;
        for (int i = 0; i < head.numEntries; i++)
        {
            internalBlk.IndInternal::getEntry(&internalEntry, i);
            int cmpVal = compareAttrs(internalEntry.attrVal, attrVal, attrType);
            if (cmpVal >= 0)
            {
                blockNum = internalEntry.lChild;
                flag = false;
                break;
            }
        }
        if (flag)
        {
            internalBlk.IndInternal::getEntry(&internalEntry, head.numEntries - 1);
            blockNum = internalEntry.rChild;
        }
    }

    return blockNum;
}

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry)
{
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret != SUCCESS)
        return ret;

    IndLeaf leafBlk(blockNum);
    HeadInfo blockHeader;
    leafBlk.BlockBuffer::getHeader(&blockHeader);
    
    Index indices[blockHeader.numEntries + 1];
    bool flag = true;
    for (int i = 0, j = 0; i < blockHeader.numEntries; i++)
    {
        Index entry;
        leafBlk.IndLeaf::getEntry(&entry, i);
        int cmpVal = compareAttrs(entry.attrVal, indexEntry.attrVal, attrCatEntry.attrType);
        if (flag && cmpVal >= 0)
        {
            indices[j++] = indexEntry;
            flag = false;
        }
        indices[j++] = entry;
    }
    if (flag)
    {
        indices[blockHeader.numEntries] = indexEntry;
    }
    

    if (blockHeader.numEntries != MAX_KEYS_LEAF)
    {
        blockHeader.numEntries++;
        leafBlk.BlockBuffer::setHeader(&blockHeader);

        for (int i = 0; i < blockHeader.numEntries; i++)
        {
            leafBlk.IndLeaf::setEntry(&indices[i], i);
        }

        return SUCCESS;
    }

    int newRightBlk = splitLeaf(blockNum, indices);
    if (newRightBlk == E_DISKFULL)
        return E_DISKFULL;

    if (blockHeader.pblock != -1)
    {
        InternalEntry internalEntry;
        internalEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        internalEntry.lChild = blockNum;
        internalEntry.rChild = newRightBlk;
        int ret = insertIntoInternal(relId, attrName, blockHeader.pblock, internalEntry);
        if (ret != SUCCESS)
            return ret;
    }
    else
    {
        int ret = createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, blockNum, newRightBlk);
        if (ret != SUCCESS)
            return ret;
    }

    return SUCCESS;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[])
{
    IndLeaf rightBlk;
    IndLeaf leftBlk(leafBlockNum);
    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = leftBlk.getBlockNum();

    if (rightBlkNum == E_DISKFULL)
    {
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    leftBlk.BlockBuffer::getHeader(&leftBlkHeader);
    rightBlk.BlockBuffer::getHeader(&rightBlkHeader);

    rightBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlkHeader.lblock = leftBlkNum;
    rightBlkHeader.rblock = leftBlkHeader.rblock;
    rightBlk.BlockBuffer::setHeader(&rightBlkHeader);
    if (rightBlkHeader.rblock != -1) {
        IndLeaf nextBlk(rightBlkHeader.rblock);
        HeadInfo nextHeader;
        nextBlk.getHeader(&nextHeader);
        nextHeader.lblock = rightBlkNum;
        nextBlk.setHeader(&nextHeader);
    }

    leftBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    leftBlkHeader.rblock = rightBlkNum;
    leftBlk.BlockBuffer::setHeader(&leftBlkHeader);

    for (int i = 0; i < leftBlkHeader.numEntries; i++)
    {
        int ret = leftBlk.IndLeaf::setEntry(&indices[i], i);
        if (ret != SUCCESS)
            return ret;
    }
    for (int i = 0; i < rightBlkHeader.numEntries; i++)
    {
        int ret = rightBlk.IndLeaf::setEntry(&indices[i + leftBlkHeader.numEntries], i);
        if (ret != SUCCESS)
            return ret;
    }

    return rightBlkNum;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry)
{
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret != SUCCESS)
        return ret;
        
    IndInternal intBlk(intBlockNum);
    HeadInfo blockHeader;
    intBlk.BlockBuffer::getHeader(&blockHeader);
    // load blockHeader with header of intBlk using BlockBuffer::getHeader().

    // declare internalEntries to store all existing entries + the new entry
    bool flag = true;
    InternalEntry internalEntries[blockHeader.numEntries + 1];
    int idx = -1;
    for (int i = 0, j = 0; i < blockHeader.numEntries; i++)
    {
        InternalEntry entry;
        intBlk.IndInternal::getEntry(&entry, i);
        int cmpVal = compareAttrs(entry.attrVal, intEntry.attrVal, attrCatEntry.attrType);
        if (flag && cmpVal >= 0)
        {
            idx = j;
            internalEntries[j++] = intEntry;
            flag = false;
        }
        internalEntries[j++] = entry;
    }
    if (flag)
    {
        idx = blockHeader.numEntries;
        internalEntries[blockHeader.numEntries] = intEntry;
    }

    if(idx > 0){
        internalEntries[idx - 1].rChild = intEntry.lChild;
    }
    if(idx < blockHeader.numEntries){
        internalEntries[idx + 1].lChild = intEntry.rChild;
    }
    
    if (blockHeader.numEntries != MAX_KEYS_INTERNAL)
    {
        blockHeader.numEntries++;
        intBlk.BlockBuffer::setHeader(&blockHeader);
        
        for (int i = 0; i < blockHeader.numEntries; i++)
        {
            intBlk.IndInternal::setEntry(&internalEntries[i], i);
        }

        return SUCCESS;
    }

    int newRightBlk = splitInternal(intBlockNum, internalEntries);

    if (newRightBlk == E_DISKFULL)
    {
        bPlusDestroy(intEntry.rChild);
        return E_DISKFULL;
    }

    if (blockHeader.pblock != -1)
    {
        InternalEntry newInternalEntry;
        newInternalEntry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        newInternalEntry.lChild = intBlockNum;
        newInternalEntry.rChild = newRightBlk;

        return insertIntoInternal(relId, attrName, blockHeader.pblock, newInternalEntry);

    }
    else
    {
        return createNewRoot(relId, attrName, internalEntries[MIDDLE_INDEX_INTERNAL].attrVal, intBlockNum, newRightBlk);

    }
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[])
{
    IndInternal rightBlk;
    IndInternal leftBlk(intBlockNum);

    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = leftBlk.getBlockNum();

    if (rightBlkNum == E_DISKFULL)
        return E_DISKFULL;
    
    HeadInfo leftBlkHeader, rightBlkHeader;

    leftBlk.BlockBuffer::getHeader(&leftBlkHeader);
    rightBlk.BlockBuffer::getHeader(&rightBlkHeader);

    rightBlkHeader.numEntries = (MAX_KEYS_INTERNAL) / 2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlk.BlockBuffer::setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = (MAX_KEYS_INTERNAL) / 2;
    leftBlk.BlockBuffer::setHeader(&leftBlkHeader);

    for (int i = 0; i < leftBlkHeader.numEntries; i++)
    {
        leftBlk.IndInternal::setEntry(&internalEntries[i], i);
    }
    for (int i = 0; i < rightBlkHeader.numEntries; i++)
    {
        rightBlk.IndInternal::setEntry(&internalEntries[i + MIDDLE_INDEX_INTERNAL + 1], i);
    }

    int type = StaticBuffer::getStaticBlockType(internalEntries[0].lChild);

    BlockBuffer firstChild(internalEntries[MIDDLE_INDEX_INTERNAL + 1].lChild);
    HeadInfo fHeader;
    firstChild.getHeader(&fHeader);
    fHeader.pblock = rightBlkNum;
    firstChild.setHeader(&fHeader);

    for (int i = 0; i < rightBlkHeader.numEntries; i++)
    {
        BlockBuffer childBlk(internalEntries[i + MIDDLE_INDEX_INTERNAL + 1].rChild);
        HeadInfo childHeader;
        childBlk.BlockBuffer::getHeader(&childHeader);
        childHeader.pblock = rightBlkNum;
        childBlk.BlockBuffer::setHeader(&childHeader);
    }

    return rightBlkNum;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild)
{
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);


    IndInternal newRootBlk;

    int newRootBlkNum = newRootBlk.getBlockNum();

    if (newRootBlkNum == E_DISKFULL)
    {
        bPlusDestroy(rChild);
        return E_DISKFULL;
    }

    HeadInfo newRootHeader;
    newRootBlk.BlockBuffer::getHeader(&newRootHeader);
    newRootHeader.numEntries = 1;
    newRootBlk.BlockBuffer::setHeader(&newRootHeader);

    InternalEntry internalEntry;
    internalEntry.attrVal = attrVal;
    internalEntry.lChild = lChild;
    internalEntry.rChild = rChild;
    newRootBlk.IndInternal::setEntry(&internalEntry, 0);

    BlockBuffer leftChildBlk(lChild);
    BlockBuffer rightChildBlk(rChild);

    HeadInfo leftChildHeader, rightChildHeader;
    leftChildBlk.BlockBuffer::getHeader(&leftChildHeader);
    rightChildBlk.BlockBuffer::getHeader(&rightChildHeader);

    leftChildHeader.pblock = newRootBlkNum;
    rightChildHeader.pblock = newRootBlkNum;

    leftChildBlk.BlockBuffer::setHeader(&leftChildHeader);
    rightChildBlk.BlockBuffer::setHeader(&rightChildHeader);

    attrCatEntry.rootBlock = newRootBlkNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
    return SUCCESS;
}
