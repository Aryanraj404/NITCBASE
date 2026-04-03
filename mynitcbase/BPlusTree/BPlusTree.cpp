#include "BPlusTree.h"

#include <cstring>
#include "Buffer/BlockBuffer.h"


RecId BPlusTree::bPlusSearch(int relId,
                             char attrName[ATTR_SIZE],
                             Attribute attrVal,
                             int op,int *(comparisonCount)) {

    IndexId searchIndex;
    AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    int block, index;

    if (searchIndex.block == -1 && searchIndex.index == -1) {

        block = attrCatEntry.rootBlock;
        index = 0;

        if (block == -1) {
            return RecId{-1, -1};
        }

    } else {

        block = searchIndex.block;
        index = searchIndex.index + 1;

        IndLeaf leaf(block);
        HeadInfo leafHead;
        leaf.getHeader(&leafHead);

        if (index >= leafHead.numEntries) {
            block = leafHead.rblock;
            index = 0;

            if (block == -1) {
                return RecId{-1, -1};
            }
        }
    }


    while (StaticBuffer::getStaticBlockType(block) == IND_INTERNAL) {

        IndInternal internalBlk(block);
        HeadInfo intHead;
        internalBlk.getHeader(&intHead);

        InternalEntry intEntry;

        if (op == NE || op == LT || op == LE) {
            internalBlk.getEntry(&intEntry, 0);
            block = intEntry.lChild;

        } else {
            bool found = false;

            for (int i = 0; i < intHead.numEntries; i++) {
                internalBlk.getEntry(&intEntry, i);
                (*comparisonCount)++;
                int cmp = compareAttrs(intEntry.attrVal, attrVal,NUMBER);

                if ((op == EQ || op == GE) && cmp >= 0) {
                    block = intEntry.lChild;
                    found = true;
                    break;
                }
                if (op == GT && cmp > 0) {
                    block = intEntry.lChild;
                    found = true;
                    break;
                }
            }

            if (!found) {
                internalBlk.getEntry(&intEntry, intHead.numEntries - 1);
                block = intEntry.rChild;
            }
        }
    }

   
    while (block != -1) {

        IndLeaf leafBlk(block);
        HeadInfo leafHead;
        leafBlk.getHeader(&leafHead);

        Index leafEntry;

        while (index < leafHead.numEntries) {

            leafBlk.getEntry(&leafEntry, index);
            (*comparisonCount)++;
            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal,NUMBER);

            if (
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0)
            ) {
                
                IndexId newIndex{block, index};
                AttrCacheTable::setSearchIndex(relId, attrName, &newIndex);

                return RecId{leafEntry.block, leafEntry.slot};
            }

            else if ((op == EQ || op == LE || op == LT) && cmpVal > 0) {
                return RecId{-1, -1};
            }

            index++;
        }

        if (op != NE) break;

        block = leafHead.rblock;
        index = 0;
    }

    return RecId{-1, -1};
}
int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE]) {

    // ❌ cannot create index on system catalogs
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID) {
        return E_NOTPERMITTED;
    }

    // get attribute entry
    AttrCatEntry attrEntry;
    int status = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrEntry);
    if (status != SUCCESS) {
        return status;
    }

    // if index already exists
    if (attrEntry.rootBlock != -1) {
        return SUCCESS;
    }

    /****** Create new B+ Tree ******/

    // create root leaf block
    IndLeaf rootBlockBuf;
    int rootBlock = rootBlockBuf.getBlockNum();

    if (rootBlock == E_DISKFULL) {
        return E_DISKFULL;
    }

    // update rootBlock in cache
    attrEntry.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrEntry);

    // get relation catalog
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int block = relCatEntry.firstBlk;

    /***** Traverse all record blocks *****/
    while (block != -1) {

        RecBuffer recBuf(block);

        unsigned char slotMap[relCatEntry.numSlotsPerBlk];
        recBuf.getSlotMap(slotMap);

        for (int slot = 0; slot < relCatEntry.numSlotsPerBlk; slot++) {

            if (slotMap[slot] == SLOT_UNOCCUPIED) continue;

            Attribute record[relCatEntry.numAttrs];
            recBuf.getRecord(record, slot);

            // get attribute offset
            AttrCatEntry tempAttr;
            AttrCacheTable::getAttrCatEntry(relId, attrName, &tempAttr);

            int offset = tempAttr.offset;

            RecId recId{block, slot};

            int retVal = bPlusInsert(relId,
                                    attrName,
                                    record[offset],
                                    recId);

            if (retVal == E_DISKFULL) {
                return E_DISKFULL;
            }
        }

        // move to next block
        HeadInfo head;
        recBuf.getHeader(&head);
        block = head.rblock;
    }

    return SUCCESS;
}
int BPlusTree::bPlusDestroy(int rootBlockNum) {

    // ❌ invalid block number
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }

    // get block type
    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    // 🔹 LEAF NODE
    if (type == IND_LEAF) {

        IndLeaf leaf(rootBlockNum);

        // free block
        leaf.releaseBlock();

        return SUCCESS;
    }

    // 🔹 INTERNAL NODE
    else if (type == IND_INTERNAL) {

        IndInternal internalBlk(rootBlockNum);

        HeadInfo head;
        internalBlk.getHeader(&head);

        InternalEntry entry;

        // process first entry separately (for lChild)
        if (head.numEntries > 0) {
            internalBlk.getEntry(&entry, 0);

            // destroy leftmost child
            bPlusDestroy(entry.lChild);
        }

        // destroy all rChild (each unique)
        for (int i = 0; i < head.numEntries; i++) {
            internalBlk.getEntry(&entry, i);
            bPlusDestroy(entry.rChild);
        }

        // free current block
        internalBlk.releaseBlock();

        return SUCCESS;
    }

    // ❌ not an index block
    else {
        return E_INVALIDBLOCK;
    }
}
int BPlusTree::bPlusInsert(int relId,
                           char attrName[ATTR_SIZE],
                           Attribute attrVal,
                           RecId recId) {

    // get attribute entry
    AttrCatEntry attrEntry;
    int status = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrEntry);
    if (status != SUCCESS) {
        return status;
    }

    int blockNum = attrEntry.rootBlock;

    // ❌ no index exists
    if (blockNum == -1) {
        return E_NOINDEX;
    }

    // find leaf block
    int leafBlkNum = findLeafToInsert(blockNum, attrVal, attrEntry.attrType);

    // create index entry
    Index entry;
    entry.attrVal = attrVal;
    entry.block = recId.block;
    entry.slot = recId.slot;

    // insert into leaf
    int retVal = insertIntoLeaf(relId, attrName, leafBlkNum, entry);

    // ❌ disk full → rollback
    if (retVal == E_DISKFULL) {

        // destroy entire tree
        bPlusDestroy(attrEntry.rootBlock);

        // reset rootBlock
        attrEntry.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrEntry);

        return E_DISKFULL;
    }

    return SUCCESS;
}
int BPlusTree::findLeafToInsert(int rootBlock,
                                Attribute attrVal,
                                int attrType) {

    int blockNum = rootBlock;

    while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF) {

        IndInternal internalBlk(blockNum);

        HeadInfo head;
        internalBlk.getHeader(&head);

        InternalEntry entry;

        bool found = false;

        // search for first key >= attrVal
        for (int i = 0; i < head.numEntries; i++) {

            internalBlk.getEntry(&entry, i);

            int cmp = compareAttrs(entry.attrVal, attrVal, attrType);

            if (cmp >= 0) {
                // go to left child
                blockNum = entry.lChild;
                found = true;
                break;
            }
        }

        // if no key >= attrVal found → go rightmost
        if (!found) {
            internalBlk.getEntry(&entry, head.numEntries - 1);
            blockNum = entry.rChild;
        }
    }

    return blockNum;
}
int BPlusTree::insertIntoLeaf(int relId,
                              char attrName[ATTR_SIZE],
                              int blockNum,
                              Index indexEntry) {

    // get attribute entry
    AttrCatEntry attrEntry;
    int status = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrEntry);
    if (status != SUCCESS) return status;

    IndLeaf leaf(blockNum);

    HeadInfo blockHeader;
    leaf.getHeader(&blockHeader);

    // temp array
    Index indices[blockHeader.numEntries + 1];

    int i = 0, j = 0;
    bool inserted = false;

    // copy + insert in sorted order
    for (i = 0; i < blockHeader.numEntries; i++) {
        Index temp;
        leaf.getEntry(&temp, i);

        if (!inserted &&
            compareAttrs(indexEntry.attrVal, temp.attrVal, attrEntry.attrType) < 0) {

            indices[j++] = indexEntry;
            inserted = true;
        }

        indices[j++] = temp;
    }

    if (!inserted) {
        indices[j++] = indexEntry;
    }

    // ✅ CASE 1: NO SPLIT
    if (blockHeader.numEntries != MAX_KEYS_LEAF) {

        blockHeader.numEntries++;

        leaf.setHeader(&blockHeader);

        for (int k = 0; k < blockHeader.numEntries; k++) {
            leaf.setEntry(&indices[k], k);
        }

        return SUCCESS;
    }

    // ✅ CASE 2: SPLIT REQUIRED
    int newRightBlk = splitLeaf(blockNum, indices);

    if (newRightBlk == E_DISKFULL) {
        return E_DISKFULL;
    }

    // reload header (important)
    leaf.getHeader(&blockHeader);

    // NOT ROOT
    if (blockHeader.pblock != -1) {

        InternalEntry newEntry;
        newEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        newEntry.lChild = blockNum;
        newEntry.rChild = newRightBlk;

        int ret = insertIntoInternal(relId,
                                     attrName,
                                     blockHeader.pblock,
                                     newEntry);

        return ret;
    }

    // ROOT SPLIT
    else {
        int ret = createNewRoot(relId,
                               attrName,
                               indices[MIDDLE_INDEX_LEAF].attrVal,
                               blockNum,
                               newRightBlk);

        return ret;
    }
}
int BPlusTree::splitLeaf(int leafBlockNum, Index indices[]) {

    // new right block
    IndLeaf rightBlk;
    int rightBlkNum = rightBlk.getBlockNum();

    // existing left block
    IndLeaf leftBlk(leafBlockNum);
    int leftBlkNum = leafBlockNum;

    if (rightBlkNum == E_DISKFULL) {
        return E_DISKFULL;
    }

    HeadInfo leftHdr, rightHdr;

    leftBlk.getHeader(&leftHdr);
    rightBlk.getHeader(&rightHdr);

    // RIGHT BLOCK HEADER
    rightHdr.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    rightHdr.pblock = leftHdr.pblock;
    rightHdr.lblock = leftBlkNum;
    rightHdr.rblock = leftHdr.rblock;

    rightBlk.setHeader(&rightHdr);

    // LEFT BLOCK HEADER
    leftHdr.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    leftHdr.rblock = rightBlkNum;

    leftBlk.setHeader(&leftHdr);

    // copy entries
    for (int i = 0; i < (MAX_KEYS_LEAF + 1) / 2; i++) {
        leftBlk.setEntry(&indices[i], i);
    }

    for (int i = 0; i < (MAX_KEYS_LEAF + 1) / 2; i++) {
        rightBlk.setEntry(&indices[i + (MAX_KEYS_LEAF + 1) / 2], i);
    }

    return rightBlkNum;
}
int BPlusTree::insertIntoInternal(int relId,
                                  char attrName[ATTR_SIZE],
                                  int intBlockNum,
                                  InternalEntry intEntry) {

    // get attribute entry
    AttrCatEntry attrEntry;
    int status = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrEntry);
    if (status != SUCCESS) return status;

    IndInternal intBlk(intBlockNum);

    HeadInfo blockHeader;
    intBlk.getHeader(&blockHeader);

    InternalEntry internalEntries[blockHeader.numEntries + 1];

    int i = 0, j = 0;
    bool inserted = false;

    // copy + insert in sorted order
    for (i = 0; i < blockHeader.numEntries; i++) {
        InternalEntry temp;
        intBlk.getEntry(&temp, i);

        if (!inserted &&
            compareAttrs(intEntry.attrVal, temp.attrVal, attrEntry.attrType) < 0) {

            internalEntries[j++] = intEntry;
            inserted = true;

            // fix child pointer
            temp.lChild = intEntry.rChild;
        }

        internalEntries[j++] = temp;
    }

    if (!inserted) {
        internalEntries[j++] = intEntry;
    }

    // ✅ CASE 1: NO SPLIT
    if (blockHeader.numEntries != MAX_KEYS_INTERNAL) {

        blockHeader.numEntries++;
        intBlk.setHeader(&blockHeader);

        for (int k = 0; k < blockHeader.numEntries; k++) {
            intBlk.setEntry(&internalEntries[k], k);
        }

        return SUCCESS;
    }

    // ✅ CASE 2: SPLIT
    int newRightBlk = splitInternal(intBlockNum, internalEntries);

    if (newRightBlk == E_DISKFULL) {
        bPlusDestroy(intEntry.rChild);
        return E_DISKFULL;
    }

    intBlk.getHeader(&blockHeader);

    // NOT ROOT
    if (blockHeader.pblock != -1) {

        InternalEntry newEntry;
        newEntry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        newEntry.lChild = intBlockNum;
        newEntry.rChild = newRightBlk;

        return insertIntoInternal(relId,
                                  attrName,
                                  blockHeader.pblock,
                                  newEntry);
    }

    // ROOT SPLIT
    else {
        return createNewRoot(relId,
                             attrName,
                             internalEntries[MIDDLE_INDEX_INTERNAL].attrVal,
                             intBlockNum,
                             newRightBlk);
    }
}
int BPlusTree::splitInternal(int intBlockNum,
                             InternalEntry internalEntries[]) {

    IndInternal rightBlk;
    int rightBlkNum = rightBlk.getBlockNum();

    IndInternal leftBlk(intBlockNum);
    int leftBlkNum = intBlockNum;

    if (rightBlkNum == E_DISKFULL) {
        return E_DISKFULL;
    }

    HeadInfo leftHdr, rightHdr;

    leftBlk.getHeader(&leftHdr);
    rightBlk.getHeader(&rightHdr);

    // RIGHT HEADER
    rightHdr.numEntries = MAX_KEYS_INTERNAL / 2;
    rightHdr.pblock = leftHdr.pblock;

    rightBlk.setHeader(&rightHdr);

    // LEFT HEADER
    leftHdr.numEntries = MAX_KEYS_INTERNAL / 2;

    leftBlk.setHeader(&leftHdr);

    // copy LEFT entries (0 → 49)
    for (int i = 0; i < MAX_KEYS_INTERNAL / 2; i++) {
        leftBlk.setEntry(&internalEntries[i], i);
    }

    // copy RIGHT entries (51 → 100)
    for (int i = 0; i < MAX_KEYS_INTERNAL / 2; i++) {
        rightBlk.setEntry(&internalEntries[i + MAX_KEYS_INTERNAL / 2 + 1], i);
    }

    // update children parent pointer
    for (int i = 0; i < rightHdr.numEntries; i++) {

        InternalEntry entry;
        rightBlk.getEntry(&entry, i);

        // update lChild
        BlockBuffer child1(entry.lChild);
        HeadInfo chHdr1;
        child1.getHeader(&chHdr1);
        chHdr1.pblock = rightBlkNum;
        child1.setHeader(&chHdr1);

        // update rChild
        BlockBuffer child2(entry.rChild);
        HeadInfo chHdr2;
        child2.getHeader(&chHdr2);
        chHdr2.pblock = rightBlkNum;
        child2.setHeader(&chHdr2);
    }

    return rightBlkNum;
}
int BPlusTree::createNewRoot(int relId,
                             char attrName[ATTR_SIZE],
                             Attribute attrVal,
                             int lChild,
                             int rChild) {

    // get attribute entry
    AttrCatEntry attrEntry;
    int status = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrEntry);
    if (status != SUCCESS) return status;

    // create new internal block
    IndInternal newRootBlk;
    int newRootBlkNum = newRootBlk.getBlockNum();

    // ❌ disk full
    if (newRootBlkNum == E_DISKFULL) {
        bPlusDestroy(rChild);
        return E_DISKFULL;
    }

    // set header
    HeadInfo header;
    newRootBlk.getHeader(&header);

    header.numEntries = 1;
    header.pblock = -1;

    newRootBlk.setHeader(&header);

    // create first entry
    InternalEntry entry;
    entry.lChild = lChild;
    entry.attrVal = attrVal;
    entry.rChild = rChild;

    newRootBlk.setEntry(&entry, 0);

    // update left child parent
    BlockBuffer leftChildBlk(lChild);
    HeadInfo leftHdr;
    leftChildBlk.getHeader(&leftHdr);
    leftHdr.pblock = newRootBlkNum;
    leftChildBlk.setHeader(&leftHdr);

    // update right child parent
    BlockBuffer rightChildBlk(rChild);
    HeadInfo rightHdr;
    rightChildBlk.getHeader(&rightHdr);
    rightHdr.pblock = newRootBlkNum;
    rightChildBlk.setHeader(&rightHdr);

    // update rootBlock in cache
    attrEntry.rootBlock = newRootBlkNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrEntry);

    return SUCCESS;
}