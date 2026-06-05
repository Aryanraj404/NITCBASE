#include "BlockAccess.h"

#include <cstring>
#include <cstdlib>
#include <cstdio>

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op)
{
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    int block, slot;

    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);
        block = relCatEntry.firstBlk;
        slot = 0;
    }
    else
    {
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    while (block != -1)
    {
        RecBuffer recBuffer(block);

        HeadInfo header;
        recBuffer.getHeader(&header);

        Attribute rec[header.numAttrs];
        recBuffer.getRecord(rec, slot);

        unsigned char slotMap[header.numSlots];
        recBuffer.getSlotMap(slotMap);

        if (slot >= header.numSlots)
        {
            block = header.rblock;
            slot = 0;
            continue;
        }
        else if (slotMap[slot] == SLOT_UNOCCUPIED)
        {
            slot++;
            continue;
        }

        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);
        union Attribute attr = rec[attrCatBuf.offset];

        int cmpVal;
        cmpVal = compareAttrs(attr, attrVal, attrCatBuf.attrType);

        if (
            (op == NE && cmpVal != 0) || // if op is "not equal to"
            (op == LT && cmpVal < 0) ||  // if op is "less than"
            (op == LE && cmpVal <= 0) || // if op is "less than or equal to"
            (op == EQ && cmpVal == 0) || // if op is "equal to"
            (op == GT && cmpVal > 0) ||  // if op is "greater than"
            (op == GE && cmpVal >= 0)    // if op is "greater than or equal to"
        )
        {
            RecId hitId = {block, slot};
            RelCacheTable::setSearchIndex(relId, &hitId);
            return RecId{block, slot};
        }

        slot++;
    }

    return RecId{-1, -1};
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute newRelationName;
    strcpy(newRelationName.sVal, newName);

    RecId recId = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, newRelationName, EQ);
    if (recId.block != -1 && recId.slot != -1)
    {
        return E_RELEXIST;
    }

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute oldRelationName;
    strcpy(oldRelationName.sVal, oldName);

    recId = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, oldRelationName, EQ);
    if (recId.block == -1 || recId.slot == -1)
    {
        return E_RELNOTEXIST;
    }

    RecBuffer relCatBuffer(recId.block);
    Attribute rec[RELCAT_NO_ATTRS];
    relCatBuffer.getRecord(rec, recId.slot);

    strcpy(rec[RELCAT_REL_NAME_INDEX].sVal, newName);
    relCatBuffer.setRecord(rec, recId.slot);

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    while (true)
    {
        RecId attrRecId = linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);

        if (attrRecId.block == -1)
            break;

        RecBuffer attrCatBuffer(attrRecId.block);
        Attribute attrCatRec[ATTRCAT_NO_ATTRS];
        attrCatBuffer.getRecord(attrCatRec, attrRecId.slot);

        strcpy(attrCatRec[ATTRCAT_REL_NAME_INDEX].sVal, newName);
        attrCatBuffer.setRecord(attrCatRec, attrRecId.slot);
    }

    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    RecId recId = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameAttr, EQ);
    if (recId.block == -1 || recId.slot == -1)
    {
        return E_RELNOTEXIST;
    }

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    while (true)
    {
        RecId recId = linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);

        if (recId.block == -1 && recId.slot == -1)
            break;

        RecBuffer attrCatBuffer(recId.block);
        Attribute rec[ATTRCAT_NO_ATTRS];
        attrCatBuffer.getRecord(rec, recId.slot);

        if (strcmp(rec[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0)
        {
            attrToRenameRecId.block = recId.block;
            attrToRenameRecId.slot = recId.slot;
        }

        if (strcmp(rec[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0)
            return E_ATTREXIST;
    }

    if (attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1)
        return E_ATTRNOTEXIST;

    RecBuffer attrCatBuffer(attrToRenameRecId.block);
    Attribute rec[ATTRCAT_NO_ATTRS];
    attrCatBuffer.getRecord(rec, attrToRenameRecId.slot);

    strcpy(rec[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
    attrCatBuffer.setRecord(rec, attrToRenameRecId.slot);

    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record)
{
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int blockNum = relCatEntry.firstBlk;

    RecId rec_id = {-1, -1};

    int numOfSlots = relCatEntry.numSlotsPerBlk;
    int numOfAttributes = relCatEntry.numAttrs;

    int prevBlockNum = -1;

    while (blockNum != -1)
    {
        RecBuffer recBuffer(blockNum);
        HeadInfo header;
        recBuffer.getHeader(&header);
        unsigned char slotMap[numOfSlots];
        recBuffer.getSlotMap(slotMap);

        for (int i = 0; i < numOfSlots; ++i)
        {
            if (slotMap[i] == SLOT_UNOCCUPIED)
            {
                rec_id.block = blockNum;
                rec_id.slot = i;
                break;
            }
        }

        if (rec_id.block != -1 && rec_id.slot != -1)
            break;

        prevBlockNum = blockNum;
        blockNum = header.rblock;
    }

    if (rec_id.block == -1 && rec_id.slot == -1)
    {
        if (relId == RELCAT_RELID)
            return E_MAXRELATIONS;

        RecBuffer recBuffer;
        int ret = recBuffer.getBlockNum();
        if (ret == E_DISKFULL)
            return E_DISKFULL;

        rec_id.block = ret;
        rec_id.slot = 0;

        HeadInfo header = {
            .blockType = REC,
            .pblock = -1,
            .lblock = prevBlockNum,
            .rblock = -1,
            .numEntries = 0,
            .numAttrs = numOfAttributes,
            .numSlots = numOfSlots};
        recBuffer.setHeader(&header);

        unsigned char slotMap[numOfSlots];

        for (int i = 0; i < numOfSlots; ++i)
        {
            slotMap[i] = SLOT_UNOCCUPIED;
        }
        recBuffer.setSlotMap(slotMap);

        if (prevBlockNum != -1)
        {
            RecBuffer recBuffer(prevBlockNum);
            HeadInfo header;
            recBuffer.getHeader(&header);

            header.rblock = rec_id.block;
            recBuffer.setHeader(&header);
        }
        else
        {
            relCatEntry.firstBlk = rec_id.block;
            RelCacheTable::setRelCatEntry(relId, &relCatEntry);
        }

        relCatEntry.lastBlk = rec_id.block;
        RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }

    RecBuffer recBuffer(rec_id.block);
    recBuffer.setRecord(record, rec_id.slot);

    unsigned char slotMap[numOfSlots];
    recBuffer.getSlotMap(slotMap);
    slotMap[rec_id.slot] = SLOT_OCCUPIED;
    recBuffer.setSlotMap(slotMap);

    HeadInfo header;
    recBuffer.getHeader(&header);
    header.numEntries++;
    recBuffer.setHeader(&header);

    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatEntry);

    int flag = SUCCESS;
    for (int attrOffset = 0; attrOffset < numOfAttributes; attrOffset++)
    {
        AttrCatEntry attrCatEntry;
        int ret = AttrCacheTable::getAttrCatEntry(relId, attrOffset, &attrCatEntry);

        int rootBlock = attrCatEntry.rootBlock;
        if(rootBlock != -1)
        {
            int retVal = BPlusTree::bPlusInsert(relId, attrCatEntry.attrName, record[attrOffset], rec_id);

            if (retVal == E_DISKFULL) {
                flag = E_INDEX_BLOCKS_RELEASED;
            }
        }
    }

    return flag;
}

int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
    RecId recId;

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret != SUCCESS)
        return ret;

    int rootBlock = attrCatEntry.rootBlock;
    if (rootBlock == -1)
    {
        recId = BlockAccess::linearSearch(relId, attrName, attrVal, op);
    }
    else
    {
        recId = BPlusTree::bPlusSearch(relId, attrName, attrVal, op);
    }

    if (recId.block == -1 && recId.slot == -1)
        return E_NOTFOUND;

    RecBuffer recBuffer(recId.block);
    recBuffer.getRecord(record, recId.slot);

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE])
{
    if (strcmp(RELCAT_RELNAME, relName) == 0 || strcmp(ATTRCAT_RELNAME, relName) == 0)
        return E_NOTPERMITTED;

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);
    RecId recId = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameAttr, EQ);
    if (recId.block == -1 && recId.slot == -1)
        return E_RELNOTEXIST;

    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
    RecBuffer recBuffer(recId.block);
    recBuffer.getRecord(relCatEntryRecord, recId.slot);

    int firstBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
    int numAttrs = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

    int currBlock = firstBlock;
    while (currBlock != -1)
    {
        RecBuffer recBuffer(currBlock);
        HeadInfo header;
        recBuffer.getHeader(&header);
        int nextBlock = header.rblock;
        recBuffer.releaseBlock();
        currBlock = nextBlock;
    }

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    int numberOfAttributesDeleted = 0;

    while (true)
    {
        RecId attrCatRecId = linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
        if (attrCatRecId.block == -1 && attrCatRecId.slot == -1)
            break;

        numberOfAttributesDeleted++;

        RecBuffer recBuffer(attrCatRecId.block);
        HeadInfo header;
        recBuffer.getHeader(&header);
        Attribute record[header.numAttrs];
        recBuffer.getRecord(record, attrCatRecId.slot);
        int rootBlock = record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;

        unsigned char *slotMap = (unsigned char *)malloc(header.numSlots * sizeof(unsigned char));
        recBuffer.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        recBuffer.setSlotMap(slotMap);
        free(slotMap);

        header.numEntries--;
        recBuffer.setHeader(&header);

        if (header.numEntries == 0)
        {
            RecBuffer recBufferlblock(header.lblock);
            HeadInfo headerlblock;
            recBufferlblock.getHeader(&headerlblock);
            headerlblock.rblock = header.rblock;
            recBufferlblock.setHeader(&headerlblock);
            if (header.rblock != -1)
            {
                RecBuffer recBufferrblock(header.rblock);
                HeadInfo headerrblock;
                recBufferrblock.getHeader(&headerrblock);
                headerrblock.lblock = header.lblock;
                recBufferrblock.setHeader(&headerrblock);
            }
            else
            {
                RelCatEntry attrCatRelEntry;
                RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatRelEntry);
                attrCatRelEntry.lastBlk = header.lblock;
                RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrCatRelEntry);

                RecBuffer relCatBuffer(RELCAT_BLOCK);
                Attribute record[RELCAT_NO_ATTRS];
                relCatBuffer.getRecord(record, RELCAT_SLOTNUM_FOR_ATTRCAT);
                record[RELCAT_LAST_BLOCK_INDEX].nVal = header.lblock;
                relCatBuffer.setRecord(record, RELCAT_SLOTNUM_FOR_ATTRCAT);
            }

            recBuffer.releaseBlock();
        }

        if (rootBlock != -1)
        {
            BPlusTree::bPlusDestroy(rootBlock);
        }
    }

    RecBuffer relCatBuffer(recId.block);
    HeadInfo relCatHeader;
    relCatBuffer.getHeader(&relCatHeader);
    relCatHeader.numEntries--;
    relCatBuffer.setHeader(&relCatHeader);
    unsigned char *slotMap = (unsigned char *)malloc(relCatHeader.numSlots * sizeof(unsigned char));
    relCatBuffer.getSlotMap(slotMap);
    slotMap[recId.slot] = SLOT_UNOCCUPIED;
    relCatBuffer.setSlotMap(slotMap);
    free(slotMap);

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntry);
    relCatEntry.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntry);

    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntry);
    relCatEntry.numRecs = relCatEntry.numRecs - numberOfAttributesDeleted;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntry);

    return SUCCESS;
}

int BlockAccess::project(int relId, Attribute *record)
{
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    int block, slot;

    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);
        block = relCatEntry.firstBlk;
        slot = 0;
    }
    else
    {
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    while (block != -1)
    {
        RecBuffer recBuffer(block);

        HeadInfo header;
        recBuffer.getHeader(&header);

        unsigned char slotMap[header.numSlots];
        recBuffer.getSlotMap(slotMap);

        if (slot >= header.numSlots)
        {
            block = header.rblock;
            slot = 0;
        }
        else if (slotMap[slot] == SLOT_UNOCCUPIED)
        {
            slot++;
        }
        else
        {
            break;
        }
    }

    if (block == -1)
    {
        return E_NOTFOUND;
    }

    RecId nextRecId{block, slot};
    RelCacheTable::setSearchIndex(relId, &nextRecId);

    RecBuffer recBuffer(block);
    recBuffer.getRecord(record, slot);

    return SUCCESS;
}
