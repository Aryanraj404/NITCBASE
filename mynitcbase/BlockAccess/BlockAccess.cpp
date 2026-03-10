#include "BlockAccess.h"          
#include "Cache/RelCacheTable.h"  
#include "Cache/AttrCacheTable.h" 
#include "Buffer/BlockBuffer.h"   
#include "define/constants.h"    
#include <cstring>               
#include <stdio.h>

RecId BlockAccess::linearSearch(int relId,
                                char attrName[ATTR_SIZE],
                                union Attribute attrVal,
                                int op) {
    
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    int block, slot;

    
    if (prevRecId.block == -1 && prevRecId.slot == -1) {

        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);
        block = relCatEntry.firstBlk;
        slot = 0;
        
    } else {
        block = prevRecId.block;
        slot  = prevRecId.slot + 1;
    }


    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    int attrOffset = attrCatEntry.offset;
    int attrType   = attrCatEntry.attrType;

    while (block != -1) {

        RecBuffer recBuf(block);

        HeadInfo head;
        recBuf.getHeader(&head);

        unsigned char slotMap[head.numSlots];
        recBuf.getSlotMap(slotMap);

        if (slot >= head.numSlots) {
            block = head.rblock;
            slot = 0;
            continue;
        }

       
        if (slotMap[slot] == SLOT_UNOCCUPIED) {
            slot++;
            continue;
        }

     
        Attribute record[head.numAttrs];
        recBuf.getRecord(record, slot);

  
        int cmpVal = compareAttrs(record[attrOffset], attrVal, attrType);

       
        if (
            (op == NE && cmpVal != 0) ||
            (op == LT && cmpVal < 0)  ||
            (op == LE && cmpVal <= 0) ||
            (op == EQ && cmpVal == 0) ||
            (op == GT && cmpVal > 0)  ||
            (op == GE && cmpVal >= 0)
        ) {
            RecId found = {block, slot};
            RelCacheTable::setSearchIndex(relId, &found);
            return found;
        }

        slot++;
    }

    return RecId{-1, -1};
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE],
                                char newName[ATTR_SIZE]) {

    /* ---------------------------------------------------------
       Step 1: Check if relation with newName already exists
       --------------------------------------------------------- */

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute newRelationName;
    strcpy(newRelationName.sVal, newName);

    RecId searchRes = BlockAccess::linearSearch(
        RELCAT_RELID,
        (char *)"RelName",
        newRelationName,
        EQ
    );

    if (!(searchRes.block == -1 && searchRes.slot == -1)) {
        return E_RELEXIST;
    }

    /* ---------------------------------------------------------
       Step 2: Search for relation with oldName
       --------------------------------------------------------- */

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute oldRelationName;
    strcpy(oldRelationName.sVal, oldName);

    RecId relcatRecId = BlockAccess::linearSearch(
        RELCAT_RELID,
        (char *)"RelName",
        oldRelationName,
        EQ
    );

    if (relcatRecId.block == -1 && relcatRecId.slot == -1) {
        return E_RELNOTEXIST;
    }

    /* ---------------------------------------------------------
       Step 3: Update RELATIONCAT record
       --------------------------------------------------------- */

    RecBuffer relCatBlock(relcatRecId.block);

    Attribute relcatRecord[RELCAT_NO_ATTRS];
    relCatBlock.getRecord(relcatRecord, relcatRecId.slot);

    // Update relation name field
    strcpy(relcatRecord[RELCAT_REL_NAME_INDEX].sVal, newName);

    // Write back record
    relCatBlock.setRecord(relcatRecord, relcatRecId.slot);

    /* ---------------------------------------------------------
       Step 4: Update all ATTRIBUTECAT entries of that relation
       --------------------------------------------------------- */

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    while (true) {

        RecId attrRecId = BlockAccess::linearSearch(
            ATTRCAT_RELID,
            (char *)"RelName",
            oldRelationName,
            EQ
        );

        if (attrRecId.block == -1 && attrRecId.slot == -1) {
            break;
        }

        RecBuffer attrCatBlock(attrRecId.block);

        Attribute attrRecord[ATTRCAT_NO_ATTRS];
        attrCatBlock.getRecord(attrRecord, attrRecId.slot);

        // Update RelName field in Attribute Catalog
        strcpy(attrRecord[ATTRCAT_REL_NAME_INDEX].sVal, newName);

        // Write back
        attrCatBlock.setRecord(attrRecord, attrRecId.slot);
    }

    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE],
                                 char oldName[ATTR_SIZE],
                                 char newName[ATTR_SIZE]) {

    /* ---------------------------------------------------------
       Step 1: Check relation exists in RELATIONCAT
       --------------------------------------------------------- */

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    RecId relRecId = BlockAccess::linearSearch(
        RELCAT_RELID,
        (char *)"RelName",
        relNameAttr,
        EQ
    );

    if (relRecId.block == -1 && relRecId.slot == -1) {
        return E_RELNOTEXIST;
    }

    /* ---------------------------------------------------------
       Step 2: Search ATTRIBUTECAT for oldName + check newName
       --------------------------------------------------------- */

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    RecId attrToRenameRecId{-1, -1};

    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    while (true) {

        RecId attrRecId = BlockAccess::linearSearch(
            ATTRCAT_RELID,
            (char *)"RelName",
            relNameAttr,
            EQ
        );

        if (attrRecId.block == -1 && attrRecId.slot == -1) {
            break;
        }

        RecBuffer attrCatBlock(attrRecId.block);
        attrCatBlock.getRecord(attrCatEntryRecord, attrRecId.slot);

        // If attribute with oldName found
        if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,
                   oldName) == 0) {

            attrToRenameRecId = attrRecId;
        }

        // If attribute with newName already exists
        if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,
                   newName) == 0) {

            return E_ATTREXIST;
        }
    }

    if (attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1) {
        return E_ATTRNOTEXIST;
    }

    
    RecBuffer renameBlock(attrToRenameRecId.block);

    renameBlock.getRecord(attrCatEntryRecord, attrToRenameRecId.slot);

    strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);

    renameBlock.setRecord(attrCatEntryRecord, attrToRenameRecId.slot);

    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record) {
  
    RelCatEntry relEntry;
     RelCacheTable::getRelCatEntry(relId, &relEntry);
    
    int blockNum = relEntry.firstBlk;

 
    RecId rec_id = {-1, -1};

    int numOfSlots = relEntry.numSlotsPerBlk;
    int numOfAttributes = relEntry.numAttrs;

    int prevBlockNum = -1;

  
    while (blockNum != -1) {
       
        RecBuffer recBuff(blockNum);
      
        HeadInfo header;
        recBuff.getHeader(&header);
     
        unsigned char slotMap[numOfSlots];
        recBuff.getSlotMap(slotMap);
        for (int i = 0; i < numOfSlots; i++) {
            if (slotMap[i] == SLOT_UNOCCUPIED) {
                rec_id.block = blockNum;
                rec_id.slot = i;
                break;
            }
        }

        if (rec_id.block != -1)
            break;

        prevBlockNum = blockNum;
        blockNum = header.rblock;
    }

    if(rec_id.block == -1)
    {
  
        if (relId == RELCAT_RELID)
            return E_MAXRELATIONS;
 
        RecBuffer newBlock;
        int ret = newBlock.getBlockNum();
  
        if (ret == E_DISKFULL) {
            return E_DISKFULL;
        }


        rec_id.block = ret;
        rec_id.slot = 0;
        HeadInfo newHeader;
        newHeader.blockType = REC;
        newHeader.pblock = -1;
        newHeader.lblock = prevBlockNum;
        newHeader.rblock = -1;
        newHeader.numEntries = 0;
        newHeader.numSlots = numOfSlots;
        newHeader.numAttrs = numOfAttributes;

        newBlock.setHeader(&newHeader);
        unsigned char slotMap[numOfSlots];
        for (int i = 0; i < numOfSlots; i++)
            slotMap[i] = SLOT_UNOCCUPIED;

        newBlock.setSlotMap(slotMap);

        if (prevBlockNum != -1) {

            RecBuffer prevBlock(prevBlockNum);

            HeadInfo prevHeader;
            prevBlock.getHeader(&prevHeader);

            prevHeader.rblock = rec_id.block;

            prevBlock.setHeader(&prevHeader);
        }
        else {
          
            relEntry.firstBlk = rec_id.block;
        }

        relEntry.lastBlk= rec_id.block;

        RelCacheTable::setRelCatEntry(relId, &relEntry);
    }

    RecBuffer targetBlock(rec_id.block);

    targetBlock.setRecord(record, rec_id.slot);

    unsigned char slotMap[numOfSlots];
    targetBlock.getSlotMap(slotMap);

    slotMap[rec_id.slot] = SLOT_OCCUPIED;

    targetBlock.setSlotMap(slotMap);

    HeadInfo header;
    targetBlock.getHeader(&header);

    header.numEntries++;

    targetBlock.setHeader(&header);

 
    relEntry.numRecs++;

    RelCacheTable::setRelCatEntry(relId, &relEntry);

    return SUCCESS;
}

int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{

    RecId recId;

    recId = linearSearch(relId, attrName, attrVal, op);

    if(recId.block == -1 && recId.slot == -1)
        return E_NOTFOUND;

    RecBuffer recBlock(recId.block);

    recBlock.getRecord(record, recId.slot);

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {

    // prevent deletion of catalogs
    if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
        return E_NOTPERMITTED;

    // reset search index of relation catalog
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    // search relation catalog
    RecId relCatRecId = linearSearch(RELCAT_RELID, (char *)"RelName", relNameAttr, EQ);

    if(relCatRecId.block == -1 && relCatRecId.slot == -1)
        return E_RELNOTEXIST;

    // fetch relation catalog record
    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
    RecBuffer relCatBlock(relCatRecId.block);
    relCatBlock.getRecord(relCatEntryRecord, relCatRecId.slot);

    int firstBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
    int numAttrs = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

    /*
        Delete all record blocks of the relation
    */
    int block = firstBlock;

    while(block != -1) {

        BlockBuffer blockBuf(block);

        HeadInfo header;
        blockBuf.getHeader(&header);

        int nextBlock = header.rblock;

        blockBuf.releaseBlock();

        block = nextBlock;
    }

    /*
        Delete attribute catalog entries
    */

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    int numberOfAttributesDeleted = 0;

    while(true) {

        RecId attrCatRecId =
            linearSearch(ATTRCAT_RELID, (char *)"RelName", relNameAttr, EQ);

        if(attrCatRecId.block == -1 && attrCatRecId.slot == -1)
            break;

        numberOfAttributesDeleted++;

        RecBuffer attrBlock(attrCatRecId.block);

        HeadInfo header;
        attrBlock.getHeader(&header);

        Attribute record[ATTRCAT_NO_ATTRS];
        attrBlock.getRecord(record, attrCatRecId.slot);

        int rootBlock = record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;

        // update slot map
        unsigned char slotMap[header.numSlots];
        attrBlock.getSlotMap(slotMap);

        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;

        attrBlock.setSlotMap(slotMap);

        header.numEntries--;
        attrBlock.setHeader(&header);

        if(header.numEntries == 0) {

            int lblock = header.lblock;
            int rblock = header.rblock;

            if(lblock != -1) {
                RecBuffer leftBlock(lblock);

                HeadInfo leftHeader;
                leftBlock.getHeader(&leftHeader);

                leftHeader.rblock = rblock;
                leftBlock.setHeader(&leftHeader);
            }

            if(rblock != -1) {
                RecBuffer rightBlock(rblock);

                HeadInfo rightHeader;
                rightBlock.getHeader(&rightHeader);

                rightHeader.lblock = lblock;
                rightBlock.setHeader(&rightHeader);
            }

            attrBlock.releaseBlock();
        }

        // (future stage for index deletion)

    }

    /*
        Delete relation catalog entry
    */

    HeadInfo relHeader;
    relCatBlock.getHeader(&relHeader);

    relHeader.numEntries--;
    relCatBlock.setHeader(&relHeader);

    unsigned char relSlotMap[relHeader.numSlots];
    relCatBlock.getSlotMap(relSlotMap);

    relSlotMap[relCatRecId.slot] = SLOT_UNOCCUPIED;

    relCatBlock.setSlotMap(relSlotMap);

    /*
        Update relation catalog cache entry
    */

    RelCatEntry relEntry;

    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relEntry);

    relEntry.numRecs--;

    RelCacheTable::setRelCatEntry(RELCAT_RELID, &relEntry);

    /*
        Update attribute catalog cache entry
    */

    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relEntry);

    relEntry.numRecs -= numberOfAttributesDeleted;

    RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relEntry);

    return SUCCESS;
}
