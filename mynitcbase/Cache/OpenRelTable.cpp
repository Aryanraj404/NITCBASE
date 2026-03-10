#include "Cache/OpenRelTable.h"
#include "Cache/RelCacheTable.h"
#include "Cache/AttrCacheTable.h"
#include "Buffer/BlockBuffer.h"
#include "define/constants.h"
#include <stdio.h>
#include <cstdlib>
#include <cstring>


OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() {

    /* ---------- Initialise caches ---------- */
    for (int i = 0; i < MAX_OPEN; i++) {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
        tableMetaInfo[i].free = true;
    }


    {
        RecBuffer relCatBlock(RELCAT_BLOCK);
        Attribute record[RELCAT_NO_ATTRS];

        relCatBlock.getRecord(record, RELCAT_SLOTNUM_FOR_RELCAT);

        RelCacheEntry *entry =
            (RelCacheEntry *)malloc(sizeof(RelCacheEntry));

        RelCacheTable::recordToRelCatEntry(record, &entry->relCatEntry);

        entry->recId.block = RELCAT_BLOCK;
        entry->recId.slot  = RELCAT_SLOTNUM_FOR_RELCAT;
        entry->searchIndex.block = -1;
        entry->searchIndex.slot  = -1;

        RelCacheTable::relCache[RELCAT_RELID] = entry;

        tableMetaInfo[RELCAT_RELID].free = false;
        strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);
    }

    /* =========================================================
       ATTRIBUTECAT (rel-id = 1)
       ========================================================= */
    {
        RecBuffer relCatBlock(RELCAT_BLOCK);
        Attribute record[RELCAT_NO_ATTRS];

        relCatBlock.getRecord(record, RELCAT_SLOTNUM_FOR_ATTRCAT);

        RelCacheEntry *entry =
            (RelCacheEntry *)malloc(sizeof(RelCacheEntry));

        RelCacheTable::recordToRelCatEntry(record, &entry->relCatEntry);

        entry->recId.block = RELCAT_BLOCK;
        entry->recId.slot  = RELCAT_SLOTNUM_FOR_ATTRCAT;
        entry->searchIndex.block = -1;
        entry->searchIndex.slot  = -1;


        RelCacheTable::relCache[ATTRCAT_RELID] = entry;
        tableMetaInfo[ATTRCAT_RELID].free = false;
        strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);
    }


    


    RecBuffer attrCatBlock(ATTRCAT_BLOCK);
    HeadInfo attrCatHeader;
    attrCatBlock.getHeader(&attrCatHeader);


    {
        AttrCacheEntry *head = nullptr, *tail = nullptr;

        for (int i = 0; i < 6; i++) {

            Attribute record[ATTRCAT_NO_ATTRS];
            attrCatBlock.getRecord(record, i);

            AttrCacheEntry *entry =
                (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));

            AttrCacheTable::recordToAttrCatEntry(record,
                                                 &entry->attrCatEntry);

            entry->recId.block = ATTRCAT_BLOCK;
            entry->recId.slot  = i;
            entry->next = nullptr;

            if (!head) head = tail = entry;
            else { tail->next = entry; tail = entry; }
        }

        AttrCacheTable::attrCache[RELCAT_RELID] = head;
    }

    {
        AttrCacheEntry *head = nullptr, *tail = nullptr;

        for (int i = 6; i < 12; i++) {

            Attribute record[ATTRCAT_NO_ATTRS];
            attrCatBlock.getRecord(record, i);

            AttrCacheEntry *entry =
                (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));

            AttrCacheTable::recordToAttrCatEntry(record,
                                                 &entry->attrCatEntry);

            entry->recId.block = ATTRCAT_BLOCK;
            entry->recId.slot  = i;
            entry->next = nullptr;

            if (!head) head = tail = entry;
            else { tail->next = entry; tail = entry; }
        }

        AttrCacheTable::attrCache[ATTRCAT_RELID] = head;
    }

   
}

int OpenRelTable::getFreeOpenRelTableEntry() {

    for (int i = 2; i < MAX_OPEN; i++) {
        if (tableMetaInfo[i].free) {
            return i;
        }
    }

    return E_CACHEFULL;
}


/* =========================================================
   getRelId
   ========================================================= */
int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {

    for (int i = 0; i < MAX_OPEN; i++) {
        if (!tableMetaInfo[i].free &&
            strcmp(tableMetaInfo[i].relName, relName) == 0) {
            return i;
        }
    }

    return E_RELNOTOPEN;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]) {

    /* If relation already open, return its rel-id */
    int existingRelId = OpenRelTable::getRelId((char *)relName);
    if (existingRelId >= 0) {
        return existingRelId;
    }

    /* Find free slot in Open Relation Table */
    int relId = OpenRelTable::getFreeOpenRelTableEntry();
    if (relId < 0) {
        return E_CACHEFULL;
    }

    /****** Setting up Relation Cache entry ******/

    /* Reset search index of RELATIONCAT */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute attrVal;
    strcpy(attrVal.sVal, (char *)relName);

    /* Search RELATIONCAT for the relation */
    RecId relcatRecId = BlockAccess::linearSearch(
        RELCAT_RELID,
        (char *)"RelName",
        attrVal,
        EQ
    );

    if (relcatRecId.block == -1 && relcatRecId.slot == -1) {
        return E_RELNOTEXIST;
    }

    /* Read relation catalog record */
    RecBuffer relCatBlock(relcatRecId.block);
    Attribute relcatRecord[RELCAT_NO_ATTRS];
    relCatBlock.getRecord(relcatRecord, relcatRecId.slot);

    RelCacheEntry *relEntry =
        (RelCacheEntry *)malloc(sizeof(RelCacheEntry));

    RelCacheTable::recordToRelCatEntry(
        relcatRecord,
        &relEntry->relCatEntry
    );

    relEntry->recId = relcatRecId;
    relEntry->searchIndex = {-1, -1};
    relEntry->dirty = false;
    RelCacheTable::relCache[relId] = relEntry;

    /****** Setting up Attribute Cache entry ******/

    AttrCacheEntry *listHead = nullptr;
    AttrCacheEntry *tail = nullptr;

    /* Reset search index of ATTRIBUTECAT */
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    while (true) {

        RecId attrcatRecId = BlockAccess::linearSearch(
            ATTRCAT_RELID,
            (char *)"RelName",
            attrVal,
            EQ
        );

        if (attrcatRecId.block == -1 && attrcatRecId.slot == -1) {
            break;
        }

        RecBuffer attrCatBlock(attrcatRecId.block);
        Attribute attrcatRecord[ATTRCAT_NO_ATTRS];
        attrCatBlock.getRecord(attrcatRecord, attrcatRecId.slot);

        AttrCacheEntry *attrEntry =
            (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));

        AttrCacheTable::recordToAttrCatEntry(
            attrcatRecord,
            &attrEntry->attrCatEntry
        );

        attrEntry->recId = attrcatRecId;
        attrEntry->next = nullptr;

        if (!listHead) {
            listHead = tail = attrEntry;
        } else {
            tail->next = attrEntry;
            tail = attrEntry;
        }
    }

    AttrCacheTable::attrCache[relId] = listHead;



    tableMetaInfo[relId].free = false;
    strcpy(tableMetaInfo[relId].relName, (char *)relName);

    return relId;
}


int OpenRelTable::closeRel(int relId) {

   
    if (relId < 2 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if (tableMetaInfo[relId].free) {
        return E_RELNOTOPEN;
    }

  

    if (RelCacheTable::relCache[relId]->dirty) {

        RelCacheEntry *relEntry = RelCacheTable::relCache[relId];

        Attribute record[RELCAT_NO_ATTRS];


        RelCacheTable::relCatEntryToRecord(
            &relEntry->relCatEntry,
            record
        );

        RecId recId = relEntry->recId;
        RecBuffer relCatBlock(recId.block);

        relCatBlock.setRecord(record, recId.slot);
    }

  

    free(RelCacheTable::relCache[relId]);
    RelCacheTable::relCache[relId] = nullptr;

    

    AttrCacheEntry *entry = AttrCacheTable::attrCache[relId];

    while (entry != nullptr) {
        AttrCacheEntry *next = entry->next;
        free(entry);
        entry = next;
    }

    AttrCacheTable::attrCache[relId] = nullptr;



    tableMetaInfo[relId].free = true;


RelCacheTable::resetSearchIndex(RELCAT_RELID);
RelCacheTable::resetSearchIndex(ATTRCAT_RELID);


    return SUCCESS;
}



OpenRelTable::~OpenRelTable()
{
    for (int i = 2; i < MAX_OPEN; ++i)
    {
        if (tableMetaInfo[i].free == false)
        {
            OpenRelTable::closeRel(i);
        }
    }

    /**** Closing the catalog relations in the relation cache ****/

    // releasing the relation cache entry of the attribute catalog

    if (RelCacheTable::relCache[ATTRCAT_RELID]->dirty == true)
    {

        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
        Attribute record[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&RelCacheTable::relCache[ATTRCAT_RELID]->relCatEntry, record);
        // declaring an object of RecBuffer class to write back to the buffer
        RecId recId = {RELCAT_BLOCK, RELCAT_SLOTNUM_FOR_ATTRCAT};
        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(record, recId.slot);
        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
    }
    // free the memory dynamically allocated to this RelCacheEntry
    free(RelCacheTable::relCache[ATTRCAT_RELID]);

    // releasing the relation cache entry of the relation catalog

    if (RelCacheTable::relCache[RELCAT_RELID]->dirty == true)
    {

        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
        Attribute record[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&RelCacheTable::relCache[RELCAT_RELID]->relCatEntry, record);

        // declaring an object of RecBuffer class to write back to the buffer
        RecId recId = {RELCAT_BLOCK, RELCAT_SLOTNUM_FOR_RELCAT};
        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(record, recId.slot);
        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
    }
    // free the memory dynamically allocated for this RelCacheEntry
    free(RelCacheTable::relCache[RELCAT_RELID]);

    for (int i = RELCAT_RELID; i <= ATTRCAT_RELID; i++)
    {
        struct AttrCacheEntry *Head = AttrCacheTable::attrCache[i];
        while (Head != nullptr)
        {
            struct AttrCacheEntry *entry = Head;
            Head = Head->next;
            free(entry);
        }
    }
    // free the memory allocated for the attribute cache entries of the
    // relation catalog and the attribute catalog
}