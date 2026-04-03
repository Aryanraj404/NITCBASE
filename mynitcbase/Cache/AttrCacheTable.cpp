#include "AttrCacheTable.h"
#include "define/constants.h"
#include <cstring>


AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN] = {nullptr};


int AttrCacheTable::getAttrCatEntry(int relId,
                                   char attrName[ATTR_SIZE],
                                   AttrCatEntry* attrCatBuf) {

    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    for (AttrCacheEntry* entry = attrCache[relId];
         entry != nullptr;
         entry = entry->next) {

        if (strcmp(entry->attrCatEntry.attrName, attrName) == 0) {
            *attrCatBuf = entry->attrCatEntry;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::getAttrCatEntry(int relId,
                                   int attrOffset,
                                   AttrCatEntry* attrCatBuf) {

    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    for (AttrCacheEntry* entry = attrCache[relId];
         entry != nullptr;
         entry = entry->next) {

        if (entry->attrCatEntry.offset == attrOffset) {
            *attrCatBuf = entry->attrCatEntry;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}


void AttrCacheTable::recordToAttrCatEntry(
        union Attribute record[ATTRCAT_NO_ATTRS],
        AttrCatEntry* attrCatEntry) {

    strcpy(attrCatEntry->relName,
           record[ATTRCAT_REL_NAME_INDEX].sVal);

    strcpy(attrCatEntry->attrName,
           record[ATTRCAT_ATTR_NAME_INDEX].sVal);

    attrCatEntry->attrType =
        (int)record[ATTRCAT_ATTR_TYPE_INDEX].nVal;

    attrCatEntry->primaryFlag =
        (int)record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;

    attrCatEntry->rootBlock =
        (int)record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;

    attrCatEntry->offset =
        (int)record[ATTRCAT_OFFSET_INDEX].nVal;
}
int AttrCacheTable::getSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex)
{
    if (relId < 0 || relId >= MAX_OPEN) return E_OUTOFBOUND;

     if (attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    AttrCacheEntry *curr = AttrCacheTable::attrCache[relId];
    while (curr) {
        if (strcmp(curr->attrCatEntry.attrName, attrName) == 0)
        {
            *searchIndex = curr->searchIndex;
            return SUCCESS;
        }
        curr = curr->next;
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::getSearchIndex(int relId, int attrOffset, IndexId *searchIndex)
{
    if (relId < 0 || relId >= MAX_OPEN) return E_OUTOFBOUND;

     if (attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }
    AttrCacheEntry *curr = AttrCacheTable::attrCache[relId];

    while (curr) {
        if (curr->attrCatEntry.offset == attrOffset)
        {
            *searchIndex = curr->searchIndex;
            return SUCCESS;
        }
        curr = curr->next;
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setSearchIndex(int relId,
                                   char attrName[ATTR_SIZE],
                                   IndexId *searchIndex)
{

    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }


    AttrCacheEntry *curr = attrCache[relId];

    while (curr != nullptr) {
        if (strcmp(curr->attrCatEntry.attrName, attrName) == 0) {

       
            curr->searchIndex = *searchIndex;

            return SUCCESS;
        }

        curr = curr->next;
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setSearchIndex(int relId,
                                   int attrOffset,
                                   IndexId *searchIndex)
{
    
    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    AttrCacheEntry *curr = attrCache[relId];

    while (curr != nullptr) {
        if (curr->attrCatEntry.offset == attrOffset) {

    
            curr->searchIndex = *searchIndex;

            return SUCCESS;
        }

        curr = curr->next;
    }

    return E_ATTRNOTEXIST;
}
int AttrCacheTable::resetSearchIndex(int relId,
                                     char attrName[ATTR_SIZE])
{
  
    IndexId idx;
    idx.block = -1;
    idx.index = -1;

 
    return AttrCacheTable::setSearchIndex(relId, attrName, &idx);
}

int AttrCacheTable::resetSearchIndex(int relId,
                                     int attrOffset)
{
  
    IndexId idx;
    idx.block = -1;
    idx.index = -1;


    return AttrCacheTable::setSearchIndex(relId, attrOffset, &idx);
}
int AttrCacheTable::setAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuf)
{
    if (relId < 0 || relId >= MAX_OPEN)
        return E_OUTOFBOUND;

    if (attrCache[relId] == nullptr)
        return E_RELNOTOPEN;

    AttrCacheEntry *curr = attrCache[relId];

    while (curr)
    {
        if (strcmp(curr->attrCatEntry.attrName, attrName) == 0)
        {
            curr->attrCatEntry = *attrCatBuf;
            curr->dirty = true;
            return SUCCESS;
        }
        curr = curr->next;
    }

    return E_ATTRNOTEXIST;
}
int AttrCacheTable::setAttrCatEntry(int relId, int attrOffset, AttrCatEntry *attrCatBuf)
{
    if (relId < 0 || relId >= MAX_OPEN)
        return E_OUTOFBOUND;

    if (attrCache[relId] == nullptr)
        return E_RELNOTOPEN;

    AttrCacheEntry *curr = attrCache[relId];

    while (curr)
    {
        if (curr->attrCatEntry.offset == attrOffset)
        {
            curr->attrCatEntry = *attrCatBuf;
            curr->dirty = true;
            return SUCCESS;
        }
        curr = curr->next;
    }

    return E_ATTRNOTEXIST;
}
void AttrCacheTable::attrCatEntryToRecord(
    AttrCatEntry *attrCatEntry,
    union Attribute record[ATTRCAT_NO_ATTRS])
{
    // copy relation name
    strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal,
           attrCatEntry->relName);

    // copy attribute name
    strcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal,
           attrCatEntry->attrName);

    // copy attribute type
    record[ATTRCAT_ATTR_TYPE_INDEX].nVal =
        attrCatEntry->attrType;

    // copy primary flag
    record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal =
        attrCatEntry->primaryFlag;

    // copy root block
    record[ATTRCAT_ROOT_BLOCK_INDEX].nVal =
        attrCatEntry->rootBlock;

    // copy offset
    record[ATTRCAT_OFFSET_INDEX].nVal =
        attrCatEntry->offset;
}