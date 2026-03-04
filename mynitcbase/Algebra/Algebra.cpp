#include "Algebra.h"
#include "BlockAccess/BlockAccess.h"
#include "Cache/OpenRelTable.h"
#include "Cache/RelCacheTable.h"
#include "Cache/AttrCacheTable.h"
#include "Buffer/BlockBuffer.h"
#include "define/constants.h"

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cerrno>

/*
 * SELECT operation
 */
int Algebra::select(char srcRel[ATTR_SIZE],
                    char targetRel[ATTR_SIZE],
                    char attr[ATTR_SIZE],
                    int op,
                    char strVal[ATTR_SIZE]) {

    int srcRelId = OpenRelTable::getRelId(srcRel);
    if (srcRelId == E_RELNOTOPEN)
        return E_RELNOTOPEN;

    AttrCatEntry attrCatEntry;
    if (AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry) != SUCCESS)
        return E_ATTRNOTEXIST;

    Attribute attrVal;
    if (attrCatEntry.attrType == NUMBER) {
        if (!isNumber(strVal))
            return E_ATTRTYPEMISMATCH;
        attrVal.nVal = atof(strVal);
    } else {
        strcpy(attrVal.sVal, strVal);
    }

    //  reset search
    RelCacheTable::resetSearchIndex(srcRelId);

    // FETCH relCatEntry ONCE (THIS FIXES YOUR ERROR)
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    //  print header
    printf("|");
    for (int i = 0; i < relCatEntry.numAttrs; i++) {
        AttrCatEntry col;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &col);
        printf(" %s |", col.attrName);
    }
    printf("\n");
 //////////////
   
    // print tuples
    while (true) {

        RecId searchRes =
            BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

        if (searchRes.block == -1 && searchRes.slot == -1)
            break;

        RecBuffer recBuf(searchRes.block);

        Attribute record[relCatEntry.numAttrs];
        recBuf.getRecord(record, searchRes.slot);

        printf("|");
        for (int i = 0; i < relCatEntry.numAttrs; i++) {
            AttrCatEntry col;
            AttrCacheTable::getAttrCatEntry(srcRelId, i, &col);

            if (col.attrType == NUMBER)
                printf(" %g |", record[i].nVal);
            else
                printf(" %s |", record[i].sVal);
        }
        printf("\n");
    }

    return SUCCESS;
}
bool isNumber(char *str) {
    char *endptr;
    errno = 0;

    strtod(str, &endptr);

    if (errno != 0)
        return false;

    while (*endptr == ' ' || *endptr == '\t')
        endptr++;

    return *endptr == '\0';
}
int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]){
    
    if(strcmp(relName,"RELATIONCAT") == 0 ||strcmp(relName,"ATTRIBUTECAT") == 0)
        return E_NOTPERMITTED;
 
    int relId = OpenRelTable::getRelId(relName);

    if(relId == E_RELNOTOPEN)
        return E_RELNOTOPEN;

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    if(relCatEntry.numAttrs!= nAttrs){
        return E_NATTRMISMATCH;
    }

    union Attribute recordValues[nAttrs];
    
   
    for(int i=0;i<nAttrs;i++)
    {
        
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, i, &attrCatEntry);
       
        int type = attrCatEntry.attrType;
        
          if(type == NUMBER)
        {
            if(isNumber(record[i]))
            {
                recordValues[i].nVal = atof(record[i]);
            }
            else
            {
                return E_ATTRTYPEMISMATCH;
            }
        }
        else if(type == STRING)
        {
            strcpy(recordValues[i].sVal, record[i]);
        }
    
}
    
    int retVal = BlockAccess::insert(relId, recordValues);
   

    return retVal;
}


