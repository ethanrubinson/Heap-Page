#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "heappage.h"
#include "heapfile.h"
#include "db.h"

#include <limits>

using namespace std;

//------------------------------------------------------------------
// HeapPage::Init
//
// Input     : Page ID.
// Output    : None.
// Purpose   : Inialize the page with given PageID.
//------------------------------------------------------------------

void HeapPage::Init(PageID pageNo)
{
	nextPage = INVALID_PAGE;
	prevPage = INVALID_PAGE;
	
	numOfSlots = 0;
	pid = pageNo;
	freePtr = 0;
	freeSpace = HEAPPAGE_DATA_SIZE;
}


//------------------------------------------------------------------
// HeapPage::InsertRecord
//
// Input     : Pointer to the record and the record's length.
// Output    : Record ID of the record inserted.
// Purpose   : Insert a record into the page.
// Return    : OK if everything went OK, DONE if sufficient space 
//             does not exist.
//------------------------------------------------------------------

Status HeapPage::InsertRecord(const char *recPtr, int length, RecordID& rid)
{

	// Check to see if we have any empty slots
	int slotNumToCheck = 1;
	bool foundEmptySlot = false;
	Slot* slotToCheck;
	for(slotNumToCheck; slotNumToCheck <= numOfSlots; slotNumToCheck++){
		// Compute the pointer to the slot we will check
		slotToCheck = GetFirstSlotPointer() - (slotNumToCheck - 1);

		// Check to if the slot is empty
		if (SlotIsEmpty(slotToCheck)) {
			foundEmptySlot = true;
			break;
		}
	}

	// Check to see if we have the propper amount of space to add a record and potentially a new slot
	if (freeSpace < length + ((foundEmptySlot) ? 0 : sizeof(Slot))) return DONE;

	// Set the new slot (or fill the empty one) and set the rid
	rid.pageNo = pid;
	Slot* newSlot;
	if (foundEmptySlot) {
		newSlot = slotToCheck;
		rid.slotNo = slotNumToCheck;
	}
	else {
		newSlot = GetFirstSlotPointer() - numOfSlots;
		numOfSlots++;
		rid.slotNo = numOfSlots;
		freeSpace -= sizeof(Slot);
	}
	newSlot->length = length;
	newSlot->offset = freePtr;
	

	// Set the new record
	char* newRecPtr = data + newSlot->offset;
	memcpy(newRecPtr, recPtr, length);

	freePtr += length;
	freeSpace -= length;

	return OK;
}


//------------------------------------------------------------------
// HeapPage::DeleteRecord 
//
// Input    : Record ID.
// Output   : None.
// Purpose  : Delete a record from the page.
// Return   : OK if successful, FAIL otherwise.
//------------------------------------------------------------------ 

Status HeapPage::DeleteRecord(RecordID rid)
{
	// Ensure pid matches this page and the slot number is valid
	
	if (rid.pageNo != pid || rid.slotNo > numOfSlots || rid.slotNo < 1) return FAIL;
	
	// Compute the pointer to the current Slot
	Slot* delSlot = GetFirstSlotPointer() - (rid.slotNo - 1);

	// Ensure that our slot is full
	if (SlotIsEmpty(delSlot)) return FAIL;

	// Get a pointer to the record
	char *delPtr = (char*)(data + delSlot->offset);

	// Close the hole created by deleting the record & update affected slots
	RecordID curRid = rid;
	RecordID nextRid;
	int baseOffset = 0;
	Status status;
	while((status = RecordWithOffset(delSlot->offset + delSlot->length + baseOffset, nextRid)) != DONE){
		curRid = nextRid;
		Slot* moveSlot = GetFirstSlotPointer() - (curRid.slotNo - 1);
		char* movePtr = (char*)(data + moveSlot->offset);
		memmove(delPtr + baseOffset, movePtr, moveSlot->length);
		moveSlot->offset -= delSlot->length;
		baseOffset += moveSlot->length;
	}

	// Update free space information
	freePtr -= delSlot->length;
	freeSpace += delSlot->length;

	// Only delete the slot if it is the last one to avoid messing up the numbering
	if (rid.slotNo == numOfSlots) {
		numOfSlots--;
		freeSpace += sizeof(Slot);
	}
		
	SetSlotEmpty(delSlot);


	return OK;
}

//------------------------------------------------------------------
// HeapPage::RecordWithOffset
//
// Input     : Desired offset of the record.
// Output    : Record ID of the record with the designated offset.
// Purpose   : Find a record with a certain offset.
// Return    : OK if a record was found, DONE otherwise.
//------------------------------------------------------------------
Status HeapPage::RecordWithOffset(int offset, RecordID& nextRid)
{

	// Find the slot with the offset if it exists
	for(int slotNumToCheck = 1; slotNumToCheck <= numOfSlots; slotNumToCheck++){
		// Compute the pointer to the slot we will check
		Slot* slotToCheck = GetFirstSlotPointer() - (slotNumToCheck - 1);

		// Check to see if it is what we're looking for
		if (!SlotIsEmpty(slotToCheck) && slotToCheck->offset == offset) {
			nextRid.pageNo = pid;
			nextRid.slotNo = slotNumToCheck;
			return OK;
		}
	}

	return DONE;
}

//------------------------------------------------------------------
// HeapPage::FirstRecord
//
// Input    : None.
// Output   : Record id of the first record on a page.
// Purpose  : To find the first record on a page.
// Return   : OK if successful, DONE otherwise.
//------------------------------------------------------------------

Status HeapPage::FirstRecord(RecordID& rid)
{
	//Find the slot that corresponds to the first record if it exists
	for(int slotNumToCheck = 1; slotNumToCheck <= numOfSlots; slotNumToCheck++){
		// Compute the pointer to the slot we will check
		Slot* slotToCheck = GetFirstSlotPointer() - (slotNumToCheck - 1);

		// Check to ensure the slot is not empty and that it's offset is 0
		if (!SlotIsEmpty(slotToCheck) && slotToCheck->offset == 0) {
			rid.pageNo = pid;
			rid.slotNo = slotNumToCheck;
			return OK;
		}
	}

	// If we get here, there was no slot containing the first record
	return DONE;
}


//------------------------------------------------------------------
// HeapPage::NextRecord
//
// Input    : ID of the current record.
// Output   : ID of the next record.
// Purpose  : To find the next record on a page.
// Return   : Return DONE if no more records exist on the page; 
//            otherwise OK.
//------------------------------------------------------------------

Status HeapPage::NextRecord(RecordID curRid, RecordID& nextRid)
{

	// Ensure pid matches this page and the slot number is valid
	if (curRid.pageNo != pid || curRid.slotNo > numOfSlots || curRid.slotNo < 1) return FAIL;
	
	// Compute the pointer to the current Slot
	Slot* curSlot = GetFirstSlotPointer() - (curRid.slotNo - 1);

	// Ensure that our slot is full
	if (SlotIsEmpty(curSlot)) return FAIL;

	// Compute the offset the next record would have if it were to exist
	int nextOffset = curSlot->offset + curSlot->length;

	// Find the slot with this offset if it exists
	for(int slotNumToCheck = 1; slotNumToCheck <= numOfSlots; slotNumToCheck++){
		// Compute the pointer to the slot we will check
		Slot* slotToCheck = GetFirstSlotPointer() - (slotNumToCheck - 1);

		// Check to see if it is what we're looking for
		if (!SlotIsEmpty(slotToCheck) && slotToCheck->offset == nextOffset) {
			nextRid.pageNo = pid;
			nextRid.slotNo = slotNumToCheck;
			return OK;
		}
	}

	return DONE;
}


//------------------------------------------------------------------
// HeapPage::GetRecord
//
// Input    : rid - Record ID. len - the length of allocated memory.
// Output   : Records length and a copy of the record itself.
// Purpose  : To retrieve a COPY of a record with ID rid from a page.
// Return   : OK if successful, FAIL otherwise.
//------------------------------------------------------------------

Status HeapPage::GetRecord(RecordID rid, char *recPtr, int& len)
{
	// Ensure pid matches this page and the slot number is valid
	if (rid.pageNo != pid || rid.slotNo > numOfSlots || rid.slotNo < 1) return FAIL;
	
	// Compute the pointer to the current Slot
	Slot* curSlot = GetFirstSlotPointer() - (rid.slotNo - 1);

	// Check to ensure enough memory has been allocated by the caller and that our slot is full
	if (len < curSlot->length || SlotIsEmpty(curSlot)) return FAIL;

	// Copy the record and set its length
	char *curRecPtr = (char*)(data + curSlot->offset);
	memcpy(recPtr,curRecPtr,curSlot->length);
	len = curSlot->length;

	return OK;
}


//------------------------------------------------------------------
// HeapPage::ReturnRecord
//
// Input    : Record ID.
// Output   : Pointer to the record, record's length.
// Purpose  : To retrieve a POINTER to the record.
// Return   : OK if successful, FAIL otherwise.
//------------------------------------------------------------------

Status HeapPage::ReturnRecord(RecordID rid, char*& recPtr, int& len)
{
	// Ensure pid matches this page and the slot number is valid
	if (rid.pageNo != pid || rid.slotNo > numOfSlots || rid.slotNo < 1) return FAIL;
	
	// Compute the pointer to the current Slot
	Slot* curSlot = GetFirstSlotPointer() - (rid.slotNo - 1);

	// Check to ensure our slot is full
	if (SlotIsEmpty(curSlot)) return FAIL;

	// Set the record pointer and its length
	recPtr = (char*)(data + curSlot->offset);
	len = curSlot->length;

	return OK;
}


//------------------------------------------------------------------
// HeapPage::AvailableSpace
//
// Input    : None.
// Output   : None.
// Purpose  : To return the amount of available space.
// Return   : The amount of available space on the heap file page.
//------------------------------------------------------------------

int HeapPage::AvailableSpace()
{
	return std::max((int)(freeSpace - sizeof(Slot)), 0);
}


//------------------------------------------------------------------
// HeapPage::IsEmpty
// 
// Input    : None.
// Output   : None.
// Purpose  : Check if there is any record in the page.
// Return   : true if the HeapPage is empty, and false otherwise.
//------------------------------------------------------------------

bool HeapPage::IsEmpty()
{
	return (numOfSlots == 0);
}

//------------------------------------------------------------------
// HeapPage::GetNumOfRecords
// 
// Input    : None.
// Output   : None.
// Purpose  : Counts the number of records in the page.
// Return   : Number of records in the page.
//------------------------------------------------------------------

int HeapPage::GetNumOfRecords()
{
	int numRecords = 0;

	// Search through every slot counting the number of full ones
	for(int slotNumToCheck = 1; slotNumToCheck <= numOfSlots; slotNumToCheck++){
		// Compute the pointer to the slot we will check
		Slot* slotToCheck = GetFirstSlotPointer() - (slotNumToCheck - 1);

		// Check to see if it has data
		if (!SlotIsEmpty(slotToCheck)) {
			numRecords++;
		}
	}

	return numRecords;
}


//------------------------------------------------------------------
// HeapPage::SetNextPage
// 
// Input    : The PageID for next page.
// Output   : None.
// Purpose  : Set the PageID for next page.
// Return   : None.
//------------------------------------------------------------------
void HeapPage::SetNextPage(PageID pageNo)
{
	nextPage = pageNo;
}

//------------------------------------------------------------------
// HeapPage::SetNextPage
// 
// Input    : The PageID for previous page.
// Output   : None.
// Purpose  : Set the PageID for previous page.
// Return   : None.
//------------------------------------------------------------------
void HeapPage::SetPrevPage(PageID pageNo)
{
	prevPage = pageNo;
}

//------------------------------------------------------------------
// HeapPage::GetNextPage
// 
// Input    : None.
// Output   : None.
// Purpose  : Get the PageID of next page.
// Return   : The PageID of next page.
//------------------------------------------------------------------
PageID HeapPage::GetNextPage()
{
	return nextPage;
}

//------------------------------------------------------------------
// HeapPage::SetPrevPage
// 
// Input    : The PageID for previous page.
// Output   : None.
// Purpose  : Get the PageID of previous page.
// Return   : The PageID of previous page.
//------------------------------------------------------------------
PageID HeapPage::GetPrevPage()
{
	return prevPage;
}

//------------------------------------------------------------------
// HeapPage::PageNo
// 
// Input    : None.
// Output   : None.
// Purpose  : Get the PageID of this page.
// Return   : The PageID of this page.
//------------------------------------------------------------------
PageID HeapPage::PageNo() 
{
	return pid;
}   
