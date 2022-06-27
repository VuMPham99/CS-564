/**
 * @author Pratyusha Emkay ID: 9077213180
 * @author Vu Pham ID: 9078085595
 * See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

/*
 *The class constructor. Allocates an array for the buffer pool with bufs page frames
 *and a corresponding BufDesc table. The way things are set up all frames will be in the
 *clear state when the buffer pool is allocated. The hash table will also start out in an empty
 *state.
 */
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/*
* flushes all dirty pages and writes to disk if needed
* @author: Pratyusha Emkay (pemkay@wisc.edu)
*/
    BufMgr::~BufMgr() {
	    //loops through all the pages
        for (std::uint32_t i = 0; i < numBufs; i++)
        {
		//condition for page to be dirty 
            if (bufDescTable[i].dirty && File::isOpen(bufDescTable[i].file->filename())) {
		    //flushes dirty pages adn writes to disk
				bufDescTable[i].file->writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
				bufStats.diskwrites++;
            }
        }
	//deallocates the buffer pool and the BufDesc table
        delete [] bufPool;
        delete [] bufDescTable;
		bufDescTable = NULL;
		bufPool = NULL;
    }

/*
* advances the clock
* @author: Pratyusha Emkay (pemkay@wisc.edu)
*/
    void BufMgr::advanceClock() {
       clockHand = (clockHand + 1) % numBufs;
    }

/*
 * Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back
 * to disk  
 * Gets called by the readPage() and allocPage() methods
 *
 * @param frame   	Frame reference, frame ID of allocated frame returned via this variable
 * @throws BufferExceededException If all buffer frames are pinned
 */
    void BufMgr::allocBuf(FrameId & frame) {
	//initialize variables
        std::uint32_t pinCount = 0;
	//loops through all the frames. And condition for clock algorithm to stop
        while(pinCount <= numBufs){
            advanceClock();
	    //does not contain a valid page
            if (bufDescTable[clockHand].valid == false){
                frame = bufDescTable[clockHand].frameNo;
                return;
		//resets refbit if true and goes to next loop
            }if(bufDescTable[clockHand].refbit){
                bufDescTable[clockHand].refbit = false;
                continue;
		//checks if current frame is pinned, if yes skip, else update pin Count 
            }if(bufDescTable[clockHand].pinCnt == 0){
                break;
            }else{
                pinCount++;
            }
        }
	//all frames are pinned
        if(pinCount > numBufs){
            throw BufferExceededException();
        }
	//flushes page to disk if it is dirty
        if(bufDescTable[clockHand].dirty){
            bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
            frame = bufDescTable[clockHand].frameNo;
        }else{
            frame = bufDescTable[clockHand].frameNo;
        }
	//contains a valid pages and deletes it from hashtable
        hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
        bufDescTable[clockHand].Clear();
    }

/**
 * Reads the given page from the file into a frame and returns the pointer to page.
 * If the requested page is already present in the buffer pool pointer to that frame is returned
 * otherwise a new frame is allocated from the buffer pool for reading the page.
 * Case 1: Page is not in the buffer pool. Call allocBuf() to allocate a buffer frame and
 * then call the method file->readPage() to read the page from disk into the buffer pool
 * frame. Next, insert the page into the hashtable. Finally, invoke Set() on the frame to
 * set it up properly. Set() will leave the pinCnt for the page set to 1. Return a pointer
 * to the frame containing the page via the page parameter.
 * Case 2: Page is in the buffer pool. In this case set the appropriate refbit, increment
 * the pinCnt for the page, and then return a pointer to the frame containing the page
 * via the page parameter.
 *
 * @param file   	File object
 * @param PageNo  Page number in the file to be read
 * @param page  	Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
 * @throws HashNotFoundException when page is not in the buffer pool, on the
 * hashtable to get a frame number
 * @throws BufferExceededException if all buffer frames are pinned
 */
    void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {
        FrameId frameNo;
        try {
		//check to see if page is in buffer pool
            hashTable->lookup(file, pageNo, frameNo);
		//set refbit and increment pin counts if page if buffer pool
            bufDescTable[frameNo].refbit = true;
            bufDescTable[frameNo].pinCnt++;
        } catch(HashNotFoundException& e) {
	   //if page not in buffer pool
            try {
		//allocate a buffer frame
                allocBuf(frameNo);
		//read page from disk into buffer pool frame    
                bufPool[frameNo] = file->readPage(pageNo);;
		//insert page into hash table
                hashTable->insert(file, pageNo, frameNo);
		//set the frame
                bufDescTable[frameNo].Set(file, pageNo);

            } catch(BufferExceededException ()) {}
        }
	//Return a pointer to the frame containing the page via the page parameter
        page = &bufPool[frameNo];

    }

/**
 * Unpin a page from memory since it is no longer required for it to remain in memory.
 * Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, sets
 * the dirty bit. Does nothing if page is not found in the hash table lookup
 * @param file   	File object
 * @param PageNo  Page number
 * @param dirty		True if the page to be unpinned needs to be marked dirty	
 * @throws  PageNotPinnedException If the page is not already pinned (i.e. pin count already 0)
 * @throws HashNotFoundException when page is not in the buffer pool, on the 
 * hashtable to get a frame number
 */
    void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) {

        FrameId frameNo;

        try {
	    //find the frame containing (file, PageNo)
            hashTable->lookup(file, pageNo, frameNo);
		//Throws PAGENOTPINNED if the pin count is already 0.
            if(bufDescTable[frameNo].pinCnt == 0){
                throw PageNotPinnedException(file->filename(), bufDescTable[frameNo].pageNo, frameNo);
            }else if(bufDescTable[frameNo].pinCnt > 0){
		    //set dirty bit if dirty is true
                if(dirty){
                    bufDescTable[frameNo].dirty = true;
                }
		    //Decrements the pinCnt of the frame
                bufDescTable[frameNo].pinCnt = bufDescTable[frameNo].pinCnt - 1;
            }
        }catch(HashNotFoundException()){}//does nothing}
    }

/**
 * Allocates a new, empty page in the file and returns the Page object.
 * The newly allocated page is also assigned a frame in the buffer pool.
 *
 * @param file   	File object
 * @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
 * @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
 */
    void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {
        FrameId frameNo;
        allocBuf(frameNo); //obtain a buffer pool frame
        bufPool[frameNo] = file->allocatePage(); //allocate an empty page in the specific file
        page = &bufPool[frameNo]; //return a pointer to the buffer frame allocated for the page
        pageNo = page->page_number(); //return page number of newly allocated page
        hashTable->insert(file, pageNo, frameNo); //insert an entry into the hash table
        bufDescTable[frameNo].Set(file, pageNo); //set up the frame
    }

/**
 * Writes out all dirty pages of the file to disk.
 * All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
 * Otherwise Error returned.
 * Scan bufTable for pages belonging to the file. For each page encountered it should:
 * (a) if the page is dirty, call file->writePage() to flush the page to disk and then set the dirty
 * bit for the page to false, (b) remove the page from the hashtable (whether the page is clean
 * or dirty) and (c) invoke the Clear() method of BufDesc for the page frame
 *
 * @param file   	File object
 * @throws  PagePinnedException If any page of the file is pinned in the buffer pool 
 * @throws BadBufferException If any frame allocated to the file is found to be invalid
 */
    void BufMgr::flushFile(const File* file) 
    {
	//loop to scan for pages belong to the file
        for (std::uint32_t i = 0; i < numBufs; i++)
  {
    BufDesc *currDesc = &bufDescTable[i];
    bufStats.accesses++;
	//condition for file to match
    if (file == currDesc->file)
    {
	    //throw BadBufferException if an invalid page belonging to the file is encountered
      if (!currDesc->valid)
      {
        throw BadBufferException(currDesc->frameNo, currDesc->dirty, currDesc->valid, currDesc->refbit);
        break;
      }
	    // throw PagePinnedException if some page of the file is pinned
      if (currDesc->pinCnt > 0)
      {
        throw PagePinnedException(file->filename(), currDesc->pageNo, currDesc->frameNo);
        break;
      }
	//flush the page to disk and then set the dirty bit for the page to false if page is dir
      if (currDesc->dirty)
      {
        Page dirtyPage = bufPool[currDesc->frameNo];
        currDesc->file->writePage(dirtyPage);
        currDesc->dirty = false;
        bufStats.diskwrites++;
      }
	    //remove the page from the hashtable
      hashTable->remove(file, currDesc->pageNo);
      currDesc->Clear();
    }
    else
    {
      continue;
    }

    }
    }

/**
 * Delete page from file and also from buffer pool if present.
 * Before deleting the page from file, it makes sure that if the page to be deleted is allocated 
 * a frame in the buffer pool, that frame is freed and correspondingly entry from hash table is also removed
 * Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
 *
 * @param file   	File object
 * @param PageNo  Page number
 */
    void BufMgr::disposePage(File* file, const PageId PageNo) {
        FrameId frameNo = -1;
        try {
	    //find the particular page 
            hashTable->lookup(file, PageNo, frameNo);
		//remove the page from the hashtable and free frame
            hashTable->remove(bufDescTable[frameNo].file, bufDescTable[frameNo].pageNo);
            bufDescTable[frameNo].Clear();
        } catch (HashNotFoundException() ) {
            // do nothing
        }
	    //delete page from file
        file->deletePage(PageNo);

    }

    void BufMgr::printSelf(void)
    {
        BufDesc* tmpbuf;
        int validFrames = 0;

        for (std::uint32_t i = 0; i < numBufs; i++)
        {
            tmpbuf = &(bufDescTable[i]);
            std::cout << "FrameNo:" << i << " ";
            tmpbuf->Print();

            if (tmpbuf->valid == true)
                validFrames++;
        }

        std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }

}
