package simpledb;

import java.awt.RenderingHints;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	File file;
	TupleDesc td;
	int numPages;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
    	this.file = f;
        this.td = td;
        numPages = (int)(file.length() / BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	int pageSize = BufferPool.getPageSize();
    	byte[] data = new byte[pageSize];
    	Page page = null;
    	try {
    		RandomAccessFile raf = new RandomAccessFile(this.file, "r");
    		int offset = pid.getPageNumber() * pageSize;
    		raf.seek(offset);
    		raf.read(data, 0, pageSize);
    		page = new HeapPage((HeapPageId)pid, data);
    		raf.close();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
            byte[] data = page.getPageData();
            raf.write(data);
            raf.close();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	ArrayList<Page> retPages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
        	HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), null);
        	if(page.getNumEmptySlots() > 0) {
        		page.insertTuple(t);
        		page.markDirty(true, tid);
        		retPages.add(page);
        		return retPages;  
        	}        	     	
        }
        
		// Need a new page.
        byte[] data = new byte[BufferPool.getPageSize()];
        for (int i = 0; i < BufferPool.getPageSize(); i++) {
        	data[i] = 0;
        }
        HeapPageId npid = new HeapPageId(getId(), numPages());
        HeapPage newPage = new HeapPage(npid, data);
        numPages++;
        this.writePage(newPage);
        
        // Get the page from buffer pool. It's not the same object from newPage.
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, npid, null);
        page.insertTuple(t);
        page.markDirty(true, tid);
        retPages.add(newPage);
        
        return retPages; 
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	ArrayList<Page> retPages = new ArrayList<>();
    	PageId pid = t.getRecordId().getPageId();
        for (int i = 0; i < numPages(); i++) {
        	if (i == pid.getPageNumber()) {
        		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        		page.deleteTuple(t);
        		retPages.add(page);
        		return retPages;
        	}
        }
        throw new DbException("deleteTuple: can't find pid of tuple in page file");
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new HeapFileIterator(tid);	
    }
    
    private class HeapFileIterator implements DbFileIterator {
    	private int pageNo;
    	private Iterator<Tuple> tuplesInPage;
    	private TransactionId tid;
    	
        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }
        
    	public Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws TransactionAbortedException, DbException {
    		HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }
    	
    	@Override
		public void open() throws DbException, TransactionAbortedException{
    		pageNo = 0;
    		HeapPageId pid = new HeapPageId(getId(), pageNo);
			tuplesInPage = getTuplesInPage(pid);
		}
    	
    	@Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
    		// The iterator is closed
    		if (tuplesInPage == null) {
    			return false;
            }
    		
    		// Find next tuple in this page.
    		if (tuplesInPage.hasNext()) {    			
                return true;
            }
    		
    		// Move to next page.
            if (pageNo < numPages() - 1) {
            	pageNo++;
                HeapPageId pid = new HeapPageId(getId(), pageNo);
                tuplesInPage = getTuplesInPage(pid);
                return tuplesInPage.hasNext();
            } else {
            	return false;
            }
    	}
    	
    	@Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tuplesInPage != null) {
            	return tuplesInPage.next();                
            }else {
            	throw new NoSuchElementException();
            }            
        }
    	
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
        	pageNo = 0;
            tuplesInPage = null;
        }
    }
    

}

