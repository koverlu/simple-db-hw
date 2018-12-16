package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    TransactionId m_tid;
    OpIterator m_child;
    TupleDesc m_td;
    int m_count;
    boolean m_accessed;
    OpIterator[] m_children;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
    	m_tid = t;
    	m_child = child;
    	m_children = null;
    	m_count = 0;
    	m_accessed = false;
    	m_td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{null});
    }

    public TupleDesc getTupleDesc() {
        return m_td;
    }

    public void open() throws DbException, TransactionAbortedException {
    	m_accessed = false;
    	super.open();
        m_child.open();
        while (m_child.hasNext()) {
            Tuple next = m_child.next();
            try {
                Database.getBufferPool().deleteTuple(m_tid, next);
                m_count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        super.close();
        m_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	m_accessed = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (m_accessed)
            return null;
        m_accessed = true;
        Tuple deletedNum = new Tuple(getTupleDesc());
        deletedNum.setField(0,new IntField(m_count));
        return deletedNum;
    }

    @Override
    public OpIterator[] getChildren() {
        return m_children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	m_children = children;
    }

}
