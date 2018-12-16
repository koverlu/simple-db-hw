package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    TransactionId m_tid;
    OpIterator m_child;
    TupleDesc m_td;
    int m_tableId;
    int m_count;
    boolean m_accessed;
    OpIterator[] m_children;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
    	m_tid = t;
    	m_child = child;
    	m_tableId = tableId;
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
                Database.getBufferPool().insertTuple(m_tid, m_tableId, next);
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (m_accessed)
            return null;
        m_accessed = true;
        Tuple insertedNum = new Tuple(getTupleDesc());
        insertedNum.setField(0,new IntField(m_count));
        return insertedNum;
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
