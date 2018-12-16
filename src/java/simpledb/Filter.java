package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    Predicate m_predicate;
    OpIterator m_child;
    OpIterator[] m_children;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
    	m_predicate = p;
    	m_child = child;
    	m_children = null;
    }

    public Predicate getPredicate() {
        return m_predicate;
    }

    public TupleDesc getTupleDesc() {
        return m_child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	super.open();
    	m_child.open();
    }

    public void close() {
    	super.close();
    	m_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	m_child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while(m_child.hasNext()) {
        	Tuple next = m_child.next();
        	if (m_predicate.filter(next)) {
    			return next;
    		}
        }
        return null;
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
