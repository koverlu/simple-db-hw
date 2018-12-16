package simpledb;

import java.util.*;

import com.sun.org.apache.bcel.internal.generic.RETURN;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    OpIterator m_child;
    int m_aField;
    int m_gField;
    Aggregator.Op m_aop;
    Aggregator m_aggr;
    OpIterator m_aggrIter;
    OpIterator[] m_children;
    TupleDesc m_td;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
		m_child = child;
		m_aField = afield;
		m_gField = gfield;
		m_aop = aop;
		m_children = null;
		
		// Aggregate type should int here, other types are not supported.
		if(m_gField != Aggregator.NO_GROUPING) {
    		Type[] typeAr = {m_child.getTupleDesc().getFieldType(m_gField), Type.INT_TYPE};
    		String[] NameAr = {groupFieldName(), aggregateFieldName()};
        	m_td = new TupleDesc(typeAr, NameAr);
		} else {
			Type[] typeAr = {Type.INT_TYPE};
			String[] NameAr = {aggregateFieldName()};
			m_td = new TupleDesc(typeAr, NameAr);
		}
		
		if(m_child.getTupleDesc().getFieldType(m_aField) == Type.INT_TYPE)
			m_aggr = new IntegerAggregator(gfield, afield, aop, m_td);
		else
			m_aggr = new StringAggregator(gfield, afield, aop, m_td);
		
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	return m_gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {		
    	if(m_gField != Aggregator.NO_GROUPING)
    		return m_child.getTupleDesc().getFieldName(m_gField);
    	else
    		return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	return m_aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	return m_child.getTupleDesc().getFieldName(m_aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	return m_aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
		super.open();
		m_child.open();
		// Merge all tuples into group
    	while(m_child.hasNext()) {
    		m_aggr.mergeTupleIntoGroup(m_child.next());
    	}
    	m_aggrIter = m_aggr.iterator();
    	m_aggrIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (m_aggrIter.hasNext())
    		return m_aggrIter.next();
    	else
    		return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	m_aggrIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
		return m_td;
    }

    public void close() {
    	super.close();
    	m_child.close();
    	m_aggrIter.close();
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
