package simpledb;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int m_gbField;
    int m_aField;
    Op m_op;
    TupleDesc m_td;
    HashMap<Field, Integer> m_gVal2agVal;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, int afield, Op what, TupleDesc td) {
        m_gbField = gbfield;
        m_aField = afield;
        m_op = what;
        m_td = td;
        m_gVal2agVal = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field gField = m_gbField == NO_GROUPING ? null : tup.getField(m_gbField);
    	boolean bHasKey = m_gVal2agVal.containsKey(gField);
    	Integer newValue = 0;
    	switch (m_op) {
        case COUNT:
        	 newValue = bHasKey ? m_gVal2agVal.get(gField) + 1 : 1;
        	 break;
        default:
        	throw new UnsupportedOperationException("Not supported in lab2!\n");
    	}
    	m_gVal2agVal.put(gField, newValue);    	
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
    	Vector<Tuple> tuples = new Vector<>();
        for (Map.Entry<Field, Integer> g2a : m_gVal2agVal.entrySet()) {
            Tuple t = new Tuple(m_td);
            if (m_gbField == NO_GROUPING) {
                t.setField(0, new IntField(g2a.getValue()));
            } else {
                t.setField(0, g2a.getKey());
                t.setField(1, new IntField(g2a.getValue()));
            }
            tuples.add(t);
        }
        return new TupleIterator(m_td, tuples);
    }

}
