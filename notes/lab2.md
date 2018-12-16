### 2.1 Filter and Join
先来看看Filter，中文就是过滤器，其实就是返回满足某种条件的tuple的操作。
看看Filter这个类构造函数：
```java
public Filter(Predicate p, OpIterator child) {
	// some code goes here
}
```
传入了参数OpIterator，这是一个扫描操作的迭代器，例如SeqScan。
传入了参数Predicate，就是上述的某种条件。
所以这个类就是再迭代扫描操作的时候，返回满足Predicate的tuple。
下面就去实现以下Predicate。其他都省略，主要说一下`filter`函数。在构造的时候，我们可以拿到field，操作符，和操作对象分别是`m_field`, `m_op`, `m_operand`。所以这个函数就是比较这个tuple对应的field经过操作符op后与操作对象的关系。在实现Field的时候已经实现了函数`compare`，直接调用即可。
```java
public boolean filter(Tuple t) {
	return t.getField(m_field).compare(m_op, m_operand);
}
```
接着回到Filter，主要就是实现`fetchNext`函数，返回在迭代中下一个满足Predicate条件的tuple。
```java
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
```
