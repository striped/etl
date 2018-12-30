package org.kot.test.cascading.tap.local;

import cascading.flow.FlowProcess;
import cascading.scheme.NullScheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryChainIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * In memory tap implementation for unit testing.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 3/12/2014 10:44
 */
public class InMemory extends Tap<Properties, InputStream, OutputStream> implements Iterable<Tuple> {

	private static final long serialVersionUID = 1L;

	private final List<Tuple> tuples;

	public static InMemory sink(final Fields fields) {
		return new InMemory(fields, SinkMode.REPLACE);
	}

	public static InMemory source(final Fields fields, final List<Tuple> tuples) {
		return new InMemory(fields, tuples);
	}

	@Override
	public String getIdentifier() {
		return id(this);
	}

	@Override
	public boolean deleteResource(final Properties conf) throws IOException {
		tuples.clear();
		return true;
	}

	@Override
	public boolean createResource(final Properties conf) throws IOException {
		tuples.clear();
		return true;
	}

	@Override
	public boolean resourceExists(Properties conf) throws IOException {
		return true;
	}

	@Override
	public long getModifiedTime(Properties conf) throws IOException {
		return System.currentTimeMillis();
	}

	@Override
	public TupleEntryIterator openForRead(final FlowProcess<? extends Properties> flowProcess, final InputStream input) throws IOException {
		final Iterator<Tuple> iterator = tuples.iterator();
		return new TupleEntryChainIterator(getSourceFields(), iterator);
	}

	@Override
	public TupleEntryCollector openForWrite(final FlowProcess<? extends Properties> flowProcess, final OutputStream output) throws IOException {
		return new TupleEntryCollector(getSinkFields()) {
			@Override
			protected void collect(final TupleEntry tupleEntry) throws IOException {
				tuples.add(tupleEntry.getTupleCopy());
			}
		};
	}

	@Override
	public Iterator<Tuple> iterator() {
		return tuples.iterator();
	}

	/**
	 * Constructor with fields declaration and optional tuples (for source)
	 * @param fields The field declaration
	 * @param tuples The list of tuples, in case if it supposed to be used as tap source
	 */
	private InMemory(Fields fields, List<Tuple> tuples) {
		super(new NullScheme<>(fields, fields));
		this.tuples = tuples;
	}

	protected InMemory(Fields fields, SinkMode sinkMode) {
		super(new NullScheme<>(fields, fields), sinkMode);
		this.tuples = new ArrayList<>();
	}
}
