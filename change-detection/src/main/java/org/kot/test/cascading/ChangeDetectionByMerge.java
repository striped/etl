package org.kot.test.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.lang.reflect.Type;
import java.util.Iterator;

/**
 * Change detection "by merge" assembly.
 * <p>
 * Unlike {@link DumbChangeDetection "simple" approach} and {@link org.kot.test.cascading.ChangeDetectionByJoin by join} implementations, this exploits the merging of data streams
 * and further aggregation by provided keys into single entry. Merging streams are indexed (0 - history, next - 1 and so on) and sorted after grouping in natural order so guarantee
 * that history will be processed first (if there is any). So, the changes might be identified as so:
 * <p>
 * <ul><li>if 1st processed is not from history (i.e. its pretty new data) or</li><li>if it has been identified as not equal to historical</li></ul>
 * <p>
 * New history accumulate all aggregation result.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 04/04/15 17:09
 * @see DumbChangeDetection
 */
public class ChangeDetectionByMerge extends ChangeDetection {

	private static final long serialVersionUID = 1L;

	private Fields target = new Fields(new String[] {"channel"}, new Type[] {int.class});

	/**
	 * Constructs the assembly with main data stream and its fields declaration.
	 * @param fields The fields declaration
	 */
	public ChangeDetectionByMerge(Fields fields) {
		super(fields);
	}

	/**
	 * Build flow and returns assembly tails.
	 * <p>
	 * Tails is lined up in array with following order: <ol><li>tail for a new history and</li><li>tail for changed tuples.</li></ol>
	 * @return The tails list as array
	 */
	public Pipe[] getTails() {
		setPrevious(history, mainstream);

		/* "Sign" history with 0 and main data with 1 */
		Pipe chronicle = new Each(history, fields, new Insert(target, 0), Fields.ALL);
		Pipe incoming = new Each(mainstream, fields, new Insert(target, 1), Fields.ALL);

		/* perform a merge the history with a main stream and group by key fields, sort by channel */
		Pipe join = new GroupBy(Pipe.pipes(chronicle, incoming), byKey, target);
		join = new Every(join, new Merger(fields, sign, target), Fields.RESULTS);

		/* dump all as a next history */
		Pipe nextHistory = new Pipe("next-history", join);
		nextHistory = new Retain(nextHistory, fields);

		/* sieve all except 0 as changed */
		Pipe changed = new Pipe("changed", join);
		changed = new Each(changed, new Sieve(target, 0));
		changed = new Retain(changed, fields);

		setTails(nextHistory, changed);
		return super.getTails();
	}

	/**
	 * Special aggregator of data stream with its historical retrospective.
	 * <p>
	 * Joins the data (right side) with its historical retrospective (left side, as bigger by nature). Produces stream that can be split into: <ul> <li>the stream of updates
	 * and</li> <li>the new version of history.</li> </ul>
	 * <p>
	 * On a join each tuple of data stream is compared with corresponding "historical" view and performs copying the left side (the history stream) with possible update (from right
	 * side) and copies right side if it is not equal to its corresponding left side (i.e. its historical retrospective so meant the data was changed).
	 */
	static class Merger extends BaseOperation<Void> implements Buffer<Void> {

		private static final long serialVersionUID = 1L;

		private final Comparable<?> compareField;

		private final Fields target;

		/**
		 * Constructor of joiner with field declaration (that supposed to be the same on both left and right sides) and field to compare on.
		 * <p>
		 * Constructs the joiner
		 * @param fields The fields of input
		 * @param compareField The field which data is compared on
		 * @param target The field that addressed source (0 - history, 1 - main data)
		 */
		public Merger(Fields fields, Comparable<?> compareField, Fields target) {
			super(fields.append(target));
			this.compareField = compareField;
			this.target = target;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void operate(FlowProcess flowProcess, BufferCall<Void> bufferCall) {
			final Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();

			if (iterator.hasNext()) {
				TupleEntry first = iterator.next();
				Tuple result = detect(first, iterator);
				bufferCall.getOutputCollector().add(result);
			}
		}

		/**
		 * Potentionally can be externalized
		 * @param first The 1st entry in group
		 * @param rest Rest of the group
		 * @return The aggregation result
		 */
		private Tuple detect(final TupleEntry first, final Iterator<TupleEntry> rest) {
			Object value = null;
			int channel = (Integer) first.getObject(target.get(0));
			TupleEntry entry = first;
			while (rest.hasNext()) {
				// choose most valuable from rest?
				if (0 == channel) {
					value = first.getObject(compareField);
				}
				entry = rest.next();
			}
			if (null != value && value.equals(entry.getObject(compareField))) {
				final Tuple tuple = entry.getTupleCopy();
				tuple.set(fieldDeclaration.getPos(target.get(0)), 0);
				return tuple;
			}
			return entry.getTupleCopy();
		}
	}

	/**
	 * Sieves out all tuples that has an specified {@code field} with value equals to {@code expected}
	 */
	static class Sieve extends BaseOperation<Void> implements Filter<Void> {

		private static final long serialVersionUID = 1L;

		private final Comparable<?> field;

		private final int expectedValue;

		public Sieve(Fields field, int expected) {
			this.field = field.get(0);
			this.expectedValue = expected;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public boolean isRemove(final FlowProcess flowProcess, final FilterCall<Void> filterCall) {
			TupleEntry argument = filterCall.getArguments();
			final Object value = argument.getObject(field);
			return null == value || value.equals(expectedValue);
		}
	}
}
