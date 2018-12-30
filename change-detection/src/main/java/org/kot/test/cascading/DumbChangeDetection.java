package org.kot.test.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.aggregator.First;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.RightJoin;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Change detection assembly.
 * <p>
 * Implements higher abstraction to perform an comparison of digested data with its "historical" retrospective with intention to identify the changes and update the "history" for
 * next run. In a formal, the calculus may be expressed as a recurrent formula:
 * <pre>
 *     y(x) = {x} \ {h(x)}
 *     h'(x) = {x} U {h(x)}
 * </pre>
 * Here {@code y(x)} is a set of data elements that has been changed since last execution. It is relative complement to the history (i.e. {@code h(x)}) that aggregate all changes
 * that has been identified since 1st run. The {@code h'(x)} is an updated history that also includes changes identified on this step.
 * <p>
 * Realized in form of builder, skeleton of which provided by Cascading' {@link cascading.pipe.SubAssembly sub-assembly} (sounds like a bad naming?).
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 04/04/15 15:29
 */
public class DumbChangeDetection extends ChangeDetection {

	private static final long serialVersionUID = 1L;

	private Fields timestamp;

	/**
	 * Constructs the assembly with expected fields declaration of data stream to work with.
	 * @param fields The fields declaration
	 */
	public DumbChangeDetection(Fields fields) {
		super(fields);
	}

	/**
	 * Instruct to use the {@code timestamp} for identifying latest variant of processing data (as an history update).
	 * @param timestamp The field(s) (beside {@link #DumbChangeDetection(cascading.tuple.Fields) main stream field declaration}) that may be used as a timestamp. New history will
	 * include tuples that is latest, according to this fields.
	 * @return This builder instance
	 */
	public DumbChangeDetection withTimestamp(Fields timestamp) {
		assert fields.contains(timestamp): "Field with timestamp expected beside stream field declaration";
		this.timestamp = timestamp;
		return this;
	}

	/**
	 * Build flow and returns assembly tails.
	 * <p>
	 * Tails is lined up in array with following order: <ol><li>tail for a new history and</li><li>tail for changed tuples.</li></ol>
	 * @return The tails list as array
	 */
	public Pipe[] getTails() {
		setPrevious(history, mainstream);

		/* Rename history for join with main */
		final Fields masked = Fields.mask(fields, fields);
		Pipe chronicle = new Rename(history, fields, masked);

		/* perform a right join the history with a main stream and filter out unchanged */
		Pipe join = new CoGroup(chronicle, fields.selectPos(byKey), mainstream, byKey, new RightJoin());
		join = new Each(join, new IsChanged(fields.getPos(sign), sign));

		// detect changes and sink only updates
		Pipe changed = new Retain(join, fields);

		Pipe nextHistory = new Pipe("next-history", history);
		nextHistory = new GroupBy(new Pipe[] {nextHistory, changed}, byKey, timestamp, true);
		nextHistory = new Every(nextHistory, new First(), Fields.RESULTS);

		setTails(nextHistory, changed);
		return super.getTails();
	}

	/**
	 * Compares 2 fields on equality and removes tuple if and only if {@code right} is the same as {@code left}.
	 */
	static class IsChanged extends BaseOperation<Void> implements Filter<Void> {

		private static final long serialVersionUID = 1L;

		private final Comparable<?> left;

		private final Comparable<?> right;

		public IsChanged(Comparable<?> left, Comparable<?> right) {
			this.left = left;
			this.right = right;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public boolean isRemove(final FlowProcess flowProcess, final FilterCall<Void> filterCall) {
			TupleEntry argument = filterCall.getArguments();
			final Object value = argument.getObject(left);
			return null != value && value.equals(argument.getObject(right));
		}
	}
}
