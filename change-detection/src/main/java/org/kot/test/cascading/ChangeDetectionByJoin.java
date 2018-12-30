package org.kot.test.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.filter.FilterNull;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.BufferJoin;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import java.util.Iterator;

/**
 * Change detection assembly with minor flow improvements
 * <p>
 * Inspired by {@link DumbChangeDetection "default"} implementation, but incorporate some flow optimizations that suppose to improve the execution time. (Experiments has been done
 * locally, on a <a href="http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation"> "pseudo distributed Hadoop
 * mode"</a> surprisingly shows no difference. Perhaps that the matter of amount of data and execution environment to see the benefit?)
 * <p>
 * Improvements related to custom join routine that together with right joins attempts to update the history stream. This trick, by idea, allows avoid additional reducer on history
 * update that happen in "default" implementation.
 * <p>
 * Running in pseudo-distributed mode on 1 node with 1,000,000 tuples:
 * <pre>
 *     Execution class org.kot.test.cascading.ChangeDetectionByJoinAssembly: 55,885,619,373
 *     Execution class org.kot.test.cascading.ChangeDetectionByJoinAssembly: 71,112,898,210
 *     Execution class org.kot.test.cascading.ChangeDetectionByJoinAssembly: 52,588,219,129
 *     Execution class org.kot.test.cascading.ChangeDetectionByJoinAssembly: 65,990,611,395
 *
 *     Execution class org.kot.test.cascading.ChangeDetectionAssembly: 45,796,704,165
 *     Execution class org.kot.test.cascading.ChangeDetectionAssembly: 80,937,540,315
 *     Execution class org.kot.test.cascading.ChangeDetectionAssembly: 62,145,741,492
 *     Execution class org.kot.test.cascading.ChangeDetectionAssembly: 63,221,260,592
 *
 *     Execution class org.kot.test.cascading.ChangeDetectionByJoinAssembly: 41,972,117,323
 *     Execution class org.kot.test.cascading.ChangeDetectionByJoinAssembly: 71,307,511,959
 *     Execution class org.kot.test.cascading.ChangeDetectionByJoinAssembly: 57,043,688,533
 *     Execution class org.kot.test.cascading.ChangeDetectionByJoinAssembly: 61,492,988,849
 * </pre>
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 04/04/15 17:09
 * @see DumbChangeDetection
 */
public class ChangeDetectionByJoin extends ChangeDetection {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructs the assembly with expected fields declaration of data stream to work with.
	 * @param fields The fields declaration
	 */
	public ChangeDetectionByJoin(Fields fields) {
		super(fields);
	}

	/**
	 * Build flow and returns assembly tails.
	 * <p>
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
		Pipe join = new CoGroup(chronicle, fields.selectPos(byKey), mainstream, byKey, new BufferJoin());
		join = new Every(join, new ChronicleJoiner(fields, sign), Fields.RESULTS);

		Pipe nextHistory = new Pipe("next-history", join);
		nextHistory = new Retain(nextHistory, masked);
		nextHistory = new Rename(nextHistory, masked, fields);

		Pipe changed = new Pipe("changed", join);
		changed = new Retain(changed, fields);
		changed = new Each(changed, fields, new FilterNull());

		setTails(nextHistory, changed);
		return super.getTails();
	}

	/**
	 * Special aggregator of data stream with its historical retrospective.
	 * <p>
	 * Joins the data (right side) with its historical retrospective (left side, as bigger by nature). Produces stream that can be split into:
	 * <ul><li>the stream of updates and</li><li>the new version of history.</li></ul>
	 * <p>
	 * On a join each tuple of data stream is compared with corresponding "historical" view and performs copying the left side (the history stream) with possible update (from right
	 * side) and copies right side if it is not equal to its corresponding left side (i.e. its historical retrospective so meant the data was changed).
	 */
	static class ChronicleJoiner extends BaseOperation<Void> implements Buffer<Void> {

		private static final long serialVersionUID = 1L;

		private final int fieldPos;

		private final Fields schema;

		/**
		 * Constructor of joiner with field declaration (that supposed to be the same on both left and right sides) and field to compare on.
		 * <p>
		 * Constructs the joiner
		 * @param schema The fields schema of input on left side and right side
		 * @param field The field which data is compared on
		 */
		public ChronicleJoiner(Fields schema, Comparable<?> field) {
			super(Fields.join(true, schema, schema));
			this.schema = schema;
			this.fieldPos = schema.getPos(field);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void operate(FlowProcess flowProcess, BufferCall<Void> bufferCall) {
			JoinerClosure joiner = bufferCall.getJoinerClosure();

			Tuple left = null, right = null;

			/* get a comparable value on a left side, if any */
			Iterator<Tuple> lhs = joiner.getIterator(0);
			if (lhs.hasNext()) {
				left = lhs.next();
			}

			/* get a right side, i.e. a new data */
			Iterator<Tuple> rhs = joiner.getIterator(1);
			if (rhs.hasNext()) {
				right = rhs.next();
			}

			final Tuple result = Tuple.size(fieldDeclaration.size());

			if (null != right) {
				if (null == left || !left.getObject(fieldPos).equals(right.getObject(fieldPos))) {
					result.set(fieldDeclaration, schema, right);
					result.set(fieldDeclaration, schema.selectPos(schema), right);
					bufferCall.getOutputCollector().add(result);
					return;
				}
			}
			result.set(fieldDeclaration, schema.selectPos(schema), left);

			bufferCall.getOutputCollector().add(result);
		}
	}
}
