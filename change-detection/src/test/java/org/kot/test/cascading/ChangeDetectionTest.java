package org.kot.test.cascading;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.platform.local.LocalPlatform;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kot.test.cascading.tap.local.InMemory;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.runners.Parameterized.Parameters;

/**
 * Test suite for cascading assembly.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 04/04/15 20:09
 */
@RunWith(Parameterized.class)
public class ChangeDetectionTest {

	private static final Fields FIELDS = new Fields(
			new String[] {"id", "timestamp", "name", "text"},
			new Type[] {String.class, long.class, String.class, String.class}
	);

	private static final Fields KEYS = new Fields(
			new String[] {"id"},
			new Type[] {String.class}
	);

	private static final Fields TIMESTAMP = new Fields(
			new String[] {"timestamp"},
			new Type[] {long.class}
	);

	private final int outSize;

	private final int histSize;

	private final List<Tuple> in;

	private final List<Tuple> history;

	@Parameters(name = "{index}: expect {0} changed from {1}")
	public static Iterable<Object[]> tuples() {
		/* define possible scenarios */
		final List<Tuple> history = asList(new Tuple("1", 1L, "first", "same"));
		final List<Tuple> history2 = asList(new Tuple("1", 1L, "first", "same"), new Tuple("2", 1L, "first", "same"));
		return Arrays.asList(new Object[][] {
				{1, 1, asList(new Tuple("1", 2L, "first", "same")), Collections.emptyList()},
				{0, 1, asList(new Tuple("1", 1L, "first", "same")), history},
				{1, 1, asList(new Tuple("1", 2L, "first", "another")), history},
				{1, 2, asList(new Tuple("1", 2L, "first", "same"), new Tuple("2", 2L, "second", "same")), history},
				{2, 2, asList(new Tuple("1", 2L, "first", "another"), new Tuple("2", 2L, "second", "same")), history},
				{1, 2, asList(new Tuple("1", 2L, "first", "another"), new Tuple("2", 2L, "second", "same")), history2},
				{2, 2, asList(new Tuple("1", 2L, "first", "another"), new Tuple("2", 2L, "second", "another")), history2},
		});
	}

	public ChangeDetectionTest(int expectedOut, int expectedHistory, List<Tuple> in, List<Tuple> history) {
		this.outSize = expectedOut;
		this.histSize = expectedHistory;
		this.in = in;
		this.history = history;
	}

	@Test
	public void testByJoin() {
		Pipe mainPipe = new Pipe("main");
		Pipe histPipe = new Pipe("history");
		ChangeDetection assembly = new ChangeDetectionByJoin(FIELDS);
		final Pipe[] tails = assembly.the(histPipe).mergeWith(mainPipe, KEYS).compare("text").getTails();
		final InMemory outTap = InMemory.sink(FIELDS);
		final InMemory nextTap = InMemory.sink(FIELDS);
		FlowDef flowDef = FlowDef.flowDef()
				.addSource(histPipe, InMemory.source(FIELDS, history))
				.addSource(mainPipe, InMemory.source(FIELDS, in))
				.addTailSink(tails[0], nextTap)
				.addTailSink(tails[1], outTap);

		final FlowConnector connector = new LocalPlatform().getFlowConnector();
		connector.connect(flowDef).complete();

		assertThat(outTap, iterableWithSize(outSize));
		assertThat(nextTap, iterableWithSize(histSize));
	}

	@Test
	public void testByMerge() {
		Pipe mainPipe = new Pipe("main");
		Pipe histPipe = new Pipe("history");
		ChangeDetection assembly = new ChangeDetectionByMerge(FIELDS);
		final Pipe[] tails = assembly.the(histPipe).mergeWith(mainPipe, KEYS).compare("text").getTails();
		final InMemory outTap = InMemory.sink(FIELDS);
		final InMemory nextTap = InMemory.sink(FIELDS);
		FlowDef flowDef = FlowDef.flowDef()
				.addSource(histPipe, InMemory.source(FIELDS, history))
				.addSource(mainPipe, InMemory.source(FIELDS, in))
				.addTailSink(tails[0], nextTap)
				.addTailSink(tails[1], outTap);

		final FlowConnector connector = new LocalPlatform().getFlowConnector();
		connector.connect(flowDef).complete();

		assertThat(outTap, iterableWithSize(outSize));
		assertThat(nextTap, iterableWithSize(histSize));
	}

	@Test
	public void testDumb() {
		Pipe mainPipe = new Pipe("main");
		Pipe histPipe = new Pipe("history");
		ChangeDetection assembly = new DumbChangeDetection(FIELDS).withTimestamp(TIMESTAMP);
		final Pipe[] tails = assembly.the(histPipe).mergeWith(mainPipe, KEYS).compare("text").getTails();
		final InMemory outTap = InMemory.sink(FIELDS);
		final InMemory nextTap = InMemory.sink(FIELDS);
		FlowDef flowDef = FlowDef.flowDef()
				.addSource(histPipe, InMemory.source(FIELDS, history))
				.addSource(mainPipe, InMemory.source(FIELDS, in))
				.addTailSink(tails[0], nextTap)
				.addTailSink(tails[1], outTap);

		final FlowConnector connector = new LocalPlatform().getFlowConnector();
		connector.connect(flowDef).complete();

		assertThat(outTap, iterableWithSize(outSize));
		assertThat(nextTap, iterableWithSize(histSize));
	}

}
