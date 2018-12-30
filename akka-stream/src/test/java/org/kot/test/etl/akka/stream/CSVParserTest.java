package org.kot.test.etl.akka.stream;

import akka.util.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kot.test.etl.akka.stream.CSVParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * TODO Describe me.
 *
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2018-12-17 20:18
 */
@RunWith(Parameterized.class)
public class CSVParserTest {

	private final int row;

	private final int col;

	private final ByteString data;

	@Parameterized.Parameters(name = "{index}: {0} x {1} from \"{2}\"")
	public static Iterable<Object[]> lines() {
		return Arrays.asList(new Object[][] {
				{1, 3, "col1,col2,col3"},
				{1, 3, "col1,col2,col3\n"},
				{1, 3, "\"col,with,commas\",col\\\"with\\\"quotas,\"col\nwith\nlf\""},
				{1, 3, "\"col,with,commas\",col\\\"with\\\"quotas,\"col\nwith\nlf\"\n"}
		});
	}

	public CSVParserTest(int row, int col, String data) {
		this.row = row;
		this.col = col;
		this.data = ByteString.fromString(data);
	}

	@Test
	public void test() throws IOException {
		CSVParser parser = new CSVParser(',', '"', '\\', 1024);

		List<List<ByteString>> lines = new ArrayList<>();
		parser.offer(data);
		drainTo(parser, lines);

		assertThat(lines, iterableWithSize(row));
		assertThat(lines, hasItem(iterableWithSize(col)));
	}

	@Test
	public void testDouble() throws IOException {
		CSVParser parser = new CSVParser(',', '"', '\\', 1024);

		ByteString doubled = '\n' != data.last() ?
				 data.concat(ByteString.fromString("\n")).concat(data) : data.concat(data);

		List<List<ByteString>> lines = new ArrayList<>();
		parser.offer(doubled);
		drainTo(parser, lines);

		assertThat(lines, iterableWithSize(row * 2));
		assertThat(lines, hasItem(iterableWithSize(col)));
	}

	private static void drainTo(CSVParser parser, List<List<ByteString>> lines) {
		List<ByteString> line;
		for (line = parser.poll(true); null != line; line = parser.poll(true)) {
			lines.add(line);
		}
		line = parser.poll(false);
		if (null != line) {
			lines.add(line);
		}
	}
}
