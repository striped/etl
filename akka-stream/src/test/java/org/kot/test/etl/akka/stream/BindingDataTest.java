package org.kot.test.etl.akka.stream;

import akka.util.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.kot.test.etl.akka.stream.Binding;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * TODO Describe me.
 *
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2018-12-23 11:36
 */
@RunWith(Parameterized.class)
public class BindingDataTest {

	private final ByteString data;

	private final Object expected;

	private final Binding.Mapping<ByteString, ?> mapping;

	@Parameters(name = "{index}: Binding {0} into {1} on applying {2}")
	public static Iterable<Object[]> data() {
		Map<String, Binding.Mapping<ByteString, ?>> mappings = Binding.inMappings().stream().collect(Collectors.toMap(
				Binding.Mapping::fromName,
				m -> m,
				(u, v) -> {throw new UnsupportedOperationException("No merge expected");},
				LinkedHashMap::new)
		);
		return Arrays.asList(new Object[][] {
				{null, null, mappings.get("id")},
				{"", null, mappings.get("id")},
				{"-1", null, mappings.get("id")},
				{"1.0", null, mappings.get("id")},
				{"one", null, mappings.get("id")},
				{"1", 1, mappings.get("id")},
				{"1000000", 1_000_000, mappings.get("id")},

				{null, null, mappings.get("col1")},
				{"", "", mappings.get("col1")},
				{"text", "text", mappings.get("col1")},

				{null, null, mappings.get("col2")},
				{"", null, mappings.get("col2")},
				{"1", null, mappings.get("col2")},
				{"text", null, mappings.get("col2")},
				{"1/1/1", null, mappings.get("col2")},
				{"1/13/2018", null, mappings.get("col2")},
				{"34/11/2018", null, mappings.get("col2")},
				{"10/10/2018", LocalDate.of(2018, 10, 10), mappings.get("col2")},


				{null, null, mappings.get("col3")},
				{"", null, mappings.get("col3")},
				{"text", null, mappings.get("col3")},
				{"1/1/1", null, mappings.get("col3")},
				{",0000", null, mappings.get("col3")},
				{".1.3.4", null, mappings.get("col3")},
				{"1", new BigDecimal("1"), mappings.get("col3")},
				{"-100", new BigDecimal("-100"), mappings.get("col3")},
				{"1000000", new BigDecimal("1000000"), mappings.get("col3")},
				{"2000.10", new BigDecimal("2000.10"), mappings.get("col3")},
				{"0.2222222", new BigDecimal("0.2222222"), mappings.get("col3")},

				{null, null, mappings.get("col4")},
				{"", null, mappings.get("col4")},
				{"-1", -1, mappings.get("col4")},
				{"1.0", null, mappings.get("col4")},
				{"one", null, mappings.get("col4")},
				{"1", 1, mappings.get("col4")},
				{"1000000", 1_000_000, mappings.get("col4")},
		});
	}

	public BindingDataTest(String data, Object expected, Binding.Mapping<ByteString, ?> mapping) {
		this.data = (null != data)? ByteString.fromString(data) : null;
		this.expected = expected;
		this.mapping = mapping;
	}

	@Test
	public void test() {
		List<String> mismatch = new ArrayList<>();
		Object actual = mapping.apply(data, mismatch);
		assertThat(actual, is(expected));
	}
}
