package org.kot.mapping;

import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * TODO Describe me.
 *
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2019-07-14 23:13
 */
public class CsvMappingTest {

	@Test
	public void shouldExist() {
		CsvMapping mapping = CsvMapping.load();
		assertThat(mapping, notNullValue());
	}

	@Test
	public void shouldProvideHeader() {
		CsvMapping mapping = CsvMapping.load();
		assertThat(mapping, notNullValue());
		assertThat(mapping.header(), aMapWithSize(greaterThan(0)));
	}

	@Test
	public void shouldProvideBinders() {
		CsvMapping mapping = CsvMapping.load();
		assertThat(mapping, notNullValue());
		assertThat(mapping.binders(), aMapWithSize(greaterThan(0)));
	}

	@Test
	public void shouldBindData() {
		Map<String, String> raw = new HashMap<String, String>() {{
			put("ID", "1111");
			put("Name", "Name");
			put("Date", "01/01/1999");
			put("Amount", "123456.90");
		}};

		CsvMapping mapping = CsvMapping.load();

		Map<String, ?> data = raw.entrySet().stream()
				.collect(Collectors.toMap(
						e -> mapping.header().get(e.getKey()),
						e -> mapping.binders().get(e.getKey()).apply(e.getValue())
				));
		assertThat(data, allOf(
				hasEntry(is("id"), instanceOf(Integer.class)),
				hasEntry(is("name"), instanceOf(String.class)),
				hasEntry(is("date"), instanceOf(LocalDate.class)),
				hasEntry(is("amount"), instanceOf(BigDecimal.class))
		));
	}
}
