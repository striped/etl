package org.kot.mapping;

import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * TODO Describe me.
 *
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2019-07-14 23:13
 */
public class AdvFileReadOnlyMappingTest {

	@Test
	public void shouldExist() {
		AdvFileReadOnlyMapping mapping = AdvFileReadOnlyMapping.load();
		assertThat(mapping, notNullValue());
	}

	@Test
	public void shouldProvideHeader() {
		AdvFileReadOnlyMapping mapping = AdvFileReadOnlyMapping.load();
		assertThat(mapping, notNullValue());
		assertThat(mapping.header(), aMapWithSize(greaterThan(0)));
	}

	@Test
	public void shouldProvideBinders() {
		AdvFileReadOnlyMapping mapping = AdvFileReadOnlyMapping.load();
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

		AdvFileReadOnlyMapping mapping = AdvFileReadOnlyMapping.load();

		List<String> errors = new ArrayList<>();
		Map<String, ?> data = raw.entrySet().stream()
				.collect(Collectors.toMap(
						e -> mapping.header().get(e.getKey()),
						e -> {
							String id = mapping.header().get(e.getKey());
							AdvFileReadOnlyMapping.Binder<String, ?> binder = mapping.binders().get(id);
							try {
								return binder.bind(e.getValue());
							} catch (Exception ex) {
								errors.add("" + e.getKey() + ": failure to bind from " + e.getValue());
								return e.getValue();
							}
						}
				));
		assertThat(errors, emptyIterable());
		assertThat(data, allOf(
				hasEntry(is("id"), instanceOf(Integer.class)),
				hasEntry(is("name"), instanceOf(String.class)),
				hasEntry(is("date"), instanceOf(LocalDate.class)),
				hasEntry(is("amount"), instanceOf(BigDecimal.class))
		));
	}

	@Test
	public void shouldBindBrokenData() {
		Map<String, String> raw = new HashMap<String, String>() {{
			put("ID", "ttt");
			put("Name", "Name");
			put("Date", "1/01/1999");
			put("Amount", "1yy23456.90");
		}};

		AdvFileReadOnlyMapping mapping = AdvFileReadOnlyMapping.load();

		List<String> errors = new ArrayList<>();
		Map<String, ?> data = raw.entrySet().stream()
				.collect(Collectors.toMap(
						e -> mapping.header().get(e.getKey()),
						e -> {
							AdvFileReadOnlyMapping.Binder<String, ?> binder = mapping.binders().get(e.getKey());
							try {
								return binder.bind(e.getValue());
							} catch (Exception ex) {
								errors.add("" + e.getKey() + ": failure to bind from " + e.getValue());
								return e.getValue();
							}
						},
						(o, n) -> n
				));
		assertThat(errors, iterableWithSize(greaterThan(0)));
		assertThat(data, allOf(
				hasEntry(is("id"), notNullValue()),
				hasEntry(is("name"), notNullValue()),
				hasEntry(is("date"), notNullValue()),
				hasEntry(is("amount"), notNullValue())
		));
	}
}
