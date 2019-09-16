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
public class AdvFileMappingTest {

	@Test
	public void shouldExist() {
		AdvFileMapping mapping = AdvFileMapping.load();
		assertThat(mapping, notNullValue());
	}

	@Test
	public void shouldProvideHeader() {
		AdvFileMapping mapping = AdvFileMapping.load();
		assertThat(mapping, notNullValue());
		assertThat(mapping.header(), aMapWithSize(greaterThan(0)));
	}

	@Test
	public void shouldProvideBinders() {
		AdvFileMapping mapping = AdvFileMapping.load();
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

		AdvFileMapping mapping = AdvFileMapping.load();

		List<String> errors = new ArrayList<>();
		Map<String, ?> data = raw.entrySet().stream()
				.collect(Collectors.toMap(
						e -> mapping.header().get(e.getKey()),
						e -> {
							String id = mapping.header().get(e.getKey());
							AdvFileMapping.Binder<String, ?> binder = mapping.binders().get(id);
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

		AdvFileMapping mapping = AdvFileMapping.load();

		List<String> errors = new ArrayList<>();
		Map<String, ?> data = raw.entrySet().stream()
				.collect(Collectors.toMap(
						e -> mapping.header().get(e.getKey()),
						e -> {
							AdvFileMapping.Binder<String, ?> binder = mapping.binders().get(e.getKey());
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

	@Test
	public void shouldProvideSerializers() {
		AdvFileMapping mapping = AdvFileMapping.load();
		assertThat(mapping, notNullValue());
		assertThat(mapping.serializers(), aMapWithSize(greaterThan(0)));
	}

	@Test
	public void shouldSerializeData() {
		Map<String, ?> raw = new HashMap<String, Object>() {{
			put("id", 1111);
			put("name", "Name");
			put("date", LocalDate.of(1999, 1, 1));
			put("amount", new BigDecimal("123456.90"));
		}};

		AdvFileMapping mapping = AdvFileMapping.load();
		Map<String, String> header = mapping.header().entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

		List<String> errors = new ArrayList<>();
		Map<String, ?> data = raw.entrySet().stream()
				.collect(Collectors.toMap(
						e -> header.get(e.getKey()),
						e -> {
							String id = e.getKey();
							Object value = e.getValue();
							try {
								return mapping.serializer(id).bind(value);
							} catch (Exception ex) {
								errors.add("" + e.getKey() + ": failure to serialize " + e.getValue());
								return e.getValue();
							}
						}
				));
		assertThat(errors, emptyIterable());
		assertThat(data, allOf(
				hasEntry(is("ID"), is("1111")),
				hasEntry(is("Name"), is("Name")),
				hasEntry(is("Date"), is("01/01/1999")),
				hasEntry(is("Amount"), is("123456.90"))
		));
	}
}
