package org.kot.mapping;

import org.hamcrest.Matchers;
import org.junit.Test;

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
		assertThat(mapping.header(), Matchers.aMapWithSize(Matchers.greaterThan(0)));
	}

	@Test
	public void shouldProvideBinders() {
		CsvMapping mapping = CsvMapping.load();
		assertThat(mapping, notNullValue());
		assertThat(mapping.binders(), Matchers.aMapWithSize(Matchers.greaterThan(0)));
	}
}
