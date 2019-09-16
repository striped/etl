package org.kot.mapping;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.iterableWithSize;

/**
 * TODO Describe me.
 *
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2019-07-14 22:05
 */
public class MappingTest {

	@Test
	public void readYaml() throws IOException {
		Yaml yaml = new Yaml();
		try (Reader reader = resource("mapping/csv.yaml")) {
			Mapping mapping = yaml.loadAs(reader, Mapping.class);
			List<Mapping.Entry> columns = mapping.entries()
					.collect(Collectors.toList());
			assertThat(columns, iterableWithSize(3));
			assertThat(columns, everyItem(
					allOf(
							hasProperty("id"),
							hasProperty("name"))));
		}
	}

	private static Reader resource(String name) throws IOException {
		URL url = Thread.currentThread().getContextClassLoader().getResource(name);
		assert null != url : "Failed to find \"" + name + '"';
		return new InputStreamReader(url.openStream());
	}
}
