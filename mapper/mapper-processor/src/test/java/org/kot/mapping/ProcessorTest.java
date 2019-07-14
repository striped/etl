package org.kot.mapping;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;

/**
 * TODO Describe me.
 *
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2019-07-14 22:05
 */
public class ProcessorTest {

	@Test
	public void readYaml() throws IOException {
		Yaml yaml = new Yaml();
		List<Map<String, String>> columns = yaml.loadAs(resource("mapping/csv.yaml"), List.class);
		assertThat(columns, iterableWithSize(3));
	}

	private static Reader resource(String name) throws IOException {
		URL url = Thread.currentThread().getContextClassLoader().getResource(name);
		assert null != url : "Failed to find \"" + name + '"';
		return new InputStreamReader(url.openStream());
	}
}
