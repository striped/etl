package org.kot.test.cascading.tap;

import cascading.scheme.NullScheme;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;
import static org.junit.runners.Parameterized.Parameters;

/**
 * Test suite for {@link org.kot.test.cascading.tap.LatestSubFolderHfs}.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 06/04/15 16:03
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class LatestSubFolderHfsTest {

	@ClassRule
	public static HadoopDFS hdfs = new HadoopDFS();

	private final Path root;

	@Parameters(name = "{index}: Check {1} on {0}")
	public static Iterable<Object[]> args() throws IOException {
		File file = new File(System.getProperty("work.directory", "target"), "lfs").getAbsoluteFile();
		FileUtil.fullyDelete(file);
		return Arrays.asList(new Object[][] {
				{FileSystem.getLocal(new Configuration(true)), file.toString()},
				{FileSystem.get(hdfs.baseURI(), new Configuration(true)), hdfs.baseURI().toString() + "/"}
		});
	}

	private final FileSystem system;

	public LatestSubFolderHfsTest(FileSystem system, String root) throws IOException {
		this.system = system;
		this.root = new Path(root, "data");
		this.system.mkdirs(this.root);
	}

	/**
	 * Checks if there is no child, accept itself as source
	 * @throws IOException On unexpected I/O failure
	 */
	@Test
	public void test_00() throws IOException {
		final Path path = root;
		system.mkdirs(path);
		FileStatus[] status = system.listStatus(root);
		assumeThat(status, arrayWithSize(0));

		final JobConf conf = new JobConf();
		final LatestSubFolderHfs folderHfs = new LatestSubFolderHfs(new NullScheme<>(), root.toString());
		folderHfs.sourceConfInit(null, conf);

		assertThat(conf.get(FileInputFormat.INPUT_DIR), endsWith(path.toString()));
	}

	/**
	 * Checks if there is no child, accept itself as source
	 * @throws IOException On unexpected I/O failure
	 */
	@Test
	public void test_01() throws IOException {
		final Path path = root;
		system.createNewFile(path.suffix("/part-00000"));
		FileStatus[] status = system.listStatus(root);
		assumeThat(status, arrayWithSize(1));

		final JobConf conf = new JobConf();
		final LatestSubFolderHfs folderHfs = new LatestSubFolderHfs(new NullScheme<>(), root.toString());
		folderHfs.sourceConfInit(null, conf);

		assertThat(conf.get(FileInputFormat.INPUT_DIR), endsWith(path.toString()));
	}

	/**
	 * Checks if there is only child, accept it as source
	 * @throws IOException On unexpected I/O failure
	 */
	@Test
	public void test_02() throws IOException {
		final Path path = new Path(root, "1");
		system.mkdirs(path);
		FileStatus[] status = system.listStatus(root);
		assumeThat(status, arrayWithSize(2));
		System.out.println(Arrays.asList(status));

		final JobConf conf = new JobConf();
		final LatestSubFolderHfs folderHfs = new LatestSubFolderHfs(new NullScheme<>(), root.toString());
		folderHfs.sourceConfInit(null, conf);

		assertThat(conf.get(FileInputFormat.INPUT_DIR), endsWith(path.toString()));

		 // ensure modification time is different
		system.setTimes(path, System.currentTimeMillis() - 1000, System.currentTimeMillis());
	}

	/**
	 * Checks if there is more than one child, accept the latest (as only created) as source
	 * @throws IOException On unexpected I/O failure
	 */
	@Test
	public void test_03() throws IOException, InterruptedException {
		final Path path = new Path(root, "2");
		system.mkdirs(path);
		FileStatus[] status = system.listStatus(root);
		assumeThat(status, arrayWithSize(3));
		System.out.println(Arrays.asList(status));

		final JobConf conf = new JobConf();
		final LatestSubFolderHfs folderHfs = new LatestSubFolderHfs(new NullScheme<>(), root.toString());
		folderHfs.sourceConfInit(null, conf);

		assertThat(conf.get(FileInputFormat.INPUT_DIR), endsWith(path.toString()));
	}

	/**
	 * Mini HDFS pseudo distributed cluster for testing purposes
	 */
	static class HadoopDFS extends ExternalResource {

		private MiniDFSCluster cluster;

		private IOException cause;

		public HadoopDFS() {
			File dir = new File(System.getProperty("work.directory", "target"), "hdfs").getAbsoluteFile();
			FileUtil.fullyDelete(dir);
			final Configuration configuration = new Configuration(true);
			configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dir.getAbsolutePath());
			try {
				cluster = new MiniDFSCluster.Builder(configuration).build();
			} catch (IOException e) {
				cause = e;
			}
		}

		public URI baseURI() {
			return cluster.getURI();
		}

		@Override
		protected void before() throws Throwable {
			if (null != cause) {
				throw cause;
			}
		}

		@Override
		protected void after() {
			cluster.shutdown();
		}
	}
}
