package org.kot.test.cascading.tap;

import cascading.scheme.Scheme;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Latest sub folder tap.
 * <p>
 * Special implementation of {@link cascading.tap.hadoop.Hfs HFS tap} that looks up the latest sub folder within specified path.
 *
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 04/04/15 19:24
 */
public class LatestSubFolderHfs extends Hfs {

	private static final long serialVersionUID = 1L;

	private Comparator<FileStatus> rule;

	/**
	 * Constructs tap with schema and path.
	 *
	 * @param scheme The tap scheme
	 * @param path   The path
	 */
	@SuppressWarnings("rawtypes")
	public LatestSubFolderHfs(final Scheme<Configuration, RecordReader, OutputCollector, ?, ?> scheme, final String path) {
		super(scheme, path);
		rule = new Latest();
	}

	@Override
	protected void sourceConfInitAddInputPaths(Configuration conf, Iterable<Path> qualifiedPaths) {
		try {
			List<Path> latest = new ArrayList<>();
			for (Path path : qualifiedPaths) {
				latest.add(resolveChild(getFileSystem(conf), path));
			}
			super.sourceConfInitAddInputPaths(conf, latest);
		} catch (IOException e) {
			throw new TapException("May not add paths, found: " + qualifiedPaths);
		}
	}

	/**
	 * Resolves the latest sub folder within specified path if there is any.
	 *
	 * @param fs   The file system to be used
	 * @param path The path with which latest sub folder is requested
	 * @return Return the latest (by its modification time) sub folder within specified path
	 * @throws IOException If unexpected I/O error occurred
	 */
	private Path resolveChild(FileSystem fs, final Path path) throws IOException {
		if (!fs.getFileStatus(path).isDirectory()) {
			return path;
		}
		final FileStatus[] statuses = fs.globStatus(path.suffix("/*"));
		if (0 == statuses.length) {
			return path;
		}
		FileStatus result = null;
		for (final FileStatus status : statuses) {
			if (status.isDirectory() && (null == result || 0 > rule.compare(result, status))) {
				result = status;
			}
		}
		return (null == result)? path: result.getPath();
	}

	/**
	 * File by its modification time comparator
	 */
	static class Latest implements Comparator<FileStatus>, Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public int compare(final FileStatus one, final FileStatus another) {
			return Long.compare(one.getModificationTime(), another.getModificationTime());
		}
	}
}
