package org.kot.test.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import joptsimple.ValueConverter;
import org.kot.test.cascading.tap.LatestSubFolderHfs;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Change detection Cascading job runner.
 * <p/>
 * Parses command line arguments and prepares environment for running Cascading flow.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 04/04/15 01:35
 */
public class CDRunner {

	/* Expected field declaration */
	static final Fields FIELDS = new Fields(new String[] {"id", "name", "timestamp", "text"}, new Type[] {int.class, String.class, long.class, String.class});

	static final Fields KEY = FIELDS.select(new Fields("id"));

	/**
	 * Command line parser
	 */
	static final OptionParser cli = new OptionParser() {{
		final PathStringConverter pathConverter = new PathStringConverter(AppProps.getApplicationID(null));

		accepts("in", "Input file / folder to process").withRequiredArg().required();
		accepts("out", "Output folder with data has been changed since last run (vs. history)").withRequiredArg().withValuesConvertedBy(pathConverter).required();
		accepts("history", "Folder with history").withRequiredArg().required();
		accepts("history-format", "Sub-folder format in history for a new").withRequiredArg().withValuesConvertedBy(pathConverter).defaultsTo("{time}-{app-id}");
		accepts("assembly", "CD assembly to use ('join', 'merge' or dumb as default)").withRequiredArg().withValuesConvertedBy(new AssemblyParamConverter())
				.defaultsTo(new DumbChangeDetection(FIELDS));
		acceptsAll(Arrays.asList("?", "h"), "Print this screen").forHelp();
	}};

	private final FlowDef flowDef;

	private final String name;

	public CDRunner(final OptionSet options) {
		Pipe data = new Pipe("data");
		Pipe history = new Pipe("history");
		SubAssembly assembly = ((ChangeDetection)options.valueOf("assembly"))
				.the(history).mergeWith(data, KEY).compare("text");
		final Pipe[] tails = assembly.getTails();

		name = assembly.getClass().getSimpleName();

		final TextDelimited scheme = new TextDelimited(FIELDS, true, ":");
		final String in = (String) options.valueOf("in");
		final String out = (String) options.valueOf("out");
		final String chronicle = (String) options.valueOf("history");
		String subChronicle = (String) options.valueOf("history-format");
		if (!subChronicle.startsWith("/")) {
			subChronicle = "/" + subChronicle;
		}
		flowDef = FlowDef.flowDef()
				.setName("change-detector")
				.addSource(data, new Hfs(scheme, in))
				.addSource(history, new LatestSubFolderHfs(scheme, chronicle))
				.addTailSink(tails[0], new Hfs(scheme, chronicle + subChronicle, SinkMode.REPLACE))
				.addTailSink(tails[1], new Hfs(scheme, out, SinkMode.REPLACE));

	}

	public FlowDef flowDef() {
		return flowDef;
	}

	@Override
	public String toString() {
		return name;
	}

	public static void main(String[] args) throws IOException {
		final OptionSet options = cli.parse(args);
		if (options.has("?")) {
			cli.printHelpOn(System.err);
			System.exit(13);
		}

		final CDRunner runner = new CDRunner(options);
		FlowDef flowDef = runner.flowDef();

		/* execute a flow */
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, CDRunner.class);
		Flow<?> flow = new Hadoop2MR1FlowConnector(properties).connect(flowDef);
		flow.writeDOT(runner.toString() + ".DOT");
		flow.complete();
	}

	/**
	 * Parameter to assembly implementation converter
	 */
	static class AssemblyParamConverter implements ValueConverter<ChangeDetection> {

		static final Pattern MERGE = Pattern.compile(".*Merge.*", Pattern.CASE_INSENSITIVE);

		static final Pattern JOIN = Pattern.compile(".*Join.*", Pattern.CASE_INSENSITIVE);

		@Override
		public ChangeDetection convert(final String value) {
			if (JOIN.matcher(value).matches()) {
				return new ChangeDetectionByJoin(FIELDS);
			}
			if (MERGE.matcher(value).matches()) {
				return new ChangeDetectionByMerge(FIELDS);
			}
			return new DumbChangeDetection(FIELDS);
		}

		@Override
		public Class<ChangeDetection> valueType() {
			return ChangeDetection.class;
		}

		@Override
		public String valuePattern() {
			return null;
		}
	}

	/**
	 * Path parameter translator for AppID and current UNIX filetime.
	 */
	static class PathStringConverter implements ValueConverter<String> {

		final String appID;

		private final String timestamp;

		PathStringConverter(String appID) {
			this.appID = appID;
			this.timestamp = String.valueOf(System.currentTimeMillis());
		}

		@Override
		public String convert(final String value) {
			final String result = value.replace("{app-id}", appID).replace("{time}", timestamp);
			if (!result.startsWith("/")) {
				return "/" + result;
			}
			return result;
		}

		@Override
		public Class<String> valueType() {
			return String.class;
		}

		@Override
		public String valuePattern() {
			return null;
		}
	}
}
