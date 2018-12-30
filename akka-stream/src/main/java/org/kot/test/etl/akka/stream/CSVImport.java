package org.kot.test.etl.akka.stream;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.util.ByteString;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * CSV Importer.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2018-12-15 21:03
 */
public class CSVImport implements Grapher {

	private static final Logger logger = LoggerFactory.getLogger(CSVImport.class);

	private final ActorSystem system;

	private final Path importDirectory;

	private final int concurrentFiles;

	private final Path errorDirectory;

	private final Path outDirectory;

	private final String filePattern;

	private CSVImport(Config config, ActorSystem system) {
		this.system = system;
		this.importDirectory = Paths.get(config.getString("importer.folder"));
		this.filePattern = config.getString("importer.file-pattern");
		this.errorDirectory = Paths.get(config.getString("importer.folder-failed"));
		this.outDirectory = Paths.get(config.getString("importer.folder-processed"));
		this.concurrentFiles = config.getInt("importer.concurrency");
	}

	private Flow<Path, Map<String, ?>, NotUsed> parseFile() {
		List<Binding.Mapping<ByteString, ?>> mappings = Binding.inMappings();
		return Flow.of(Path.class)
				.flatMapConcat(file -> {
					InputStream inputStream = Files.newInputStream(file);
					return StreamConverters.fromInputStream(() -> inputStream)
							.via(CSVParsingStage.parse(',', '"', '\\', 10240))
							.via(CSV2JavaStage.toMappings(mappings));
				});
	}

	private CompletionStage<IOResult> importFromFiles() throws IOException {
		Files.createDirectories(outDirectory);
		Files.createDirectories(errorDirectory);

		PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + filePattern);
		List<Path> files = Files.list(importDirectory)
				.filter(matcher::matches)
				.collect(Collectors.toList());
		logger.info("Starting import of {} files from {}", files.size(), importDirectory);

		long time = System.nanoTime();

		Sink<Map<String, ?>, CompletionStage<IOResult>> outSink = Flow.<Map<String, ?>>create()
				.filter(m -> !m.containsKey("failures"))
				.via(Java2CSVStage.toMapping(Binding.outMappings()))
				.toMat(FileIO.toPath(outDirectory.resolve("success.csv")), Keep.right());

		Sink<Map<String, ?>, CompletionStage<IOResult>> errSink = Flow.<Map<String, ?>>create()
				.filter(m -> m.containsKey("failures"))
				.via(Java2CSVStage.toMapping(Binding.errorMappings()))
				.toMat(FileIO.toPath(errorDirectory.resolve("error.csv")), Keep.right());

		return Source.from(files)
				.via(balance(concurrentFiles, parseFile()))
				.runWith(sinkTo(outSink, errSink), ActorMaterializer.create(system))
				.whenComplete((d, e) -> {
					if (d != null) {
						logger.info("Import finished in {} ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time));
					} else {
						logger.error("Import failed", e);
					}
				});
	}

	public static void main(String[] args) throws IOException {
		Config config = ConfigFactory.load();
		ActorSystem system = ActorSystem.create();

		new CSVImport(config, system)
				.importFromFiles()
				.thenAccept(d -> system.terminate());

	}
}
