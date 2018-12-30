package org.kot.test.etl.akka.stream;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * CSV Generator.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2018-12-15 21:03
 */
public class CSVGenerator implements Grapher {

	private static final Logger logger = LoggerFactory.getLogger(CSVGenerator.class);

	private static final char[] textAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

	private final ActorSystem system;

	private final Config config;

	private final double invalidLineProbability;

	private final Random random;

	private final int numberOfFiles;

	private final int numberOfRecords;

	@SuppressWarnings("WeakerAccess")
	public CSVGenerator(Config config, ActorSystem system) {
		this.config = config;
		this.system = system;
		numberOfFiles = config.getInt("generator.number-of-files");
		numberOfRecords = config.getInt("generator.number-of-records");
		invalidLineProbability = config.getDouble("generator.invalid-line-probability");
		random = new Random();
	}

	public static void main(String[] args) throws IOException {
		Config config = ConfigFactory.load();
		ActorSystem system = ActorSystem.create("data-generator");
		new CSVGenerator(config, system)
				.generate(args)
				.thenAccept(d -> system.terminate());
	}

	private String screw(String value, Binding.Mapping<ByteString, ?> b) {
		if (random.nextDouble() < invalidLineProbability) {
			switch (b.fromName()) {
				case "id":
					return "a" + value;
				case "col1":
					return "";
				case "col2":
					return "a-" + value;
				case "col3":
					return "a" + value;
				case "col4":
					return "e" + random.nextInt();
				default:
					throw new UnsupportedOperationException("Field '" + b.fromName() + "' is not supported");
			}
		}
		return value;
	}

	private String generateBy(int pos, Binding.Mapping<ByteString, ?> b) {
		switch (b.fromName()) {
			case "id":
				return Integer.toString(pos);
			case "col1":
				int length = random.nextInt(100);
				return IntStream.range(0, length)
						.mapToObj(i -> textAlphabet[random.nextInt(textAlphabet.length)])
						.collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString();
			case "col2":
				int year = random.nextInt(10000);
				int month = 1 + random.nextInt(12);
				int dayOfMonth = 1 + random.nextInt(28);
				return Binding.DATE.format(LocalDate.of(year, month, dayOfMonth));
			case "col3":
				length = 1 + random.nextInt(15);
				return IntStream.range(0, length)
						.mapToObj(i -> '0' + random.nextInt(10))
						.collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString();
			case "col4":
				return "" + random.nextInt();
			default:
				throw new UnsupportedOperationException("Field '" + b.fromName() + "' is not supported");
		}
	}

	private Sink<Integer, CompletionStage<IOResult>> toTail(Path file) {
		List<Binding.Mapping<ByteString, ?>> mappings = Binding.inMappings();
		List<String> header = mappings.stream().map(Binding.Mapping::fromName).collect(Collectors.toList());
		return Flow.of(Integer.class)
				.map(i -> {
					if (0 == i) {
						return String.join(",", header) + "\n";
					}
					return mappings.stream()
							.map(b -> screw(generateBy(i, b), b))
							.collect(Collectors.joining(",")) + "\n";
				})
				.map(ByteString::fromString)
				.toMat(FileIO.toPath(file), Keep.right());
	}

	private CompletionStage<Void> generate(String[] args) throws IOException {
		logger.info("Starting generation");

		Path folder = Paths.get((0 < args.length)? args[0] : config.getString("importer.folder"));
		Files.createDirectories(folder);

		List<Sink<Integer, CompletionStage<IOResult>>> tails = IntStream.range(0, numberOfFiles)
				.mapToObj(i -> UUID.randomUUID().toString() + ".csv")
				.map(f -> toTail(folder.resolve(f))).collect(Collectors.toList());

		List<CompletionStage<IOResult>> result = Source.range(0, numberOfRecords)
				.runWith(sinkTo(tails), ActorMaterializer.create(system));

		return CompletableFuture.allOf(result.stream()
				.map(CompletionStage::toCompletableFuture)
				.toArray(CompletableFuture[]::new))
				.thenAccept(d -> {
					logger.info("Generation finished");
					system.terminate();
				});
	}
}
