package org.kot.mapping;

import org.yaml.snakeyaml.Yaml;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TODO Describe me.
 *
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2019-07-14 22:13
 */
public class Processor extends AbstractProcessor {

	private Filer filer;

	private Messager messager;

	private Yaml yaml;

	private boolean verbose;

	private Elements utils;

	@Override
	public synchronized void init(ProcessingEnvironment processingEnv) {
		super.init(processingEnv);
		verbose = Boolean.parseBoolean(processingEnv.getOptions().getOrDefault("verbose", "false"));
		filer = processingEnv.getFiler();
		messager = processingEnv.getMessager();
		yaml = new Yaml();
		utils = processingEnv.getElementUtils();
	}

	@Override
	public Set<String> getSupportedAnnotationTypes() {
		return Collections.singleton(Mapping.class.getCanonicalName());
	}

	@Override
	public SourceVersion getSupportedSourceVersion() {
		return SourceVersion.latestSupported();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean process(Set<? extends TypeElement> set, RoundEnvironment roundEnv) {
		Generator generator = new Generator(filer, e -> utils.getPackageOf(e).getQualifiedName() + "", e -> e.getSimpleName() + "Impl");

		for (Element element : roundEnv.getElementsAnnotatedWith(Mapping.class)) {
			if (ElementKind.INTERFACE != element.getKind()) {
				error(element, "%s applicable to interface only", Mapping.class);
				continue;
			}
			String uri = element.getAnnotation(Mapping.class).value();
			if ("".equals(uri.trim())) {
				error(element, "Associated mapping \"%s\" is invalid", uri);
				continue;
			}
			try {
				FileObject file = filer.getResource(StandardLocation.CLASS_OUTPUT, "", uri);
				List<Map<String, String>> columns = yaml.loadAs(file.openReader(false), List.class);
				TypeElement face = (TypeElement) element;

				info(element, "Generating mapping implementation");
				generator.generateClass(face, columns);
				info(element, "Generating service meta");
				generator.generateServiceMeta(face);
			} catch (IOException e) {
				error(element, "Failed to generate mapping due to", e);
			}
		}
		return true;
	}

	private void error(Element element, String message, Object... params) {
		messager.printMessage(Diagnostic.Kind.ERROR, String.format(message, params), element);
	}

	private void info(Element element, String message) {
		messager.printMessage(Diagnostic.Kind.NOTE, message, element);
	}
}
