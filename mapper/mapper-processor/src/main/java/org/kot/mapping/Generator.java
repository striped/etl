package org.kot.mapping;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;

import javax.annotation.Generated;
import javax.annotation.processing.Filer;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.io.Writer;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * TODO Describe me.
 *
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2019-07-14 23:16
 */
public class Generator {

	private final Filer filer;

	private final Function<TypeElement, String> packageNameGenerator;

	private final Function<TypeElement, String> classNameGenerator;

	private final Map<String, BiFunction<TypeName, List<Map<String, String>>, CodeBlock>> initializers;

	public Generator(Filer filer, Function<TypeElement, String> packageNameGenerator, Function<TypeElement, String> classNameGenerator) {
		this.filer = filer;
		this.packageNameGenerator = packageNameGenerator;
		this.classNameGenerator = classNameGenerator;

		this.initializers = new HashMap<String, BiFunction<TypeName, List<Map<String, String>>, CodeBlock>>() {{
			put("header", (t, l) -> l.stream()
					.map(c -> CodeBlock.of("$S, $S", c.get("name"), c.get("id")))
					.collect(CodeBlock.joining(",\n", "$N(\n", "\n)")));
			put("binders", (t, l) -> l.stream()
					.map(c -> CodeBlock.of("$S, ($T) " + c.get("binder"), c.get("name"), t))
					.collect(CodeBlock.joining(",\n", "$N(\n", "\n)")));
		}};

	}

	public void generateClass(TypeElement element, List<Map<String, String>> columns) throws IOException {
		String packageName = packageNameGenerator.apply(element);
		String className = classNameGenerator.apply(element);

		MethodSpec initMethod = generateInitFrom();

		TypeSpec.Builder classSpec = TypeSpec.classBuilder(className)
				.addModifiers(Modifier.PUBLIC, Modifier.FINAL)
				.addSuperinterface(TypeName.get(element.asType()))
				.addAnnotation(
						AnnotationSpec.builder(Generated.class)
								.addMember("value", "$S", Processor.class.getCanonicalName())
								.addMember("comments", "$S", "Do not amend, class is generated")
								.addMember("date", "$S", LocalDateTime.now())
								.build())
				.addMethod(initMethod);

		for (Element e : element.getEnclosedElements()) {
			if (ElementKind.METHOD != e.getKind() || !e.getModifiers().contains(Modifier.ABSTRACT)) {
				continue;
			}
			ExecutableElement method = (ExecutableElement) e;
			BiFunction<TypeName, List<Map<String, String>>, CodeBlock> initializer = initializers.get(method.getSimpleName().toString());
			if (null == initializer) {
				continue;
			}

			ParameterizedTypeName mapType = (ParameterizedTypeName) ParameterizedTypeName.get(method.getReturnType());
			classSpec.addField(
					FieldSpec.builder(mapType, method.getSimpleName().toString(), Modifier.PRIVATE, Modifier.FINAL)
							.initializer(initializer.apply(mapType.typeArguments.get(1), columns).toString(), initMethod.name)
							.build());

			classSpec.addMethod(
					MethodSpec.methodBuilder(method.getSimpleName().toString())
							.addAnnotation(Override.class)
							.addModifiers(Modifier.PUBLIC)
							.returns(mapType)
							.addStatement("return $N", method.getSimpleName().toString())
							.build());

		}

		JavaFile.builder(packageName, classSpec.build())
				.build().writeTo(filer);
	}

	public void generateServiceMeta(TypeElement element) throws IOException {
		String packageName = packageNameGenerator.apply(element);
		String className = classNameGenerator.apply(element);
		FileObject resource = filer.createResource(StandardLocation.CLASS_OUTPUT, "", "META-INF/services/" + element.getQualifiedName());
		try (Writer w = resource.openWriter()) {
			w.append(packageName).append('.').append(className);
		}
	}

	private MethodSpec generateInitFrom() {
		TypeVariableName keyType = TypeVariableName.get("K");
		TypeVariableName valType = TypeVariableName.get("V");
		ParameterizedTypeName resultType = ParameterizedTypeName.get(ClassName.get(Map.class), keyType, valType);
		ParameterizedTypeName resultImpl = ParameterizedTypeName.get(ClassName.get(LinkedHashMap.class), keyType, valType);
		return MethodSpec.methodBuilder("initFrom")
				.addModifiers(Modifier.PRIVATE, Modifier.STATIC)
				.addTypeVariable(keyType)
				.addTypeVariable(valType)
				.returns(resultType)
				.addParameter(Object[].class, "values")
				.varargs(true)
				.addStatement("$T result = new $T(values.length, 1F)", resultType, resultImpl)
				.beginControlFlow("for (int i = $L; i < values.length;)", 0)
				.addStatement("result.put(($T) values[i++], ($T) values[i++])", keyType, valType)
				.endControlFlow()
				.addStatement("return $T.unmodifiableMap(result)", Collections.class)
				.build();
	}
}
