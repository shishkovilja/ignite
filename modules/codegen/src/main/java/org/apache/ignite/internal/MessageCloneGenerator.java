/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.processing.FilerException;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static org.apache.ignite.internal.MessageSerializerGenerator.identicalFileIsAlreadyGenerated;

/**
 * Generates "clones" of messages with fields annotated by {@link Marshalable} annotation.
 * Clone encapsulates byte array fields containing serialized (marshaled) data for all annotated fields of the original
 * message.
 * Simple marhsaling methods of <code>MarshallableMessage</code> will be generated.
 * For complicated cases of marshaling you should implement <code>MarshallableMessage</code> manually.
 */
public class MessageCloneGenerator {
    /** */
    public static final String TAB = "    ";

    /** */
    public static final String NL = System.lineSeparator();

    /** */
    private static final String CLS_JAVADOC = "/** " + NL +
        " * This class is generated automatically." + NL +
        " *" + NL +
        " * @see org.apache.ignite.internal.MarshalableFieldsProcessor" + NL +
        " */";

    /** */
    public static final String METHOD_JAVADOC = "/** */";

    /** Bytes fields suffix. */
    public static final String BYTES = "Bytes";

    /** Collection of message-specific imports. */
    private final Set<String> imports = new TreeSet<>();

    /** */
    private Set<String> marshFields;

    /** */
    private String cloneClsName;

    /** */
    private final ProcessingEnvironment env;

    /** Stored type of the message being processed. */
    private TypeElement superType;

    /** */
    private int indent;

    /** */
    MessageCloneGenerator(ProcessingEnvironment env) {
        this.env = env;
    }

    /** */
    void generate(TypeElement superType, List<VariableElement> marshFields) throws Exception {
        assert this.superType == null : "Marhslable fields generator isn't stateless and is supposed to be single-use.";

        this.superType = superType;

        this.marshFields = marshFields.stream()
            .map(VariableElement::getSimpleName)
            .map(Object::toString)
            .collect(Collectors.toSet());

        cloneClsName = superType.getSimpleName() + "Clone";
        String cloneClsFqnName = env.getElementUtils().getPackageOf(superType) + "." + cloneClsName;
        String serCode = generateCode(cloneClsName);

        try {
            JavaFileObject file = env.getFiler().createSourceFile(cloneClsFqnName);

            try (Writer writer = file.openWriter()) {
                writer.append(serCode);
                writer.flush();
            }
        }
        catch (FilerException e) {
            // IntelliJ IDEA parses Ignite's pom.xml and configures itself to use this annotation processor on each Run.
            // During a Run, it invokes the processor and may fail when attempting to generate sources that already exist.
            // There is no a setting to disable this invocation. The IntelliJ community suggests a workaround — delegating
            // all Run commands to Maven. However, this significantly slows down test startup time.
            // This hack checks whether the content of a generating file is identical to already existed file, and skips
            // handling this class if it is.
            if (!identicalFileIsAlreadyGenerated(env, serCode, cloneClsFqnName)) {
                env.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "Message clone of " + cloneClsName + " is already generated. Try 'mvn clean install' to fix the issue.");

                throw e;
            }
        }
    }

    /** Generates full code for a serializer class. */
    private String generateCode(String cloneClsName) throws IOException {
        try (Writer writer = new StringWriter()) {
            writeClassHeader(writer, env.getElementUtils().getPackageOf(superType).toString(), cloneClsName);

            writeClassFields(writer);

            writeMarshalMethods(writer);

            writer.write("}");

            writer.write(NL);

            return writer.toString();
        }
    }

    /** */
    private void writeMarshalMethods(Writer writer) throws IOException {
        writeMethod(writer,
            f -> indentedLine("%s = U.marshal(marsh, %s);", f + BYTES, f),
            "prepareMarshal(Marshaller marsh)",
            "marshal"
        );

        writer.write(NL);

        writeMethod(writer,
            f ->
                indentedLine("%s = U.unmarshal(marsh, %s, clsLdr); %2$s = null;", f, f + BYTES),
            "finishUnmarshal(Marshaller marsh, ClassLoader clsLdr)",
            "unmarshal"
        );
    }

    /** */
    private void writeMethod(Writer writer, Function<String, String> marshFun, String method, String action) throws IOException {
        ++indent;

        writer.write(indentedLine(METHOD_JAVADOC));
        writer.write(NL);
        writer.write(indentedLine("@Override public void %s {", method));

        writer.write(NL);

        ++indent;

        writer.write(indentedLine("try {"));
        writer.write(NL);

        ++indent;

        for (String field : marshFields) {
            writer.write(marshFun.apply(field));
            writer.write(NL);
        }

        --indent;

        writer.write(indentedLine("}"));
        writer.write(NL);

        writer.write(indentedLine("catch (IgniteCheckedException e) {"));
        writer.write(NL);

        ++indent;

        writer.write(indentedLine("throw new IgniteException(\"Failed to %s message [msgCls=%s, cloneClsName=%s]\");",
            action, superType.getSimpleName(), cloneClsName));
        writer.write(NL);

        --indent;

        writer.write(indentedLine("}"));
        writer.write(NL);

        --indent;

        writer.write(indentedLine("}"));
        writer.write(NL);

        --indent;
    }

    /**
     * Creates line with current indent from given arguments.
     *
     * @return Line with current indent.
     */
    private String indentedLine(String format, Object... args) {
        SB sb = new SB();

        for (int i = 0; i < indent; i++)
            sb.a(TAB);

        sb.a(String.format(format, args));

        return sb.toString();
    }

    /** */
    private void writeClassFields(Writer writer) throws IOException {
        boolean serializable = env.getTypeUtils().isAssignable(
            superType.asType(),
            env.getElementUtils()
                .getTypeElement(Serializable.class.getName())
                .asType());

        indent = 1;

        if (serializable)
            writeField(writer, "private static final long serialVersionUID = 0L;");

        int ord = 0;

        for (String field : marshFields) {
            writeField(writer, "@Order(%s) %sbyte[] %s;",
                ord++,
                serializable ? "transient " : "",
                field + BYTES);
        }

        indent = 0;
    }

    /** */
    private void writeField(Writer writer, String str, Object... args) throws IOException {
        writer.write(indentedLine(METHOD_JAVADOC));
        writer.write(NL);

        writer.write(indentedLine(str, args));

        writer.write(NL);
        writer.write(NL);
    }

    /** Write header of serializer class: license, imports, class declaration. */
    private void writeClassHeader(Writer writer, String pkgName, String serClsName) throws IOException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("license.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            PrintWriter out = new PrintWriter(writer);

            String line;

            while ((line = reader.readLine()) != null)
                out.println(line);
        }

        writer.write(NL);
        writer.write("package " + pkgName + ";" + NL + NL);

        imports.add("org.apache.ignite.IgniteCheckedException");
        imports.add("org.apache.ignite.IgniteException");
        imports.add("org.apache.ignite.internal.Order");
        imports.add("org.apache.ignite.internal.util.typedef.internal.U");
        imports.add("org.apache.ignite.marshaller.Marshaller");
        imports.add("org.apache.ignite.plugin.extensions.communication.MarshallableMessage");

        for (String regularImport : imports)
            writer.write("import " + regularImport + ";" + NL);

        writer.write(NL);
        writer.write(CLS_JAVADOC);
        writer.write(NL);

        writer.write("public class " + serClsName + " extends " + superType.getSimpleName() + " implements MarshallableMessage {" + NL);
    }
}
