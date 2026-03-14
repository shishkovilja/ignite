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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;

import static org.apache.ignite.internal.MessageProcessor.MESSAGE_INTERFACE;

/**
 */
@SupportedAnnotationTypes("org.apache.ignite.internal.Marshalable")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class MarshalableFieldsProcessor extends AbstractProcessor {
    /**
     * Processes all classes implementing the {@code Message} interface and generates corresponding serializer code.
     */
    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (roundEnv.errorRaised())
            return true;

        TypeMirror msgType = processingEnv.getElementUtils().getTypeElement(MESSAGE_INTERFACE).asType();

        Map<TypeElement, List<VariableElement>> msgFields = new HashMap<>();

        for (Element el: roundEnv.getRootElements()) {
            if (el.getKind() != ElementKind.CLASS)
                continue;

            TypeElement clazz = (TypeElement)el;

            if (!processingEnv.getTypeUtils().isAssignable(clazz.asType(), msgType))
                continue;

            // Skip generated clones.
            if (clazz.getSimpleName().toString().endsWith("Clone"))
                continue;

            if (clazz.getModifiers().contains(Modifier.ABSTRACT))
                continue;

            List<VariableElement> marshFields = marhalableFields(clazz);

            if (!marshFields.isEmpty())
                msgFields.put(clazz, marshFields);
        }

        for (Map.Entry<TypeElement, List<VariableElement>> type: msgFields.entrySet()) {
            try {
                new MessageCloneGenerator(processingEnv).generate(type.getKey(), type.getValue());
            }
            catch (Exception e) {
                processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "Failed to generate a message with marshallable fields:" + e.getMessage(),
                    type.getKey());
            }
        }

        return true;
    }

    /**
     * Collects all fields annotated with {@link Order} from the given {@link TypeElement} and all its superclasses.
     * <p>
     * The resulting list is sorted in ascending order of the {@code @Order} value.
     *
     * @param type the {@code TypeElement} representing the class to inspect.
     * @return a list of {@code VariableElement} objects representing all ordered fields, including those declared in superclasses.
     */
    private List<VariableElement> marhalableFields(TypeElement type) {
        Element superType = processingEnv.getTypeUtils().asElement(type.getSuperclass());

        List<VariableElement> hierList = superType == null ? new ArrayList<>() : marhalableFields((TypeElement)superType);

        for (Element el : type.getEnclosedElements()) {
            if (el.getAnnotation(Marshalable.class) != null)
                hierList.add((VariableElement)el);
        }

        return hierList;
    }
}
