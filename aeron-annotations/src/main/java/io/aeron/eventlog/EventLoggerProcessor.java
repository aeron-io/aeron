/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.eventlog;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Generates an implementation class for a {@link GeneratedLogger}-annotated interface, wiring up the
 * mechanical {@code tryClaim}/encode/{@code commit} ring-buffer boilerplate for every
 * {@link LoggerMethod}-annotated method. The per-field encoding logic itself is left entirely to the
 * {@link GeneratedLogger#encoder()} class referenced by name - this processor only reproduces the
 * wrapper around it.
 */
@SupportedAnnotationTypes("io.aeron.eventlog.GeneratedLogger")
public class EventLoggerProcessor extends AbstractProcessor
{
    private static final String MANY_TO_ONE_RING_BUFFER_TYPE = "org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer";
    private static final String UNSAFE_BUFFER_TYPE = "org.agrona.concurrent.UnsafeBuffer";
    private static final String COMMON_EVENT_ENCODER_TYPE = "io.aeron.logging.CommonEventEncoder";

    /**
     * Default constructor.
     */
    public EventLoggerProcessor()
    {
    }

    /**
     * {@inheritDoc}
     */
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.latest();
    }

    /**
     * {@inheritDoc}
     */
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv)
    {
        for (final TypeElement annotation : annotations)
        {
            for (final Element element : roundEnv.getElementsAnnotatedWith(annotation))
            {
                if (element.getKind() != ElementKind.INTERFACE)
                {
                    error(element, "@GeneratedLogger may only be applied to an interface");
                    continue;
                }

                generate((TypeElement)element);
            }
        }

        return true;
    }

    private void generate(final TypeElement iface)
    {
        final GeneratedLogger generatedLogger = iface.getAnnotation(GeneratedLogger.class);

        final TypeElement encoderType = processingEnv.getElementUtils().getTypeElement(generatedLogger.encoder());
        if (null == encoderType)
        {
            error(iface, "GeneratedLogger.encoder() '" + generatedLogger.encoder() + "' could not be resolved");
            return;
        }

        final TypeElement eventCodeType =
            processingEnv.getElementUtils().getTypeElement(generatedLogger.eventCodeType());
        if (null == eventCodeType || eventCodeType.getKind() != ElementKind.ENUM)
        {
            error(iface, "GeneratedLogger.eventCodeType() '" + generatedLogger.eventCodeType() +
                "' could not be resolved to an enum");
            return;
        }

        final PackageElement packageElement = processingEnv.getElementUtils().getPackageOf(iface);
        final String packageName = packageElement.getQualifiedName().toString();
        final String interfaceName = iface.getSimpleName().toString();
        final String implName = interfaceName + "Impl";

        final List<MethodPlan> plans = new ArrayList<>();
        boolean ok = true;
        for (final ExecutableElement method : ElementFilter.methodsIn(iface.getEnclosedElements()))
        {
            final LoggerMethod loggerMethod = method.getAnnotation(LoggerMethod.class);
            if (null == loggerMethod)
            {
                continue;
            }

            final MethodPlan plan = buildPlan(method, loggerMethod, encoderType, eventCodeType);
            if (null == plan)
            {
                ok = false;
            }
            else
            {
                plans.add(plan);
            }
        }

        if (!ok)
        {
            return;
        }

        try
        {
            final JavaFileObject sourceFile = processingEnv.getFiler().createSourceFile(
                packageName + "." + implName, iface);
            try (PrintWriter out = new PrintWriter(sourceFile.openWriter()))
            {
                writeImpl(out, packageName, implName, interfaceName, plans);
            }
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private MethodPlan buildPlan(
        final ExecutableElement method,
        final LoggerMethod loggerMethod,
        final TypeElement encoderType,
        final TypeElement eventCodeType)
    {
        boolean ok = true;

        final String eventCodeExpr = resolveEventCode(method, loggerMethod, eventCodeType);
        if (null == eventCodeExpr)
        {
            ok = false;
        }

        final boolean useFixedLength = loggerMethod.fixedLength() >= 0;
        if (useFixedLength && !loggerMethod.lengthMethod().isEmpty())
        {
            error(method, "LoggerMethod.fixedLength() and lengthMethod() are mutually exclusive");
            ok = false;
        }
        if (!useFixedLength && loggerMethod.lengthMethod().isEmpty())
        {
            error(method, "LoggerMethod requires exactly one of fixedLength() or lengthMethod()");
            ok = false;
        }

        String lengthExpr = null;
        if (useFixedLength)
        {
            lengthExpr = Integer.toString(loggerMethod.fixedLength());
        }
        else if (!loggerMethod.lengthMethod().isEmpty())
        {
            if (!validateArgNames(method, loggerMethod.lengthArgs(), "lengthArgs"))
            {
                ok = false;
            }
            else if (!hasStaticMethod(encoderType, loggerMethod.lengthMethod(), loggerMethod.lengthArgs().length))
            {
                error(method, "no static " + loggerMethod.lengthMethod() + "(" + loggerMethod.lengthArgs().length +
                    " args) found on " + encoderType.getQualifiedName());
                ok = false;
            }
            else
            {
                lengthExpr = encoderType.getQualifiedName() + "." + loggerMethod.lengthMethod() + "(" +
                    String.join(", ", loggerMethod.lengthArgs()) + ")";
            }
        }

        String encodeName = loggerMethod.encodeMethod();
        if (encodeName.isEmpty())
        {
            encodeName = deriveEncodeMethodName(method);
            if (null == encodeName)
            {
                error(method, "LoggerMethod-annotated method name must start with 'log' so an 'encodeXxx' " +
                    "counterpart name can be derived, or LoggerMethod.encodeMethod() must be set explicitly, " +
                    "but method was: " + method.getSimpleName());
                ok = false;
                return null;
            }
        }

        final List<String> encodeArgNames = loggerMethod.encodeArgs().length > 0 ?
            List.of(loggerMethod.encodeArgs()) :
            method.getParameters().stream().map(p -> p.getSimpleName().toString()).toList();

        if (!validateArgNames(method, encodeArgNames.toArray(new String[0]), "encodeArgs"))
        {
            ok = false;
        }

        final ExecutableElement encodeMethod =
            findStaticMethod(encoderType, encodeName, 4 + encodeArgNames.size());
        if (null == encodeMethod)
        {
            error(method, "no static " + encodeName + "(" + (4 + encodeArgNames.size()) +
                " args) found on " + encoderType.getQualifiedName() + " (from method '" +
                method.getSimpleName() + "')");
            ok = false;
        }

        if (!ok)
        {
            return null;
        }

        final String firstParamType = encodeMethod.getParameters().get(0).asType().toString();
        final String bufferCast = UNSAFE_BUFFER_TYPE.equals(firstParamType) ? "(" + UNSAFE_BUFFER_TYPE + ")" : "";

        return new MethodPlan(
            method,
            eventCodeExpr,
            lengthExpr,
            loggerMethod.skipCaptureLength(),
            encoderType.getQualifiedName() + "." + encodeName,
            bufferCast,
            encodeArgNames);
    }

    private String resolveEventCode(
        final ExecutableElement method, final LoggerMethod loggerMethod, final TypeElement eventCodeType)
    {
        final List<VariableElement> matchingParams = new ArrayList<>();
        for (final VariableElement param : method.getParameters())
        {
            if (processingEnv.getTypeUtils().isSameType(param.asType(), eventCodeType.asType()))
            {
                matchingParams.add(param);
            }
        }

        if (!loggerMethod.eventCode().isEmpty())
        {
            if (!matchingParams.isEmpty())
            {
                error(method, "LoggerMethod.eventCode() is set but this method also has a parameter of type " +
                    eventCodeType.getQualifiedName() + " - use exactly one mechanism, not both");
                return null;
            }

            final boolean constantExists = ElementFilter.fieldsIn(eventCodeType.getEnclosedElements()).stream()
                .anyMatch(f -> f.getKind() == ElementKind.ENUM_CONSTANT &&
                    f.getSimpleName().contentEquals(loggerMethod.eventCode()));
            if (!constantExists)
            {
                error(method, "LoggerMethod.eventCode() '" + loggerMethod.eventCode() + "' is not a constant of " +
                    eventCodeType.getQualifiedName());
                return null;
            }

            return eventCodeType.getQualifiedName() + "." + loggerMethod.eventCode();
        }

        if (matchingParams.size() != 1)
        {
            error(method, "LoggerMethod.eventCode() is blank, so exactly one parameter of type " +
                eventCodeType.getQualifiedName() + " was expected, but found " + matchingParams.size());
            return null;
        }

        return matchingParams.get(0).getSimpleName().toString();
    }

    private boolean validateArgNames(final ExecutableElement method, final String[] argNames, final String attrName)
    {
        boolean ok = true;
        for (final String argName : argNames)
        {
            final boolean found = method.getParameters().stream()
                .anyMatch(p -> p.getSimpleName().contentEquals(argName));
            if (!found)
            {
                error(method, "LoggerMethod." + attrName + "() references '" + argName +
                    "' which is not a parameter of this method");
                ok = false;
            }
        }
        return ok;
    }

    private boolean hasStaticMethod(final TypeElement type, final String name, final int paramCount)
    {
        return null != findStaticMethod(type, name, paramCount);
    }

    private ExecutableElement findStaticMethod(final TypeElement type, final String name, final int paramCount)
    {
        for (final ExecutableElement m : ElementFilter.methodsIn(type.getEnclosedElements()))
        {
            if (m.getSimpleName().contentEquals(name) && m.getParameters().size() == paramCount)
            {
                return m;
            }
        }
        return null;
    }

    private static String deriveEncodeMethodName(final ExecutableElement method)
    {
        final String name = method.getSimpleName().toString();
        if (!name.startsWith("log"))
        {
            return null;
        }
        return "encode" + name.substring(3);
    }

    private void error(final Element element, final String message)
    {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, message, element);
    }

    private void writeImpl(
        final PrintWriter out,
        final String packageName,
        final String implName,
        final String interfaceName,
        final List<MethodPlan> plans)
    {
        out.printf("package %s;%n%n", packageName);
        out.printf("final class %s implements %s%n{%n", implName, interfaceName);
        out.printf("    private final %s ringBuffer;%n%n", MANY_TO_ONE_RING_BUFFER_TYPE);
        out.printf("    %s(final %s eventRingBuffer)%n    {%n", implName, MANY_TO_ONE_RING_BUFFER_TYPE);
        out.printf("        this.ringBuffer = eventRingBuffer;%n    }%n");

        for (final MethodPlan plan : plans)
        {
            out.println();
            writeMethod(out, plan);
        }

        out.printf("}%n");
    }

    private void writeMethod(final PrintWriter out, final MethodPlan plan)
    {
        final ExecutableElement method = plan.method;

        out.printf("    @Override%n    public %svoid %s(%s)%n    {%n",
            renderTypeParameters(method), method.getSimpleName(), renderParameters(method));
        out.printf("        final int length = %s;%n", plan.lengthExpr);
        if (plan.skipCaptureLength)
        {
            out.printf("        final int captureLength = length;%n");
        }
        else
        {
            out.printf("        final int captureLength = %s.captureLength(length);%n", COMMON_EVENT_ENCODER_TYPE);
        }
        out.printf("        final int encodedLength = %s.encodedLength(captureLength);%n", COMMON_EVENT_ENCODER_TYPE);
        out.printf("        final %s ringBuffer = this.ringBuffer;%n", MANY_TO_ONE_RING_BUFFER_TYPE);
        out.printf("        final int index = ringBuffer.tryClaim(%s.toEventCodeId(), encodedLength);%n%n",
            plan.eventCodeExpr);
        out.printf("        if (index > 0)%n        {%n            try%n            {%n");
        out.printf("                %s(%n", plan.encodeMethodFqn);
        out.printf("                    %sringBuffer.buffer(),%n", plan.bufferCast);
        out.printf("                    index,%n");
        out.printf("                    captureLength,%n");
        out.printf("                    length");
        for (final String argName : plan.encodeArgNames)
        {
            out.printf(",%n                    %s", argName);
        }
        out.printf(");%n            }%n            finally%n            {%n");
        out.printf("                ringBuffer.commit(index);%n            }%n        }%n    }%n");
    }

    private static String renderTypeParameters(final ExecutableElement method)
    {
        final List<? extends TypeParameterElement> typeParams = method.getTypeParameters();
        if (typeParams.isEmpty())
        {
            return "";
        }

        final StringBuilder sb = new StringBuilder("<");
        for (int i = 0; i < typeParams.size(); i++)
        {
            if (i > 0)
            {
                sb.append(", ");
            }

            final TypeParameterElement typeParam = typeParams.get(i);
            sb.append(typeParam.getSimpleName());

            final List<String> realBounds = new ArrayList<>();
            for (final TypeMirror bound : typeParam.getBounds())
            {
                if (!"java.lang.Object".equals(bound.toString()))
                {
                    realBounds.add(bound.toString());
                }
            }

            if (!realBounds.isEmpty())
            {
                sb.append(" extends ").append(String.join(" & ", realBounds));
            }
        }
        sb.append("> ");

        return sb.toString();
    }

    private static String renderParameters(final ExecutableElement method)
    {
        final StringBuilder sb = new StringBuilder();
        final List<? extends VariableElement> params = method.getParameters();
        for (int i = 0; i < params.size(); i++)
        {
            if (i > 0)
            {
                sb.append(", ");
            }

            final VariableElement param = params.get(i);
            sb.append("final ").append(param.asType().toString()).append(' ').append(param.getSimpleName());
        }

        return sb.toString();
    }

    private static final class MethodPlan
    {
        private final ExecutableElement method;
        private final String eventCodeExpr;
        private final String lengthExpr;
        private final boolean skipCaptureLength;
        private final String encodeMethodFqn;
        private final String bufferCast;
        private final List<String> encodeArgNames;

        private MethodPlan(
            final ExecutableElement method,
            final String eventCodeExpr,
            final String lengthExpr,
            final boolean skipCaptureLength,
            final String encodeMethodFqn,
            final String bufferCast,
            final List<String> encodeArgNames)
        {
            this.method = method;
            this.eventCodeExpr = eventCodeExpr;
            this.lengthExpr = lengthExpr;
            this.skipCaptureLength = skipCaptureLength;
            this.encodeMethodFqn = encodeMethodFqn;
            this.bufferCast = bufferCast;
            this.encodeArgNames = encodeArgNames;
        }
    }
}
