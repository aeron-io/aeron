/*
 * Copyright 2014-2026 Real Logic Limited.
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
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Generates a CBOR-encoding implementation class for a {@link GeneratedLogger}-annotated interface, wiring up
 * the ring-buffer {@code tryClaim}/encode/{@code commit} boilerplate for every {@link LoggerMethod}-annotated
 * method, inlining calls to {@code io.aeron.logging.CborEncode} directly rather than delegating to a per-event
 * encoder class, which this processor otherwise mirrors in structure.
 * The generated class is named {@code <Interface>CborImpl}.
 * <p>
 * Each body field's CBOR tag is inferred from its Java type (numbers/strings use no tag, enum-typed
 * parameters use the enum tag, buffers use the byte-array tag, addresses use the IPv4/IPv6 tag chosen at
 * generation time by address family) unless the parameter carries a {@link Tag} annotation, in which case its
 * value is used verbatim instead. Likewise, {@code allowTruncate} defaults to {@code false} for every field
 * unless the parameter carries an {@link AllowTruncate} annotation.
 */
@SupportedAnnotationTypes("io.aeron.eventlog.GeneratedLogger")
public class CborEventLoggerProcessor extends AbstractProcessor
{
    private static final String MANY_TO_ONE_RING_BUFFER_TYPE = "org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer";
    private static final String UNSAFE_BUFFER_TYPE = "org.agrona.concurrent.UnsafeBuffer";
    private static final String ENCODING_STATE_TYPE = "io.aeron.logging.EncodingState";
    private static final String CBOR_ENCODE_TYPE = "io.aeron.logging.CborEncode";
    private static final String CBOR_UTILS_TYPE = "io.aeron.logging.CborUtils";
    private static final int MAX_BUFFER_LENGTH = 4096;
    private static final Set<String> RESERVED_LOCAL_NAMES =
        Set.of("timestampNanos", "length", "bufferLength", "index", "encodingState");

    /**
     * Default constructor.
     */
    public CborEventLoggerProcessor()
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
        final String implName = interfaceName + "CborImpl";

        final List<CborMethodPlan> plans = new ArrayList<>();
        boolean ok = true;
        boolean usesBuffer = false;
        boolean usesAddress = false;

        for (final ExecutableElement method : ElementFilter.methodsIn(iface.getEnclosedElements()))
        {
            final LoggerMethod loggerMethod = method.getAnnotation(LoggerMethod.class);
            if (null == loggerMethod)
            {
                continue;
            }

            final CborMethodPlan plan = buildCborMethodPlan(method, loggerMethod, eventCodeType);
            if (null == plan)
            {
                ok = false;
            }
            else
            {
                plans.add(plan);
                usesBuffer |= plan.usesBufferField;
                usesAddress |= plan.usesAddressField;
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
                writeImpl(out, packageName, implName, interfaceName, plans, usesBuffer, usesAddress);
            }
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private CborMethodPlan buildCborMethodPlan(
        final ExecutableElement method, final LoggerMethod loggerMethod, final TypeElement eventCodeType)
    {
        boolean ok = true;

        for (final VariableElement param : method.getParameters())
        {
            if (RESERVED_LOCAL_NAMES.contains(param.getSimpleName().toString()))
            {
                error(param, "@LoggerMethod parameter '" + param.getSimpleName() + "' of method '" +
                    method.getSimpleName() + "' collides with a local variable name the CBOR processor " +
                    "generates internally (" + RESERVED_LOCAL_NAMES + ") - rename the parameter");
                ok = false;
            }
        }

        final String eventCodeExpr = resolveEventCode(method, loggerMethod, eventCodeType);
        if (null == eventCodeExpr)
        {
            ok = false;
        }

        final Set<String> excludedParamNames = new HashSet<>();
        if (null != eventCodeExpr && !eventCodeExpr.contains("."))
        {
            // A bare identifier (no '.') means resolveEventCode() auto-detected this method's sole
            // eventCodeType-typed parameter - exclude it from body-field iteration. A fixed eventCode()
            // literal always resolves to a dotted "Type.CONSTANT" expression and consumes no parameter.
            excludedParamNames.add(eventCodeExpr);
        }

        final String[] bufferViewAttr = loggerMethod.bufferView();
        final BufferViewSpec bufferViewSpec = buildBufferViewSpec(method, loggerMethod);
        if (bufferViewAttr.length != 0 && null == bufferViewSpec)
        {
            ok = false;
        }
        if (null != bufferViewSpec)
        {
            excludedParamNames.add(bufferViewSpec.offsetParamName);
            excludedParamNames.add(bufferViewSpec.lengthParamName);
        }

        final List<FieldPlan> fields = new ArrayList<>();
        int bufferFieldCount = 0;
        int addressFieldCount = 0;

        for (final VariableElement param : method.getParameters())
        {
            if (excludedParamNames.contains(param.getSimpleName().toString()))
            {
                continue;
            }

            final FieldPlan field = buildFieldPlan(method, param, bufferViewSpec);
            if (null == field)
            {
                ok = false;
                continue;
            }

            if (field.usesBufferField)
            {
                bufferFieldCount++;
            }
            if (field.usesAddressField)
            {
                addressFieldCount++;
            }

            fields.add(field);
        }

        if (bufferFieldCount > 1)
        {
            error(method, "at most one DirectBuffer/ByteBuffer body field is supported per method, found " +
                bufferFieldCount);
            ok = false;
        }
        if (addressFieldCount > 1)
        {
            error(method, "at most one InetSocketAddress/InetAddress body field is supported per method, found " +
                addressFieldCount);
            ok = false;
        }

        if (!ok)
        {
            return null;
        }

        return new CborMethodPlan(method, eventCodeExpr, fields, 1 == bufferFieldCount, 1 == addressFieldCount);
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

    private BufferViewSpec buildBufferViewSpec(final ExecutableElement method, final LoggerMethod loggerMethod)
    {
        final String[] bufferView = loggerMethod.bufferView();
        if (0 == bufferView.length)
        {
            return null;
        }

        if (3 != bufferView.length)
        {
            error(method, "LoggerMethod.bufferView() must be empty or exactly 3 elements " +
                "{bufferParamName, offsetParamName, lengthParamName}, but found " + bufferView.length);
            return null;
        }

        final String bufferParamName = bufferView[0];
        final String offsetParamName = bufferView[1];
        final String lengthParamName = bufferView[2];

        final VariableElement bufferParam = findParam(method, bufferParamName);
        final VariableElement offsetParam = findParam(method, offsetParamName);
        final VariableElement lengthParam = findParam(method, lengthParamName);

        boolean ok = true;

        if (null == bufferParam)
        {
            error(method, "LoggerMethod.bufferView() references '" + bufferParamName + "' which is not a " +
                "parameter of this method");
            ok = false;
        }
        else if (!isBufferType(bufferParam.asType()))
        {
            error(method, "LoggerMethod.bufferView() buffer parameter '" + bufferParamName +
                "' must be a DirectBuffer or ByteBuffer");
            ok = false;
        }

        if (null == offsetParam)
        {
            error(method, "LoggerMethod.bufferView() references '" + offsetParamName + "' which is not a " +
                "parameter of this method");
            ok = false;
        }
        else if (TypeKind.INT != offsetParam.asType().getKind())
        {
            error(method, "LoggerMethod.bufferView() offset parameter '" + offsetParamName + "' must be an int");
            ok = false;
        }

        if (null == lengthParam)
        {
            error(method, "LoggerMethod.bufferView() references '" + lengthParamName + "' which is not a " +
                "parameter of this method");
            ok = false;
        }
        else if (TypeKind.INT != lengthParam.asType().getKind())
        {
            error(method, "LoggerMethod.bufferView() length parameter '" + lengthParamName + "' must be an int");
            ok = false;
        }

        return ok ? new BufferViewSpec(bufferParamName, offsetParamName, lengthParamName) : null;
    }

    private FieldPlan buildFieldPlan(
        final ExecutableElement method, final VariableElement param, final BufferViewSpec bufferViewSpec)
    {
        final Kind kind = classifyKind(param, method);
        if (null == kind)
        {
            return null;
        }

        final String name = param.getSimpleName().toString();
        final Tag tagAnnotation = param.getAnnotation(Tag.class);
        final AllowTruncate allowTruncateAnnotation = param.getAnnotation(AllowTruncate.class);

        if (Kind.BOOLEAN == kind)
        {
            boolean ok = true;
            if (null != tagAnnotation)
            {
                error(param, "@Tag is not supported on boolean parameter '" + name + "' - the CBOR boolean " +
                    "encoding has no tag");
                ok = false;
            }
            if (null != allowTruncateAnnotation)
            {
                error(param, "@AllowTruncate is not supported on boolean parameter '" + name + "' - the CBOR " +
                    "boolean encoding has no truncation");
                ok = false;
            }
            return ok ? FieldPlan.simple(param, kind, name, null, false) : null;
        }

        if (Kind.NUMBER == kind && null != allowTruncateAnnotation)
        {
            error(param, "@AllowTruncate is not supported on '" + name + "' - numeric values are never truncated");
            return null;
        }

        final boolean allowTruncate = null != allowTruncateAnnotation;

        switch (kind)
        {
            case NUMBER:
                return FieldPlan.simple(param, kind, name, tagOrDefault(tagAnnotation, "NO_TAG"), false);

            case STRING:
                return FieldPlan.simple(param, kind, name, tagOrDefault(tagAnnotation, "NO_TAG"), allowTruncate);

            case ENUM:
                return FieldPlan.simple(
                    param, kind, "null != " + name + " ? " + name + ".name() : null",
                    tagOrDefault(tagAnnotation, "ENUM_TAG"), allowTruncate);

            case BUFFER:
                return buildBufferFieldPlan(param, name, bufferViewSpec, tagAnnotation, allowTruncate);

            case ADDRESS:
                return buildAddressFieldPlan(param, name, tagAnnotation, allowTruncate);

            default:
                throw new IllegalStateException("unhandled kind: " + kind);
        }
    }

    private FieldPlan buildBufferFieldPlan(
        final VariableElement param,
        final String name,
        final BufferViewSpec bufferViewSpec,
        final Tag tagAnnotation,
        final boolean allowTruncate)
    {
        final String tagExpr = tagOrDefault(tagAnnotation, "UINT8_TYPED_ARRAY_TAG");
        final boolean isByteBuffer = "java.nio.ByteBuffer".equals(param.asType().toString());

        final List<String> preamble = new ArrayList<>();
        final String valueExpr;

        if (null != bufferViewSpec && bufferViewSpec.bufferParamName.equals(name))
        {
            preamble.add("final " + UNSAFE_BUFFER_TYPE + " " + name + "View = bufferViewThreadLocal.get();");
            preamble.add(name + "View.wrap(" + name + ", " + bufferViewSpec.offsetParamName + ", " +
                bufferViewSpec.lengthParamName + ");");
            valueExpr = name + "View";
        }
        else if (isByteBuffer)
        {
            preamble.add("final " + UNSAFE_BUFFER_TYPE + " " + name + "View = bufferViewThreadLocal.get();");
            preamble.add(name + "View.wrap(" + name + ", " + name + ".position(), " + name + ".remaining());");
            valueExpr = name + "View";
        }
        else
        {
            valueExpr = name;
        }

        return new FieldPlan(param, Kind.BUFFER, valueExpr, tagExpr, allowTruncate, preamble, true, false);
    }

    private FieldPlan buildAddressFieldPlan(
        final VariableElement param, final String name, final Tag tagAnnotation, final boolean allowTruncate)
    {
        final boolean isSocketAddress = "java.net.InetSocketAddress".equals(param.asType().toString());
        final String bytesExpr = isSocketAddress ? name + ".getAddress().getAddress()" : name + ".getAddress()";
        final String instanceofTarget = isSocketAddress ? name + ".getAddress()" : name;
        final boolean dynamicTag = null == tagAnnotation;

        final List<String> preamble = new ArrayList<>();
        preamble.add("final " + UNSAFE_BUFFER_TYPE + " " + name + "View;");
        if (dynamicTag)
        {
            preamble.add("final long " + name + "Tag;");
        }
        preamble.add("if (null != " + name + ")");
        preamble.add("{");
        preamble.add("    " + name + "View = addressViewThreadLocal.get();");
        preamble.add("    " + name + "View.wrap(" + bytesExpr + ");");
        if (dynamicTag)
        {
            preamble.add("    " + name + "Tag = (" + instanceofTarget + " instanceof java.net.Inet6Address) ? " +
                CBOR_UTILS_TYPE + ".IPV6_TAG : " + CBOR_UTILS_TYPE + ".IPV4_TAG;");
        }
        preamble.add("}");
        preamble.add("else");
        preamble.add("{");
        preamble.add("    " + name + "View = null;");
        if (dynamicTag)
        {
            preamble.add("    " + name + "Tag = " + CBOR_UTILS_TYPE + ".IPV4_TAG;");
        }
        preamble.add("}");

        final String tagExpr = dynamicTag ? name + "Tag" : tagAnnotation.value() + "L";

        return new FieldPlan(param, Kind.ADDRESS, name + "View", tagExpr, allowTruncate, preamble, false, true);
    }

    private String tagOrDefault(final Tag tagAnnotation, final String defaultConstantSimpleName)
    {
        return null != tagAnnotation ? tagAnnotation.value() + "L" : CBOR_UTILS_TYPE + "." + defaultConstantSimpleName;
    }

    private Kind classifyKind(final VariableElement param, final ExecutableElement method)
    {
        final TypeMirror type = param.asType();
        final TypeKind typeKind = type.getKind();

        if (TypeKind.INT == typeKind || TypeKind.LONG == typeKind || TypeKind.SHORT == typeKind)
        {
            return Kind.NUMBER;
        }
        if (TypeKind.BOOLEAN == typeKind)
        {
            return Kind.BOOLEAN;
        }
        if (isEnumLike(type))
        {
            return Kind.ENUM;
        }

        final String typeName = type.toString();
        if ("java.lang.String".equals(typeName) || "java.lang.CharSequence".equals(typeName))
        {
            return Kind.STRING;
        }
        if (isBufferType(type))
        {
            return Kind.BUFFER;
        }
        if ("java.net.InetSocketAddress".equals(typeName) || "java.net.InetAddress".equals(typeName))
        {
            return Kind.ADDRESS;
        }

        error(param, "@LoggerMethod parameter '" + param.getSimpleName() + "' of method '" +
            method.getSimpleName() + "' has unsupported type '" + typeName + "' for CBOR encoding " +
            "(supported: int/long/short, boolean, String, enum types, DirectBuffer/ByteBuffer, " +
            "InetSocketAddress/InetAddress)");
        return null;
    }

    private boolean isEnumLike(final TypeMirror type)
    {
        if (TypeKind.TYPEVAR == type.getKind())
        {
            return isEnumLike(((TypeVariable)type).getUpperBound());
        }
        if (TypeKind.DECLARED != type.getKind())
        {
            return false;
        }

        final Element element = processingEnv.getTypeUtils().asElement(type);
        if (!(element instanceof TypeElement))
        {
            return false;
        }

        final TypeElement typeElement = (TypeElement)element;
        return ElementKind.ENUM == typeElement.getKind() ||
            "java.lang.Enum".contentEquals(typeElement.getQualifiedName());
    }

    private static boolean isBufferType(final TypeMirror type)
    {
        final String typeName = type.toString();
        return "org.agrona.DirectBuffer".equals(typeName) || "java.nio.ByteBuffer".equals(typeName);
    }

    private static VariableElement findParam(final ExecutableElement method, final String name)
    {
        for (final VariableElement param : method.getParameters())
        {
            if (param.getSimpleName().contentEquals(name))
            {
                return param;
            }
        }
        return null;
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
        final List<CborMethodPlan> plans,
        final boolean usesBuffer,
        final boolean usesAddress)
    {
        out.printf("package %s;%n%n", packageName);
        out.printf("final class %s implements %s%n{%n", implName, interfaceName);
        out.printf("    private static final int MAX_BUFFER_LENGTH = %d;%n%n", MAX_BUFFER_LENGTH);
        out.printf("    private final %s ringBuffer;%n", MANY_TO_ONE_RING_BUFFER_TYPE);
        out.printf("    private final ThreadLocal<%s> encodingStateThreadLocal = ThreadLocal.withInitial(%s::new);%n",
            ENCODING_STATE_TYPE, ENCODING_STATE_TYPE);
        if (usesBuffer)
        {
            out.printf(
                "    private final ThreadLocal<%s> bufferViewThreadLocal = ThreadLocal.withInitial(%s::new);%n",
                UNSAFE_BUFFER_TYPE, UNSAFE_BUFFER_TYPE);
        }
        if (usesAddress)
        {
            out.printf(
                "    private final ThreadLocal<%s> addressViewThreadLocal = ThreadLocal.withInitial(%s::new);%n",
                UNSAFE_BUFFER_TYPE, UNSAFE_BUFFER_TYPE);
        }
        out.println();
        out.printf("    %s(final %s eventRingBuffer)%n    {%n", implName, MANY_TO_ONE_RING_BUFFER_TYPE);
        out.printf("        this.ringBuffer = eventRingBuffer;%n    }%n");

        for (final CborMethodPlan plan : plans)
        {
            out.println();
            writeMethod(out, plan);
        }

        out.printf("}%n");
    }

    private void writeMethod(final PrintWriter out, final CborMethodPlan plan)
    {
        final ExecutableElement method = plan.method;

        out.printf("    @Override%n    public %svoid %s(%s)%n    {%n",
            renderTypeParameters(method), method.getSimpleName(), renderParameters(method));
        out.printf("        final long timestampNanos = System.nanoTime();%n");

        boolean printedPreambleBlank = false;
        for (final FieldPlan field : plan.fields)
        {
            if (!field.preamble.isEmpty())
            {
                if (!printedPreambleBlank)
                {
                    out.println();
                    printedPreambleBlank = true;
                }
                for (final String line : field.preamble)
                {
                    out.println("        " + line);
                }
            }
        }

        out.println();
        out.printf("        int length = %s.lengthHeader(%s, timestampNanos);%n", CBOR_ENCODE_TYPE,
            plan.eventCodeExpr);
        for (final FieldPlan field : plan.fields)
        {
            out.println("        length += " + renderLengthCall(field) + ";");
        }
        out.printf("        length += %s.lengthFooter();%n", CBOR_ENCODE_TYPE);
        out.println();
        out.println("        final int bufferLength = Math.min(length, MAX_BUFFER_LENGTH);");
        out.printf("        final int index = ringBuffer.tryClaim(%s.toEventCodeId(), bufferLength);%n",
            plan.eventCodeExpr);
        out.println();
        out.printf("        final %s encodingState = encodingStateThreadLocal.get();%n", ENCODING_STATE_TYPE);
        out.println("        encodingState.reset(ringBuffer.buffer(), index, bufferLength);");
        out.println();
        out.println("        try");
        out.println("        {");
        out.printf("            %s.encodeHeader(encodingState, %s, timestampNanos);%n", CBOR_ENCODE_TYPE,
            plan.eventCodeExpr);
        for (final FieldPlan field : plan.fields)
        {
            out.println("            " + renderEncodeCall(field) + ";");
        }
        out.printf("            %s.encodeFooter(encodingState);%n", CBOR_ENCODE_TYPE);
        out.println("        }");
        out.println("        finally");
        out.println("        {");
        out.println("            ringBuffer.commit(index);");
        out.println("        }");
        out.println("    }");
    }

    private String renderLengthCall(final FieldPlan field)
    {
        final String key = "\"" + field.param.getSimpleName() + "\"";
        if (Kind.BOOLEAN == field.kind)
        {
            return CBOR_ENCODE_TYPE + ".length(" + key + ", " + field.valueExpr + ")";
        }
        return CBOR_ENCODE_TYPE + ".length(" + key + ", " + field.tagExpr + ", " + field.valueExpr + ")";
    }

    private String renderEncodeCall(final FieldPlan field)
    {
        final String key = "\"" + field.param.getSimpleName() + "\"";
        if (Kind.BOOLEAN == field.kind)
        {
            return CBOR_ENCODE_TYPE + ".encode(encodingState, " + key + ", " + field.valueExpr + ")";
        }
        if (Kind.NUMBER == field.kind)
        {
            return CBOR_ENCODE_TYPE + ".encode(encodingState, " + key + ", " + field.tagExpr + ", " +
                field.valueExpr + ")";
        }
        return CBOR_ENCODE_TYPE + ".encode(encodingState, " + key + ", " + field.tagExpr + ", " + field.valueExpr +
            ", " + field.allowTruncate + ")";
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

    private enum Kind
    {
        NUMBER, BOOLEAN, STRING, ENUM, BUFFER, ADDRESS
    }

    private record BufferViewSpec(String bufferParamName, String offsetParamName, String lengthParamName)
    {
    }

    private record FieldPlan(
        VariableElement param,
        Kind kind,
        String valueExpr,
        String tagExpr,
        boolean allowTruncate,
        List<String> preamble,
        boolean usesBufferField,
        boolean usesAddressField)
    {
        private static FieldPlan simple(
            final VariableElement param,
            final Kind kind,
            final String valueExpr,
            final String tagExpr,
            final boolean allowTruncate)
        {
            return new FieldPlan(param, kind, valueExpr, tagExpr, allowTruncate, List.of(), false, false);
        }
    }

    private record CborMethodPlan(
        ExecutableElement method,
        String eventCodeExpr,
        List<FieldPlan> fields,
        boolean usesBufferField,
        boolean usesAddressField)
    {
    }
}
