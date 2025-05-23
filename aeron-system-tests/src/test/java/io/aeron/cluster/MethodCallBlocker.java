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
package io.aeron.cluster;

import io.aeron.test.Tests;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.TypeValidation;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaModule;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class MethodCallBlocker
{
    private static final Method RUNNABLE_RUN_METHOD = runnableRunMethod();
    private static final String ON_ENTER_FIELD_NAME = "onEnter";
    private final HashMap<String, MethodCallHandler> methodCallHandlers = new HashMap<>();
    private final Instrumentation instrumentation = ByteBuddyAgent.install();

    public Controller getControllerFor(
        final String className,
        final String methodName,
        final String threadName
    )
    {
        return obtainInstrumentedMethodCallHandler(className, methodName).getControllerForThread(threadName);
    }

    public void removeInstrumentation()
    {
        for (final MethodCallHandler methodCallHandler : methodCallHandlers.values())
        {
            methodCallHandler.removeInstrumentation();
        }
        methodCallHandlers.clear();
    }

    private static Method runnableRunMethod()
    {
        try
        {
            return Runnable.class.getMethod("run");
        }
        catch (final NoSuchMethodException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    private MethodCallHandler obtainInstrumentedMethodCallHandler(final String className, final String methodName)
    {
        final String key = className + "#" + methodName;
        return methodCallHandlers.computeIfAbsent(key, ignored -> instrumentMethodCall(className, methodName));
    }

    private MethodCallHandler instrumentMethodCall(
        final String className,
        final String methodName)
    {
        final AgentBuilder agentBuilder = new AgentBuilder.Default(new ByteBuddy()
            .with(TypeValidation.DISABLED))
            .disableClassFormatChanges()
            .with(new AgentBuilderListener())
            .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);

        final String adviceClassName = "io.aeron.cluster.DynamicThreadBlockAdvice" +
            UUID.randomUUID().toString().replace("-", "");

        try (DynamicType.Unloaded<Object> adviceType = createAdviceType(adviceClassName))
        {
            final Class<?> dynamicAdviceClass = adviceType
                .load(MethodCallBlocker.class.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();

            final MethodCallHandler methodCallHandler = new MethodCallHandler();

            setStaticOnEnterField(dynamicAdviceClass, methodCallHandler::onEnter);

            final TypeDescription adviceTypeDesc = adviceType.getTypeDescription();

            @SuppressWarnings("resource") final ClassFileLocator.Simple classFileLocator = new ClassFileLocator.Simple(
                Map.of(adviceTypeDesc.getName(), adviceType.getBytes())
            );

            final AgentBuilder.Transformer change = (builder, typeDescription, classLoader, module, protectionDomain) ->
                builder.visit(Advice.to(adviceTypeDesc, classFileLocator).on(ElementMatchers.named(methodName)));

            final ResettableClassFileTransformer transformer = agentBuilder
                .type(ElementMatchers.named(className))
                .transform(change)
                .installOn(instrumentation);

            methodCallHandler.transformer(transformer);

            return methodCallHandler;
        }
    }

    private static void setStaticOnEnterField(final Class<?> dynamicAdviceClass, final Runnable onEnterRunnable)
    {
        try
        {
            final Field onEnterField = dynamicAdviceClass.getField(ON_ENTER_FIELD_NAME);
            onEnterField.set(null, onEnterRunnable);
        }
        catch (final NoSuchFieldException | IllegalAccessException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    private static FieldDescription getOnEnterFieldDescription(final String adviceClassName)
    {
        final FieldDescription onEnterDescription;
        try (DynamicType.Unloaded<Object> firstPass = new ByteBuddy()
            .subclass(Object.class)
            .name(adviceClassName)
            .defineField(ON_ENTER_FIELD_NAME, Runnable.class, Modifier.PUBLIC | Modifier.STATIC)
            .make())
        {

            onEnterDescription = firstPass.getTypeDescription()
                .getDeclaredFields()
                .filter(ElementMatchers.named(ON_ENTER_FIELD_NAME))
                .getOnly();
        }
        return onEnterDescription;
    }

    private static DynamicType.Unloaded<Object> createAdviceType(final String adviceClassName)
    {
        final FieldDescription onEnterDescription = getOnEnterFieldDescription(adviceClassName);

        return new ByteBuddy()
            .subclass(Object.class)
            .name(adviceClassName)
            .defineField(ON_ENTER_FIELD_NAME, Runnable.class, Modifier.PUBLIC | Modifier.STATIC)
            .defineMethod("enter", void.class, Modifier.PUBLIC | Modifier.STATIC)
            .intercept(new Implementation.Simple(new StackManipulation.Compound(
                FieldAccess.forField(onEnterDescription).read(),
                MethodInvocation.invoke(new MethodDescription.ForLoadedMethod(RUNNABLE_RUN_METHOD)),
                MethodReturn.VOID
            )))
            .annotateMethod(AnnotationDescription.Builder.ofType(Advice.OnMethodEnter.class).build())
            .make();
    }

    private final class MethodCallHandler
    {
        private final ConcurrentHashMap<Thread, Controller> controllers = new ConcurrentHashMap<>();
        private ResettableClassFileTransformer transformer;

        public void transformer(final ResettableClassFileTransformer transformer)
        {
            this.transformer = transformer;
        }

        public Controller getControllerForThread(final String threadName)
        {
            final Set<Thread> threads = Thread.getAllStackTraces().keySet();
            Thread matchingThread = null;

            for (final Thread thread : threads)
            {
                if (thread.getName().equals(threadName))
                {
                    if (matchingThread != null)
                    {
                        throw new IllegalStateException("Multiple threads match: '" + threadName + "'");
                    }

                    matchingThread = thread;
                }
            }

            if (matchingThread == null)
            {
                throw new IllegalStateException("No thread found matching: '" + threadName + "'. Available threads: " +
                    threads.stream().map(Thread::getName).toList());
            }

            return getControllerForThread(matchingThread);
        }

        private Controller getControllerForThread(final Thread thread)
        {
            return controllers.computeIfAbsent(thread, ignored -> new Controller());
        }

        private void removeInstrumentation()
        {
            transformer.reset(instrumentation, AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);
        }

        private void onEnter()
        {
            final Controller controller = getControllerForThread(Thread.currentThread());
            controller.onEnter();
        }
    }

    private static final class AgentBuilderListener extends AgentBuilder.Listener.Adapter
    {
        @SuppressWarnings("NullableProblems")
        public void onError(
            final String typeName,
            final ClassLoader classLoader,
            final JavaModule javaModule,
            final boolean loaded,
            final Throwable throwable)
        {
            System.err.println("typeName=" + typeName +
                ", classLoader=" + classLoader +
                ", javaModule=" + javaModule +
                ", loaded=" + loaded);
            throwable.printStackTrace(System.err);
        }
    }

    public static final class Controller
    {
        private static final int OPEN = 0;
        private static final int BLOCKING = 1;
        private static final int BLOCKED = 2;
        private final AtomicInteger state = new AtomicInteger(OPEN);

        private void onEnter()
        {
            if (state.compareAndSet(BLOCKING, BLOCKED))
            {
                while (state.get() == BLOCKED)
                {
                    Tests.sleep(1);
                }
            }
        }

        public void blockNextEntry()
        {
            if (!state.compareAndSet(OPEN, BLOCKING))
            {
                throw new IllegalStateException("expected OPEN state");
            }
        }

        public void awaitBlocked()
        {
            if (state.get() == OPEN)
            {
                throw new IllegalStateException("expected BLOCKING or BLOCKED state");
            }

            while (state.get() != BLOCKED)
            {
                Tests.sleep(1);
            }
        }

        public void release()
        {
            state.set(OPEN);
        }
    }
}
