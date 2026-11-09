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

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.TypeValidation;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaModule;

import java.lang.instrument.Instrumentation;

class ClusterInstrumentor
{
    private final ResettableClassFileTransformer resettableClassFileTransformer;
    private final Instrumentation instrumentation;

    ClusterInstrumentor(
        final Class<?> adviceClass,
        final String simpleClassName,
        final String methodName)
    {
        instrumentation = ByteBuddyAgent.install();

        final AgentBuilder agentBuilder = new AgentBuilder.Default(new ByteBuddy()
            .with(TypeValidation.DISABLED))
            .disableClassFormatChanges()
            .with(new AgentBuilderListener())
            .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);

        resettableClassFileTransformer = agentBuilder
            .type(ElementMatchers.nameEndsWith(simpleClassName))
            .transform(
                (builder, typeDescription, classLoader, module, protectionDomain) ->
                    builder.visit(Advice.to(adviceClass).on(ElementMatchers.named(methodName))))
            .installOn(instrumentation);
    }

    void reset()
    {
        resettableClassFileTransformer.reset(instrumentation, AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);
    }

    static class AgentBuilderListener implements AgentBuilder.Listener
    {
        @Override
        public void onDiscovery(
            final String typeName,
            final ClassLoader classLoader,
            final JavaModule javaModule,
            final boolean loaded)
        {
        }

        @Override
        public void onTransformation(
            final TypeDescription typeDescription,
            final ClassLoader classLoader,
            final JavaModule javaModule,
            final boolean loaded,
            final DynamicType dynamicType)
        {
        }

        @Override
        public void onIgnored(
            final TypeDescription typeDescription,
            final ClassLoader classLoader,
            final JavaModule javaModule,
            final boolean loaded)
        {
        }

        @Override
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

        @Override
        public void onComplete(
            final String typeName,
            final ClassLoader classLoader,
            final JavaModule javaModule,
            final boolean loaded)
        {
        }
    }
}
