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
package io.aeron.utility;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.tools.Diagnostic;
import java.util.Set;

/**
 * Abstract processor.
 */
public abstract class Processor extends AbstractProcessor
{
    private boolean enabled = false;

    private boolean printNotes = false;

    private Diagnostic.Kind errorKind;

    /**
     * Default constructor.
     */
    public Processor()
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
    public synchronized void init(final ProcessingEnvironment processingEnv)
    {
        enabled = System.getProperty(getEnabledPropertyName(), "true").equalsIgnoreCase("true");
        printNotes = System.getProperty(getPrintNotesPropertyName(), "false").equalsIgnoreCase("true");
        errorKind =
            System.getProperty(getFailOnErrorPropertyName(), "false").equalsIgnoreCase("true") ?
                Diagnostic.Kind.ERROR :
                Diagnostic.Kind.NOTE;
        super.init(processingEnv);
    }

    /**
     * Get enabled property name.
     *
     * @return enabled property name.
     */
    protected abstract String getEnabledPropertyName();

    /**
     * Get print notes property name.
     *
     * @return print notes property name.
     */
    protected abstract String getPrintNotesPropertyName();

    /**
     * Get fail on error property name.
     *
     * @return fail on error property name.
     */
    protected abstract String getFailOnErrorPropertyName();

    /**
     * Process annotations.
     *
     * @param annotations to be processed.
     * @param roundEnv    environment info.
     */
    protected abstract void doProcess(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv);

    /**
     * {@inheritDoc}
     */
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv)
    {
        if (enabled)
        {
            doProcess(annotations, roundEnv);
        }

        return false;
    }

    /**
     * Get doc comment.
     *
     * @param element element.
     * @return comment.
     */
    protected String getDocComment(final Element element)
    {
        final String description = processingEnv.getElementUtils().getDocComment(element);
        if (description == null)
        {
            error("no javadoc found", element);
            return "NO DESCRIPTION FOUND";
        }

        return description.trim();
    }

    /**
     * On error hook.
     *
     * @param errMsg string.
     */
    protected void error(final String errMsg)
    {
        error(errMsg, null);
    }

    /**
     * Error hook with extra context.
     *
     * @param errMsg  message.
     * @param element element.
     */
    protected void error(final String errMsg, final Element element)
    {
        printMessage(errorKind, errMsg, element);
    }

    /**
     * Add a note.
     *
     * @param msg note.
     */
    protected void note(final String msg)
    {
        note(msg, null);
    }

    /**
     * Add note to an element.
     *
     * @param msg     note.
     * @param element element.
     */
    protected void note(final String msg, final Element element)
    {
        if (printNotes)
        {
            printMessage(Diagnostic.Kind.NOTE, msg, element);
        }
    }

    private void printMessage(final Diagnostic.Kind kind, final String msg, final Element element)
    {
        processingEnv.getMessager().printMessage(kind, msg, element);
    }
}
