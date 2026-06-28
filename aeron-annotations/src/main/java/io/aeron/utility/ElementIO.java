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

import javax.tools.FileObject;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utility class for wring configuration elements.
 */
public final class ElementIO
{
    private ElementIO()
    {
    }

    /**
     * Read a list of objects from the named file.
     *
     * @param elementsFilename the name of the filename that contains a list of objects
     * @return a list of elements
     * @param <T> the type of elements - ConfigInfo or CounterInfo
     * @throws Exception on IO failure.
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> read(final String elementsFilename) throws Exception
    {
        try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(Paths.get(elementsFilename))))
        {
            return ((List<T>)in.readObject());
        }
    }

    /**
     * Write a list of objects to the named file.
     *
     * @param resourceFile the destination file to write to
     * @param elements a Collection of elements
     * @throws Exception on IO failure.
     */
    public static void write(final FileObject resourceFile, final Collection<?> elements) throws Exception
    {
        try (ObjectOutputStream out = new ObjectOutputStream(resourceFile.openOutputStream()))
        {
            out.writeObject(new ArrayList<>(elements));
        }
    }
}
