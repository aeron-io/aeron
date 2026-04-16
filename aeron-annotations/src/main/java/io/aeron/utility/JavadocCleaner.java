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

import java.util.regex.Pattern;

/**
 * Converts Javadoc markup (inline tags, HTML) in description strings to Markdown equivalents,
 * and strips block Javadoc tags such as {@literal @}return, {@literal @}param, etc.
 */
public final class JavadocCleaner
{
    private JavadocCleaner()
    {
    }

    // Inline Javadoc tags
    private static final Pattern CODE_TAG =
        Pattern.compile("\\{@code\\s+([^}]+?)\\s*}");
    private static final Pattern LINK_WITH_LABEL =
        Pattern.compile("\\{@link(?:plain)?\\s+[^\\s}]+\\s+([^}]+?)\\s*}");
    private static final Pattern LINK_NO_LABEL =
        Pattern.compile("\\{@link(?:plain)?\\s+([^\\s}]+)\\s*}");
    private static final Pattern VALUE_TAG =
        Pattern.compile("\\{@value\\s+([^}]+?)\\s*}");
    private static final Pattern OTHER_INLINE_TAG =
        Pattern.compile("\\{@\\w+\\s*([^}]*)}");

    // HTML tags
    private static final Pattern HTML_ANCHOR =
        Pattern.compile("<a\\s+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern HTML_CODE =
        Pattern.compile("<code>(.*?)</code>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern HTML_EM =
        Pattern.compile("<(?:em|i)>(.*?)</(?:em|i)>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern HTML_BOLD =
        Pattern.compile("<(?:b|strong)>(.*?)</(?:b|strong)>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern HTML_LI =
        Pattern.compile("<li>(.*?)(?:</li>|(?=<)|$)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern HTML_BLOCK =
        Pattern.compile("</?(?:p|ul|ol)(?:\\s[^>]*)?>", Pattern.CASE_INSENSITIVE);
    private static final Pattern HTML_OTHER =
        Pattern.compile("<[^>]+>");

    // Block Javadoc tags converted to natural language sentences
    private static final Pattern RETURN_TAG =
        Pattern.compile("\\s*@return\\b\\s+", Pattern.DOTALL);
    private static final Pattern SEE_TAG =
        Pattern.compile("\\s*@see\\b\\s+([^\\s@]+)", Pattern.DOTALL);
    private static final Pattern SINCE_TAG =
        Pattern.compile("\\s*@since\\b\\s+", Pattern.DOTALL);
    private static final Pattern DEPRECATED_TAG =
        Pattern.compile("\\s*@deprecated\\b\\s+", Pattern.DOTALL);
    private static final Pattern THROWS_TAG =
        Pattern.compile("\\s*@(?:throws|exception)\\b\\s+", Pattern.DOTALL);

    // Block Javadoc tags — strip from first occurrence to end
    private static final Pattern BLOCK_TAG =
        Pattern.compile("\\s*@(?:param|serial|serialField)\\b.*",
            Pattern.DOTALL);

    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    /**
     * Clean Javadoc markup from a description string, returning a Markdown-friendly version.
     * Returns {@code null} if the input is {@code null}.
     *
     * @param javadoc the raw Javadoc text to clean
     * @return cleaned Markdown text, or {@code null}
     */
    public static String clean(final String javadoc)
    {
        if (javadoc == null)
        {
            return null;
        }

        String text = javadoc;

        // Inline Javadoc tags
        text = CODE_TAG.matcher(text).replaceAll(m -> "`" + m.group(1) + "`");
        text = LINK_WITH_LABEL.matcher(text).replaceAll(m -> m.group(1).trim());
        text = LINK_NO_LABEL.matcher(text).replaceAll(m -> shortTarget(m.group(1)));
        text = VALUE_TAG.matcher(text).replaceAll(m -> shortTarget(m.group(1)));
        text = OTHER_INLINE_TAG.matcher(text).replaceAll(m -> m.group(1).trim());

        // HTML
        text = HTML_ANCHOR.matcher(text).replaceAll(m -> "[" + m.group(2).trim() + "](" + m.group(1) + ")");
        text = HTML_CODE.matcher(text).replaceAll(m -> "`" + m.group(1) + "`");
        text = HTML_EM.matcher(text).replaceAll(m -> "_" + m.group(1) + "_");
        text = HTML_BOLD.matcher(text).replaceAll(m -> "**" + m.group(1) + "**");
        text = HTML_LI.matcher(text).replaceAll(m -> "- " + m.group(1).trim());
        text = HTML_BLOCK.matcher(text).replaceAll(" ");
        text = HTML_OTHER.matcher(text).replaceAll("");

        // Convert @return/@see/@since to natural language before stripping other block tags
        text = RETURN_TAG.matcher(text).replaceAll(" Returns ");
        text = SEE_TAG.matcher(text).replaceAll(m -> " See " + shortTarget(m.group(1)));
        text = SINCE_TAG.matcher(text).replaceAll(" Since ");
        text = DEPRECATED_TAG.matcher(text).replaceAll(" ");
        text = THROWS_TAG.matcher(text).replaceAll(" Throws ");

        // Block Javadoc tags — strip from first occurrence to end
        text = BLOCK_TAG.matcher(text).replaceAll("");

        // Normalise whitespace
        text = WHITESPACE.matcher(text).replaceAll(" ").trim();

        if (text.isEmpty())
        {
            return null;
        }

        // Return null when cleaning produced no change (comparing against normalised original)
        final String normalisedOriginal = WHITESPACE.matcher(javadoc).replaceAll(" ").trim();
        return text.equals(normalisedOriginal) ? null : text;
    }

    private static String shortTarget(final String target)
    {
        // Preserve trailing sentence punctuation
        String t = target;
        final StringBuilder suffix = new StringBuilder();
        while (!t.isEmpty() && ".,;:".indexOf(t.charAt(t.length() - 1)) >= 0)
        {
            suffix.insert(0, t.charAt(t.length() - 1));
            t = t.substring(0, t.length() - 1);
        }

        final String result;
        final int hashIdx = t.indexOf('#');
        if (hashIdx >= 0)
        {
            final String classPart = t.substring(0, hashIdx);
            final String memberPart = t.substring(hashIdx + 1);
            final int dotIdx = classPart.lastIndexOf('.');
            final String shortClass = dotIdx >= 0 ? classPart.substring(dotIdx + 1) : classPart;
            result = shortClass.isEmpty() ? memberPart : shortClass + "." + memberPart;
        }
        else
        {
            final int dotIdx = t.lastIndexOf('.');
            result = dotIdx >= 0 ? t.substring(dotIdx + 1) : t;
        }

        return result + suffix;
    }
}
