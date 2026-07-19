/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive.client;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class ArchiveExceptionTest
{
    @ParameterizedTest
    @CsvSource({
        "0,GENERIC",
        "1,ACTIVE_LISTING",
        "2,ACTIVE_RECORDING",
        "3,ACTIVE_SUBSCRIPTION",
        "4,UNKNOWN_SUBSCRIPTION",
        "5,UNKNOWN_RECORDING",
        "6,UNKNOWN_REPLAY",
        "7,MAX_REPLAYS",
        "8,MAX_RECORDINGS",
        "9,INVALID_EXTENSION",
        "10,AUTHENTICATION_REJECTED",
        "11,STORAGE_SPACE",
        "12,UNKNOWN_REPLICATION",
        "13,UNAUTHORISED_ACTION",
        "14,REPLICATION_CONNECTION_FAILURE",
        "15,EMPTY_RECORDING",
        "16,INVALID_POSITION" })
    void errorCodeAsString(final int errorCode, final String expected)
    {
        assertEquals(expected, ArchiveException.errorCodeAsString(errorCode));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 1111111, 54})
    void shouldHandlerUnknownErrorCodes(final int errorCode)
    {
        assertEquals("unknown error code: " + errorCode, ArchiveException.errorCodeAsString(errorCode));
    }
}
