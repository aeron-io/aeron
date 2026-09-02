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

#include "io_aeron_topology_ThreadAffinity.h"
#include "concurrent/aeron_thread.h"
#include "util/aeron_error.h"

JNIEXPORT void JNICALL Java_io_aeron_topology_ThreadAffinity_setAffinity(JNIEnv *env, jclass clazz, jint cpu)
{
    if (cpu < 0)
    {
        (*env)->ThrowNew(env, (*env)->FindClass(env, "io/aeron/exceptions/AeronException"), "cpu out of range");
        return;
    }

    if (aeron_thread_set_affinity("java_thread", (uint8_t)cpu) < 0)
    {
        (*env)->ThrowNew(env, (*env)->FindClass(env, "io/aeron/exceptions/AeronException"), aeron_errmsg());
        return;
    }
}

JNIEXPORT jint JNICALL Java_io_aeron_topology_ThreadAffinity_getAffinity(JNIEnv *env, jclass clazz)
{
    uint8_t cpu;
    if (aeron_thread_get_affinity(&cpu) < 0)
    {
        (*env)->ThrowNew(env, (*env)->FindClass(env, "io/aeron/exceptions/AeronException"), aeron_errmsg());
        return -1;
    }
    return (jint)cpu;
}

JNIEXPORT void JNICALL Java_io_aeron_topology_ThreadAffinity_setThreadName(JNIEnv *env, jclass clazz, jstring name)
{
    const char *c_name = (*env)->GetStringUTFChars(env, name, NULL);
    if (aeron_thread_set_name(c_name) < 0)
    {
        (*env)->ThrowNew(env, (*env)->FindClass(env, "io/aeron/exceptions/AeronException"), aeron_errmsg());
        return;
    }
}
