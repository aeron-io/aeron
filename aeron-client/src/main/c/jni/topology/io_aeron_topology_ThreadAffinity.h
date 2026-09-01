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

#ifndef AERON_IO_AERON_TOPOLOGY_THREADAFFINITY_H
#define AERON_IO_AERON_TOPOLOGY_THREADAFFINITY_H

#include <jni.h>

JNIEXPORT void JNICALL Java_io_aeron_topology_ThreadAffinity_setAffinity(JNIEnv *env, jclass clazz, jint cpu);
JNIEXPORT jint JNICALL Java_io_aeron_topology_ThreadAffinity_getAffinity(JNIEnv *env, jclass clazz);

// Test method only (for more visible results)
JNIEXPORT void JNICALL Java_io_aeron_topology_ThreadAffinity_setThreadName(JNIEnv *env, jclass clazz, jstring name);


#endif //AERON_IO_AERON_TOPOLOGY_THREADAFFINITY_H
