/*
 * Copyright 2014-2023 Real Logic Limited.
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
#ifndef AERON_AERON_VERSION_H
#define AERON_AERON_VERSION_H

#include "string"

namespace aeron
{

static constexpr char AERON_VERSION[] = AERON_VERSION_TXT;
static constexpr char AERON_GIT_SHA[] = AERON_VERSION_GITSHA;
static constexpr int AERON_MAJOR_VERSION = AERON_VERSION_MAJOR;
static constexpr int AERON_MINOR_VERSION = AERON_VERSION_MINOR;
static constexpr int AERON_PATCH_VERSION = AERON_VERSION_PATCH;

}

#endif //AERON_AERON_VERSION_H
