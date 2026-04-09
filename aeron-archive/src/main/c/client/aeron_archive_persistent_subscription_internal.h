/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

#ifndef AERON_AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_INTERNAL_H
#define AERON_AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_INTERNAL_H

#include "aeron_archive_persistent_subscription.h"

int aeron_archive_persistent_subscription_context_conclude(aeron_archive_persistent_subscription_context_t *context);

aeron_counter_t *aeron_archive_persistent_subscription_context_get_state_counter(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Returns the join difference, i.e. the difference between the live position and the replay position
 * at the point the switch to live was attempted. A negative value means the replay was ahead of
 * the live stream. Zero means they were aligned. INT64_MIN means no join has been attempted yet.
 *
 * @param persistent_subscription to check.
 * @return the join difference.
 */
int64_t aeron_archive_persistent_subscription_join_difference(aeron_archive_persistent_subscription_t *persistent_subscription);

/**
 * Overrides the message timeout used for internal deadline calculations.
 * Intended for testing timeout code paths.
 *
 * @param persistent_subscription to modify.
 * @param message_timeout_ns new timeout in nanoseconds.
 */
void aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(
    aeron_archive_persistent_subscription_t *persistent_subscription, uint64_t message_timeout_ns);

#endif //AERON_AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_INTERNAL_H
