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

#include "aeron_cluster.h"
#include "aeron_cluster_credentials_supplier.h"

aeron_cluster_encoded_credentials_t *aeron_cluster_credentials_supplier_encoded_credentials(
    aeron_cluster_credentials_supplier_t *supplier)
{
    if (NULL == supplier->encoded_credentials)
    {
        return NULL;
    }

    return supplier->encoded_credentials(supplier->clientd);
}

aeron_cluster_encoded_credentials_t *aeron_cluster_credentials_supplier_on_challenge(
    aeron_cluster_credentials_supplier_t *supplier,
    aeron_cluster_encoded_credentials_t *encoded_challenge)
{
    if (NULL == supplier->on_challenge)
    {
        return NULL;
    }

    return supplier->on_challenge(encoded_challenge, supplier->clientd);
}

void aeron_cluster_credentials_supplier_on_free(
    aeron_cluster_credentials_supplier_t *supplier,
    aeron_cluster_encoded_credentials_t *credentials)
{
    if (NULL != supplier->on_free)
    {
        supplier->on_free(credentials, supplier->clientd);
    }
}
