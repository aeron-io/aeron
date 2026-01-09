
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include "concurrent/aeron_spsc_concurrent_array_queue.h"


static inline void cpu_relax(void)
{
    __asm__ __volatile__("pause");
}

static inline uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec * 1000000000ull) + (uint64_t)ts.tv_nsec;
}

static int pin_self_to_cpu(int cpu)
{
    if (cpu < 0)
    {
        return 0;
    }

    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET((unsigned)cpu, &set);

    const int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (0 != rc)
    {
        errno = rc;
        return -1;
    }

    return 0;
}

typedef struct bench_ctx_stct
{
    aeron_spsc_concurrent_array_queue_t queue;

    size_t items;
    size_t queue_length;

    int producer_cpu;
    int consumer_cpu;

    uint64_t *values;

    pthread_barrier_t start_barrier;

    volatile uint64_t consumer_sum;
    volatile int producer_err;
    volatile int consumer_err;
}
bench_ctx_t;

static void *producer_main(void *arg)
{
    bench_ctx_t *ctx = (bench_ctx_t *)arg;

    if (0 != pin_self_to_cpu(ctx->producer_cpu))
    {
        ctx->producer_err = errno ? errno : 1;
        return NULL;
    }

    pthread_barrier_wait(&ctx->start_barrier);

    for (size_t i = 0; i < ctx->items; i++)
    {
        void *elem = (void *)&ctx->values[i];

        while (AERON_OFFER_SUCCESS !=
               aeron_spsc_concurrent_array_queue_offer(&ctx->queue, elem))
        {
            cpu_relax();
        }
    }

    return NULL;
}

static void *consumer_main(void *arg)
{
    bench_ctx_t *ctx = (bench_ctx_t *)arg;

    if (0 != pin_self_to_cpu(ctx->consumer_cpu))
    {
        ctx->consumer_err = errno ? errno : 1;
        return NULL;
    }

    pthread_barrier_wait(&ctx->start_barrier);

    uint64_t sum = 0;

    for (size_t i = 0; i < ctx->items; i++)
    {
        void *elem;

        while (NULL ==
               (elem = aeron_spsc_concurrent_array_queue_poll(&ctx->queue)))
        {
            cpu_relax();
        }

        sum += *(uint64_t *)elem;
    }

    ctx->consumer_sum = sum;
    return NULL;
}

static void usage(const char *prog)
{
    fprintf(stderr,
        "Usage: %s [-n items] [-q queue_length] [-p producer_cpu] [-c consumer_cpu]\n"
        "  -n items        number of items (default: 100000000)\n"
        "  -q length       queue length (power of two recommended, default: 1024)\n"
        "  -p cpu          pin producer to CPU (default: -1)\n"
        "  -c cpu          pin consumer to CPU (default: -1)\n",
        prog);
}

int main(int argc, char **argv)
{
    bench_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    ctx.items = 100000000ull;
    ctx.queue_length = 1024;
    ctx.producer_cpu = -1;
    ctx.consumer_cpu = -1;

    int opt;
    while ((opt = getopt(argc, argv, "n:q:p:c:h")) != -1)
    {
        switch (opt)
        {
            case 'n':
                ctx.items = (size_t)strtoull(optarg, NULL, 10);
                break;

            case 'q':
                ctx.queue_length = (size_t)strtoull(optarg, NULL, 10);
                break;

            case 'p':
                ctx.producer_cpu = (int)strtol(optarg, NULL, 10);
                break;

            case 'c':
                ctx.consumer_cpu = (int)strtol(optarg, NULL, 10);
                break;

            case 'h':
            default:
                usage(argv[0]);
                return (opt == 'h') ? 0 : 1;
        }
    }

    ctx.values = malloc(ctx.items * sizeof(uint64_t));
    if (NULL == ctx.values)
    {
        fprintf(stderr, "malloc failed: %s\n", strerror(errno));
        return 1;
    }

    for (size_t i = 0; i < ctx.items; i++)
    {
        ctx.values[i] = (uint64_t)i + 1;
    }

    if (0 != aeron_spsc_concurrent_array_queue_init(&ctx.queue, ctx.queue_length))
    {
        fprintf(stderr, "queue init failed\n");
        free(ctx.values);
        return 1;
    }

    if (0 != pthread_barrier_init(&ctx.start_barrier, NULL, 3))
    {
        fprintf(stderr, "barrier init failed\n");
        aeron_spsc_concurrent_array_queue_close(&ctx.queue);
        free(ctx.values);
        return 1;
    }

    pthread_t producer;
    pthread_t consumer;

    pthread_create(&producer, NULL, producer_main, &ctx);
    pthread_create(&consumer, NULL, consumer_main, &ctx);

    pthread_barrier_wait(&ctx.start_barrier);
    const uint64_t t0 = now_ns();

    pthread_join(producer, NULL);
    pthread_join(consumer, NULL);

    const uint64_t t1 = now_ns();

    const uint64_t dur_ns = t1 - t0;
    const double secs = (double)dur_ns / 1e9;

    printf("items=%zu queue=%zu prod_cpu=%d cons_cpu=%d\n",
        ctx.items, ctx.queue_length, ctx.producer_cpu, ctx.consumer_cpu);
    printf("time=%.6f s  throughput=%.3f ops/s  ns/op=%.3f  sum=%" PRIu64 "\n",
        secs,
        (double)ctx.items / secs,
        (double)dur_ns / (double)ctx.items,
        ctx.consumer_sum);

    pthread_barrier_destroy(&ctx.start_barrier);
    aeron_spsc_concurrent_array_queue_close(&ctx.queue);
    free(ctx.values);

    return 0;
}
