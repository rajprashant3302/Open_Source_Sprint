import {
    describe,
    it,
    beforeAll,
    afterAll,
    expect,
} from '@jest/globals';
import {
    initializeRedis,
    closeRedis,
    getRedisClient
} from '../services/redis';
import { TaskQueue } from '../services/task-queue';
import { TaskExecutor } from '../services/task-executor';
import { WorkerPool } from '../services/worker-pool';
import { MetricsCollector } from '../services/metrics-collector';

describe('System Load Test', () => {
    const LOAD_RATES = [100, 500, 1000, 2000, 5000];

    let workerId: string;
    let stopWorkerLoop = false;
    let workerLoopPromise: Promise<void>;

    beforeAll(async () => {
        workerLoopPromise = startWorkerLoop();
        await initializeRedis(
            process.env.REDIS_URL || 'redis://localhost:6379'
        );

        TaskExecutor.registerHandler(
            'load-test-handler',
            async () => {
                await new Promise((resolve) =>
                    setTimeout(resolve, 10)
                );

                return { success: true };
            }
        );

        const worker = await WorkerPool.registerWorker(
            'load-test-worker',
            ['load-test-handler'],
            {
                maxConcurrent: 100,
            }
        );

        workerId = worker.id;

        startWorkerLoop();
    });

    afterAll(async () => {
        stopWorkerLoop = true;

        if (workerLoopPromise) {
            await workerLoopPromise;
        }

        await closeRedis();
    });

    async function startWorkerLoop(): Promise<void> {
        const client = getRedisClient();

        while (!stopWorkerLoop) {
            try {
                const task =
                    await TaskQueue.getNextTask('load-test');

                if (!task) {
                    await sleep(50);
                    continue;
                }

                await WorkerPool.assignTask(
                    workerId,
                    task
                );

                await TaskExecutor.execute(
                    workerId,
                    task
                );

                await client.zRem(
                    `queue:${task.queue}`,
                    task.id
                );
            } catch (err) {
                console.error(err);

                await sleep(100);
            }
        }
    }

    it(
        'should establish baseline metrics and identify breakpoints',
        async () => {
            const results: any[] = [];

            for (const rate of LOAD_RATES) {
                console.log('\n====================');
                console.log(
                    `Running ${rate} tasks/sec`
                );
                console.log('====================');

                const metrics =
                    await runLoadScenario(rate);

                results.push(metrics);

                console.table(metrics);

                if (
                    metrics.avgQueueLatency > 5000 ||
                    metrics.avgCompletionTime > 10000
                ) {
                    console.log(
                        `BREAKPOINT DETECTED @ ${rate} tasks/sec`
                    );
                    break;
                }
            }

            console.log('\nBASELINE RESULTS');
            console.table(results);

            expect(results.length).toBeGreaterThan(0);
        },
        600000
    );

    async function runLoadScenario(
        tasksPerSecond: number
    ) {
        const DURATION_SECONDS = 10;

        const totalTasks =
            tasksPerSecond * DURATION_SECONDS;

        const taskIds: string[] = [];

        const startBenchmark = Date.now();

        // Create tasks
        for (
            let second = 0;
            second < DURATION_SECONDS;
            second++
        ) {
            const batch = [];

            for (
                let i = 0;
                i < tasksPerSecond;
                i++
            ) {
                batch.push(
                    TaskQueue.createTask(
                        `load-${Date.now()}-${i}`,
                        'load-test-handler',
                        {
                            index: i,
                        },
                        {
                            queueName: 'load-test',
                            priority: 'medium',
                        }
                    )
                );
            }

            const created =
                await Promise.all(batch);

            taskIds.push(
                ...created.map((t) => t.id)
            );

            await sleep(1000);
        }

        console.log(
            `Created ${taskIds.length} tasks`
        );

        // Give workers time to process
        await sleep(5000);

        const queueLatencies: number[] = [];
        const completionTimes: number[] = [];

        const tasks = await Promise.all(
            taskIds.map((id) =>
                TaskQueue.getTask(id)
            )
        );

        let completedTasks = 0;

        for (const task of tasks) {
            if (!task) continue;

            if (
                task.startedAt &&
                task.completedAt
            ) {
                completedTasks++;

                const createdAt =
                    new Date(
                        task.createdAt
                    ).getTime();

                const startedAt =
                    new Date(
                        task.startedAt
                    ).getTime();

                const completedAt =
                    new Date(
                        task.completedAt
                    ).getTime();

                queueLatencies.push(
                    startedAt - createdAt
                );

                completionTimes.push(
                    completedAt - startedAt
                );
            }
        }

        const elapsed =
            (Date.now() - startBenchmark) /
            1000;

        const workerMetrics =
            await WorkerPool.getWorkerMetrics(
                workerId
            );

        const snapshot =
            await MetricsCollector.captureSnapshot();

        return {
            rate: tasksPerSecond,

            tasksCreated: totalTasks,

            tasksCompleted: completedTasks,

            throughput:
                completedTasks / elapsed,

            avgQueueLatency:
                average(queueLatencies),

            p95QueueLatency:
                percentile(
                    queueLatencies,
                    95
                ),

            p99QueueLatency:
                percentile(
                    queueLatencies,
                    99
                ),

            avgCompletionTime:
                average(completionTimes),

            p95CompletionTime:
                percentile(
                    completionTimes,
                    95
                ),

            p99CompletionTime:
                percentile(
                    completionTimes,
                    99
                ),

            workerSuccessRate:
                workerMetrics.successRate,

            memoryMB:
                Math.round(
                    snapshot.system.memoryUsage
                        .heapUsed /
                    1024 /
                    1024
                ),

            queues:
                Object.keys(snapshot.queues)
                    .length,
        };
    }

    function average(
        values: number[]
    ): number {
        if (!values.length) return 0;

        return (
            values.reduce(
                (a, b) => a + b,
                0
            ) / values.length
        );
    }

    function percentile(
        values: number[],
        p: number
    ): number {
        if (!values.length) return 0;

        const sorted = [...values].sort(
            (a, b) => a - b
        );

        const idx =
            Math.ceil(
                (p / 100) *
                sorted.length
            ) - 1;

        return sorted[
            Math.max(idx, 0)
        ];
    }

    function sleep(
        ms: number
    ): Promise<void> {
        return new Promise((resolve) =>
            setTimeout(resolve, ms)
        );
    }
});