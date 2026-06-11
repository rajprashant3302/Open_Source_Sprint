import { TaskQueue } from '../task-queue';
import dotenv from 'dotenv';
import { initializeRedis, closeRedis } from '../redis';

dotenv.config();

beforeAll(async () => {
  await initializeRedis(process.env.REDIS_URL!);
});

afterAll(async () => {
  await closeRedis();
});

describe('Race Condition Demonstration', () => {
  it('shows last-write-wins behavior', async () => {

    const task = await TaskQueue.createTask(
      'Race Demo',
      'dataProcessor',
      {}
    );

    console.log('\n========================');
    console.log('INITIAL TASK');
    console.log(JSON.stringify(task, null, 2));
    console.log('========================\n');

    const workers = [0, 1, 2];

    await Promise.all(
      workers.map(i =>
        TaskQueue.updateTaskStatus(
          task.id,
          'processing',
          {
            workerId: `worker-${i}`,
            note: `updated-by-worker-${i}`
          }
        )
      )
    );

    const finalTask = await TaskQueue.getTask(task.id);

    console.log('\n========================');
    console.log('FINAL TASK IN REDIS');
    console.log(JSON.stringify(finalTask, null, 2));
    console.log('========================\n');

    expect(finalTask).not.toBeNull();
  });
});