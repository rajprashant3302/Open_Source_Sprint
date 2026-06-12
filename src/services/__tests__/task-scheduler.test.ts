import { TaskScheduler } from '../task-scheduler';
import { getRedisClient } from '../redis';

jest.mock('../redis', () => ({
  getRedisClient: jest.fn()
}));

describe('TaskScheduler', () => {
  let redisClient: any;

  beforeEach(() => {
    jest.useFakeTimers();
    redisClient = {
      set: jest.fn(),
      zRange: jest.fn().mockResolvedValue([]),
      get: jest.fn(),
      del: jest.fn(),
      eval: jest.fn().mockResolvedValue(1),
      zAdd: jest.fn(),
      zRem: jest.fn(),
    };
    (getRedisClient as jest.Mock).mockReturnValue(redisClient);
  });

  afterEach(async () => {
    await TaskScheduler.stopScheduler();
    jest.useRealTimers();
    jest.clearAllMocks();
  });

  describe('Concurrency & Lock Management', () => {
    it('should use an atomic Lua script (eval) to release lock to prevent race conditions', async () => {
      redisClient.set.mockResolvedValue(true);
      
      TaskScheduler.startScheduler(50);
      
      await jest.advanceTimersByTimeAsync(1);
      
      expect(redisClient.eval).toHaveBeenCalled();
      expect(redisClient.get).not.toHaveBeenCalledWith('scheduler:lock');
      expect(redisClient.del).not.toHaveBeenCalledWith('scheduler:lock');
    });

    it('should continue polling even if lock acquisition fails', async () => {
      redisClient.set
        .mockResolvedValueOnce(null)
        .mockResolvedValueOnce('OK');

      TaskScheduler.startScheduler(100);
      
      await jest.advanceTimersByTimeAsync(1);
      expect(redisClient.set).toHaveBeenCalledTimes(1);

      await jest.advanceTimersByTimeAsync(100);
      expect(redisClient.set).toHaveBeenCalledTimes(2);
    });
  });
});
