import { createClient, RedisClientType } from 'redis';
import logger from '../utils/logger';

let redisClient: RedisClientType | null = null;

const MAX_RECONNECT_ATTEMPTS = 10;
const MAX_QUEUE_SIZE = 10000;

let reconnectAttempts = 0;
let redisConnected = false;

interface QueuedOperation<T = any> {
  operation: () => Promise<T>;
  resolve: (value: T) => void;
  reject: (reason?: any) => void;
  retries: number;
}

const operationQueue: QueuedOperation[] = [];

async function flushQueuedOperations(): Promise<void> {
  if (!redisConnected || !redisClient) {
    return;
  }

  logger.info(
    `Flushing ${operationQueue.length} queued Redis operations`
  );

  while (operationQueue.length > 0) {
    const queued = operationQueue.shift();

    if (!queued) {
      continue;
    }

    try {
      const result = await queued.operation();
      queued.resolve(result);
    } catch (error) {
      queued.reject(error);
    }
  }
}

export async function initializeRedis(
  url: string
): Promise<RedisClientType> {
  if (redisClient) {
    return redisClient;
  }

  redisClient = createClient({
    url,
    socket: {
      reconnectStrategy: (retries: number) => {
        reconnectAttempts = retries;

        if (retries >= MAX_RECONNECT_ATTEMPTS) {
          logger.error(
            `Redis reconnection failed after ${MAX_RECONNECT_ATTEMPTS} attempts`
          );

          return new Error('Redis reconnection failed');
        }

        const delay = Math.min(
          1000 * Math.pow(2, retries),
          30000
        );

        logger.warn(
          `Redis reconnect attempt ${retries}. Retrying in ${delay}ms`
        );

        return delay;
      },
    },
  });

  redisClient.on('connect', () => {
    logger.info('Connected to Redis');
    redisConnected = true;
    reconnectAttempts = 0;
  });

  redisClient.on('ready', async () => {
    logger.info('Redis ready');
    redisConnected = true;

    try {
      await flushQueuedOperations();
    } catch (err) {
      logger.error(
        { err },
        'Failed to flush queued Redis operations'
      );
    }
  });

  redisClient.on('reconnecting', () => {
    redisConnected = false;
    logger.warn('Reconnecting to Redis...');
  });

  redisClient.on('end', () => {
    redisConnected = false;
    logger.warn('Redis connection closed');
  });

  redisClient.on('error', (err) => {
    redisConnected = false;
    logger.error({ err }, 'Redis error');
  });

  await redisClient.connect();

  return redisClient;
}

export function getRedisClient(): RedisClientType {
  if (!redisClient) {
    throw new Error(
      'Redis client not initialized. Call initializeRedis first.'
    );
  }

  return new Proxy(redisClient, {
    get(target, prop, receiver) {
      const original = Reflect.get(target, prop, receiver);

      if (typeof original !== 'function') {
        return original;
      }

      return (...args: any[]) => {
        if (redisConnected && target.isOpen) {
          return original.apply(target, args);
        }

        if (operationQueue.length >= MAX_QUEUE_SIZE) {
          return Promise.reject(
            new Error('Redis operation queue is full')
          );
        }

        logger.warn(
          `Redis unavailable. Queuing operation ${String(prop)}`
        );

        return new Promise((resolve, reject) => {
          operationQueue.push({
            operation: () => original.apply(target, args),
            resolve,
            reject,
            retries: 0,
          });
        });
      };
    },
  }) as RedisClientType;
}

/**
 * Execute Redis operation safely.
 * If Redis is disconnected, operation is queued.
 */
export async function executeRedisOperation<T>(
  operation: () => Promise<T>
): Promise<T> {
  if (redisConnected && redisClient?.isOpen) {
    return operation();
  }

  if (operationQueue.length >= MAX_QUEUE_SIZE) {
    throw new Error(
      'Redis operation queue is full'
    );
  }

  logger.warn(
    `Redis unavailable. Queuing operation. Queue size: ${operationQueue.length}`
  );

  return new Promise<T>((resolve, reject) => {
    operationQueue.push({
      operation,
      resolve,
      reject,
      retries: 0,
    });
  });
}

export async function closeRedis(): Promise<void> {
  if (redisClient) {
    await redisClient.quit();
    redisClient = null;
    redisConnected = false;
  }
}

export function isRedisConnected(): boolean {
  return redisConnected;
}

export function getRedisHealth() {
  return {
    connected: redisConnected,
    isOpen: redisClient?.isOpen ?? false,
    reconnectAttempts,
    queuedOperations: operationQueue.length,
    maxReconnectAttempts: MAX_RECONNECT_ATTEMPTS,
  };
}