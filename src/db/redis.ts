import { createClient, RedisClientType } from 'redis'

interface RedisOptions {
    socket: {
        port: number,
        host: any
    }
}

interface CacheOptions {
    ttl_threshold: number
    cache_key: any
    cache_data: any
}

interface LockOptions {
    lockKey: any
    ttl: number
}

interface LockRetryOptions extends LockOptions {
    maxRetries?: number
    retryDelay?: number // ms
    retryJitter?: number // ms
}


export const RedisClient = createClient({ socket: { 
    port: parseInt(process.env.PORT_REDIS || '6379'), 
    host: process.env.HOST_REDIS || 'localhost'
} })
RedisClient.on('error', (err) => console.error('Redis Client Error', err))
RedisClient.connect()

export async function cacheInvalidation(cache_key: any): Promise<void> {
    const keys = await RedisClient.keys(cache_key)

    for (const key of keys) await RedisClient.del(key)

    console.log('cache links invalidated')
}

export async function checkAndRenewCache({ ttl_threshold, cache_key, cache_data }: CacheOptions): Promise<void> {
    const ttl = await RedisClient.ttl(cache_key)

    if (ttl < ttl_threshold) {
        await RedisClient.set(
            cache_key,
            JSON.stringify(cache_data), {
                EX: 60 * 5 // 5 min
            }
        )
    }
}

export async function acquireLock({ lockKey, ttl }: LockOptions): Promise<boolean> {
    const result = await RedisClient.set(`lock:${lockKey}`, 'locked', {
        EX: ttl,
        NX: true
    })

    if (result === 'OK') console.log('lock acquired')
    else console.log('lock not acquired')
    
    return false   
}

export async function acquireLockWith({ 
    lockKey, 
    ttl, 
    maxRetries = 10, 
    retryDelay = 100,
    retryJitter 
}: LockRetryOptions): Promise<boolean> {
    let lockAcquired = false
    let retryTime = retryDelay

    for (let i = 0; i < maxRetries; i++) {
        lockAcquired = await acquireLock({ lockKey, ttl });
        if (lockAcquired) break

        if (retryJitter) retryTime += Math.floor(Math.random() * retryJitter)

        await new Promise(resolve => setTimeout(resolve, retryTime));
    }

    return lockAcquired
}

export async function extendLock(lockKey: string, ttl: number): Promise<boolean> {
    const ttlNow = await RedisClient.ttl(`lock:${lockKey}`)
    if (ttlNow < 0) return false // -2 if key does not exist

    const result = await RedisClient.expire(`lock:${lockKey}`, ttl + ttlNow)
    return result
}

export async function releaseLock(lockKey: string): Promise<void> {
    await RedisClient.del(`lock:${lockKey}`)
    console.log('lock released')
}