import { Hono } from 'hono'
import { HTTPException } from 'hono/http-exception' 
import { createClient } from 'redis'
import sql from './db/db'
import throw_err from './utils/error-handling'
import {randomBytes} from 'crypto'
import Redlock from 'redlock'

const app = new Hono().basePath('/api/v1')
const clientRedis = createClient({
    socket: {
        port: 6379,
        host: 'localhost' 
    }
})
const redlock = new Redlock([clientRedis], {
    retryCount: 10,
    retryDelay: 500, // in ms -> 0.5s
    retryJitter: 200 // the max time in ms randomly added to retries
})

// * FUNCTION 
const checkConn = async () => {
    try {
        await sql.ping 
        console.log('Connection to database is successful')
        clientRedis
        .on('error', (err) => console.log('Redis Failed to connect with error: ', err))
        .connect()
        console.log('Connection to Redis is successful')
    } catch (e: any) {
        console.log('Failed connection check with error: ', e)
    }
}

const cacheInvalidation = async () => {
    const keys = await clientRedis.keys('url-short:links:*')

    for (let key of keys) await clientRedis.del(key) 
    

    console.log('cache links invalidate')
}

const checkAndRenewCache = async (clientRedis: any, ttl_threshold: number, cache_key: string, cache_data: any) => {
    const ttl = await clientRedis.ttl(cache_key)
    if(ttl < ttl_threshold) {
        await clientRedis.set(
            cache_key, 
            JSON.stringify(cache_data), {
                EX: 60 * 5 // 5 min
            }
        )
    }
}

// * cache lock with redlock
const acquireLock = async (key: string, ttl: number) => {
    const lock = await redlock.acquire([`locks:${key}`], ttl)
    console.log('lock acquired')

    return lock
}

const releaseLock = async (lock: any) => {
    lock.release()
    console.log('lock released')
}

// * cache lock without redlock
const acquireLockRedis = async (lockKey: string, ttl: number) => {
    const result = await clientRedis.set(`lock:${lockKey}`,'locked', {
        EX: ttl,
        NX: true
    })

    if (result === 'OK') console.log('lock acquired')
    else console.log('lock not acquired')
    
    return result === 'OK'
}

const acquireLockRedisWithRetry = async (lockKey: string, ttl: number, maxRetries: number, retryDelay: number, retryJitter = 100) => {
    let lockAcquired = false
    let retryTime = retryDelay

    for(let i = 0; i < maxRetries; i++) {
        lockAcquired = await acquireLockRedis(lockKey, ttl)
        if(lockAcquired) break

        retryTime = retryDelay + Math.floor(Math.random() * retryJitter)
        console.log(retryTime)

        await new Promise(resolve => setTimeout(resolve, retryTime))
    }

    return lockAcquired
}

const releaseLockRedis = async (lockKey: string) => {
    await clientRedis.del(lockKey)
    console.log('lock released')
}

// const invalidateCacheForItem = async (id: any) => {
//     const keys = await clientRedis.keys('url-short:links:*');
//     for (const key of keys) {
//         const cache = await clientRedis.get(key);
//         if (cache) {
//             const links = JSON.parse(cache);
//             if (links.some(link => link.id === id)) {
//                 await clientRedis.del(key);
//                 console.log(`Cache invalidated for key: ${key}`);
//             }
//         }
//     }
// };


// * ROUTES
app.get('/links', async (c) => {
    try {
        const page = parseInt(c.req.query('page') || '1') 
        const size = parseInt(c.req.query('size') || '5')  
        const offset = (page - 1) * size 
        const cacheKey = `url-short:links:page:${page}:limit:${size}`

        let links = await clientRedis.get(cacheKey)
        const ttl_threshold = 30 // 30 sec
        let total_data

        if(!links) {
            let query = 'select id, short_url, long_url, created_at, expired_at, is_active, last_visited, total_visited, user_id from urls where 1=1'
            total_data = (await sql.query(query))[0].length

            query = query + ' limit ' + size + ' offset ' + offset

            links = (await sql.query(query))[0]

            const cacheValue = JSON.stringify({ links, total_data })
            await clientRedis.set(
                cacheKey, 
                cacheValue, {
                    EX: 60 * 5 // 5 min
                }
            )
            console.log('cache set for page:', page)

        } else {
            console.log('using cache links paging')

            const cacheData = JSON.parse(links)
            links = cacheData.links 
            total_data = cacheData.total_data

            // * re cache for renewing ttl if ttl less than threshold
            await checkAndRenewCache(clientRedis, ttl_threshold, cacheKey, {links, total_data})
            
        }

        return c.json({
            errors: false,
            data: {
                page: page,
                per_page: size,
                total: total_data,
                links: links
            }
        }, 200,)

    } catch (e: any) {
        throw new HTTPException(
            e.statusCode, { message: e.message }
        )
    }
})


app.get('/links/:id', async (c) => {
    try {
        const cache = await clientRedis.get(`url-short:${c.req.param('id')}`)

        let links
        const ttl_threshold = 30 // 30 sec

        if (!cache) {
            let query = 'select id, short_url, long_url, created_at, expired_at, is_active, last_visited, total_visited, user_id from urls where 1=1'

            query = query + " and id = " + c.req.param('id')

            links = (await sql.query(query))[0][0]
            if(!links) throw_err('Data not found', 404)

            await clientRedis.set(
                'url-short:'+c.req.param('id'),
                JSON.stringify(links), {
                    EX: 60 * 5 // 5 min
                }
            )
            console.log('cache set')

        } else {
            links = JSON.parse(cache)

            await checkAndRenewCache(clientRedis, ttl_threshold, 'url-short:'+c.req.param('id'), links)

        }

        return c.json({
            errors: false,
            data: {
                links: links
                }   
        }, 200,)
    } catch (e: any) {
        throw new HTTPException(
            e.statusCode, { message: e.message }
        )
    }
})


app.post('/links', async (c) => {
    let connection
    try {
        const data_url = await c.req.json()
        const short_url = randomBytes(5).toString('hex')
        
        connection = await sql.getConnection()
        await connection.beginTransaction()

        const query = `insert into urls (short_url, long_url, created_at, expired_at, is_active, total_visited, user_id) values (?, ?, NOW(), DATE_ADD(NOW(), INTERVAL 7 DAY), 1, 0, NULL)`

        const [insertResult] = await connection.query(query, [short_url, data_url.long_url])
        const newId = insertResult.insertId

        const [rows] = await connection.query('select * from urls where id = ?', [newId])

        await connection.commit()
        connection.release()

        // * set cache for new data inserted
        await clientRedis.set(
            'url-short:'+newId,
            JSON.stringify(rows[0]), {
                EX: 60 * 5 // 5 min
            }
        )
        console.log('cache insert set')

        await cacheInvalidation() // cache invalidation for paging cache

        return c.json({
            errors: false,
            message: 'Data inserted successfully'
        }, 200)

    } catch (e: any) {
        if(connection) {
            await connection.rollback()
            connection.release()
        }
        
        throw new HTTPException(
            e.statusCode, { message: e.message }
        )
    }
})

// * ----- ----- ----- REDIS LOCK IMPLEMENTS ----- ----- ----- ----- 
app.get('/links/:id/locks', async (c) => {
    const cacheKey = 'url-short:'+c.req.param('id')
    const MAX_RETRIES = 10
    const RETRY_DELAY = 100 // ms
    const TTL_THRESHOLD = 30 // 30 sec

    let lockAcquired
    try {
        const cache = await clientRedis.get(`url-short:${c.req.param('id')}`)

        let links

        if (!cache) {
            lockAcquired = await acquireLockRedisWithRetry(cacheKey, 60, MAX_RETRIES, RETRY_DELAY)

            if(!lockAcquired) throw_err('Resource is locked, try again later', 423)
            
            let query = 'select id, short_url, long_url, created_at, expired_at, is_active, last_visited, total_visited, user_id from urls where 1=1'

            query = query + " and id = " + c.req.param('id')

            links = (await sql.query(query))[0][0]
            if(!links) throw_err('Data not found', 404)

            await clientRedis.set(
                cacheKey,
                JSON.stringify(links), {
                    EX: 60 * 5 // 5 min
                }
            )
            console.log('cache set')

        } else {
            links = JSON.parse(cache)

            await checkAndRenewCache(clientRedis, TTL_THRESHOLD, cacheKey, links)
        }

        return c.json({
            errors: false,
            data: {
                links: links
                }   
        }, 200,)

    } catch (e: any) {
        throw new HTTPException(
            e.statusCode, { message: e.message }
        )
    } finally {
        if(lockAcquired) await releaseLockRedis(cacheKey)
    }
})



app.patch('/links/:id/locks', async (c) => {
    let connection
    const cacheKey = 'url-short:'+c.req.param('id')
    try {
        const new_data = await c.req.json()

        connection = await sql.getConnection()
        await connection.beginTransaction()

        const lockAcquired = await acquireLockRedis(cacheKey, 20)
        if(!lockAcquired) throw_err('Resource is locked, try again later', 423)

        const [check_url]= (await connection.query('select id, short_url, long_url, created_at, expired_at, is_active, last_visited, total_visited, user_id from urls where id = ?', [c.req.param('id')]))[0]

        if(!check_url) throw_err('Data not found', 404)

        const query = 'update urls set long_url = ?, expired_at = DATE_ADD(NOW(), INTERVAL 7 DAY) where id = ?'
        await connection.query(query, [new_data.long_url, c.req.param('id')])

        await connection.commit()

        check_url.long_url = new_data.long_url

        await clientRedis.set(
            cacheKey,
            JSON.stringify(check_url), {
                EX: 60 * 5 // 5 min
            }
        )
        console.log('cache edit set')
        
        await cacheInvalidation() // cache invalidation for paging cache

        return c.json({
            errors: false,
            message: 'Data updated successfully'
        }, 200)

    } catch (e: any) {
        if(connection) await connection.rollback()
        
        throw new HTTPException(
            e.statusCode, { message: e.message }
        )
    } finally {
        connection.release()

        await releaseLockRedis(cacheKey)
    }
})



app.patch('/links/:id/locks2', async (c) => {
    let connection, lock
    try {
        const new_data = await c.req.json()
        const cacheKey = 'url-short:'+c.req.param('id') 

        connection = await sql.getConnection()
        await connection.beginTransaction()

        // * lock the cache
        lock = await acquireLock(cacheKey, 5000)

        const [check_url]= (await connection.query('select id, short_url, long_url, created_at, expired_at, is_active, last_visited, total_visited, user_id from urls where id = ?', [c.req.param('id')]))[0]

        if(!check_url) throw_err('Data not found', 404)

        const query = 'update urls set long_url = ?, expired_at = DATE_ADD(NOW(), INTERVAL 7 DAY) where id = ?'
        await connection.query(query, [new_data.long_url, c.req.param('id')])

        await connection.commit()

        check_url.long_url = new_data.long_url

        await clientRedis.set(
            cacheKey,
            JSON.stringify(check_url), {
                EX: 60 * 5 // 5 min
            }
        )
        console.log('cache edit set')
        
        await cacheInvalidation() // cache invalidation for paging cache

        return c.json({
            errors: false,
            message: 'Data updated successfully'
        }, 200)

    } catch (e: any) {
        if(connection) await connection.rollback()
        
        throw new HTTPException(
            e.statusCode, { message: e.message }
        )

    } finally {

        connection.release()

        if(lock) await releaseLock(lock)
    }
})
// * ----- ----- ----- ----- ----- ----- ----- 


app.patch('/links/:id', async (c) => {
    let connection
    try {
        const new_data = await c.req.json()

        connection = await sql.getConnection()
        await connection.beginTransaction()

        const [check_url]= (await connection.query('select id, short_url, long_url, created_at, expired_at, is_active, last_visited, total_visited, user_id from urls where id = ?', [c.req.param('id')]))[0]

        if(!check_url) throw_err('Data not found', 404)

        const query = 'update urls set long_url = ?, expired_at = DATE_ADD(NOW(), INTERVAL 7 DAY) where id = ?'
        await connection.query(query, [new_data.long_url, c.req.param('id')])

        await connection.commit()
        connection.release()

        check_url.long_url = new_data.long_url

        await clientRedis.set(
            'url-short:'+c.req.param('id'),
            JSON.stringify(check_url), {
                EX: 60 * 5 // 5 min
            }
        )
        console.log('cache edit set')
        
        await cacheInvalidation() // cache invalidation for paging cache

        return c.json({
            errors: false,
            message: 'Data updated successfully'
        }, 200)

    } catch (e: any) {
        if(connection) {
            await connection.rollback()
            connection.release()
        }
        throw new HTTPException(
            e.statusCode, { message: e.message }
        )
    }
})


app.delete('/links/:id', async (c) => {
    let connection
    try {
        connection = await sql.getConnection()
        await connection.beginTransaction()

        const [check_url]= (await connection.query('select id from urls where id = ?', [c.req.param('id')]))[0]

        if(!check_url) throw_err('Data not found', 404)

        await connection.query('delete from urls where id = ?', [c.req.param('id')])

        // * check cache if exist delete it
        const cache = await clientRedis.get(`url-short:${c.req.param('id')}`)
        if(cache) {
            await clientRedis.del(`url-short:${c.req.param('id')}`)
            console.log('cache deleted')
        }

        await cacheInvalidation() // cache invalidation for paging cache

        await connection.commit()
        connection.release()

        return c.json({
            errors: false,
            message: 'Data delete successfully'
        }, 200)

    } catch (e: any) {
        if(connection) {
            await connection.rollback()
            connection.release()
        }
        throw new HTTPException(
            e.statusCode, { message: e.message }
        )
    }
})


// * GLOBAL ERROR HANDLING 
app.notFound((c) => {
    c.status(404)
    return c.json({
        errors: true,
        message: "Endpoint not found"
    })
}) 


app.onError((err, c) => {
    console.error(`${err}`)
    c.status = c.status || 500
    err.message = err.message || 'Internal Server Error'
    return c.json({
        errors: true,
        message: err.message
    })
})


checkConn()
export default app