import { Hono } from 'hono'
import { HTTPException } from 'hono/http-exception' 
import { createClient } from 'redis'
import sql from './db/db'
import throw_err from './utils/error-handling'
import {randomBytes} from 'crypto'

const app = new Hono().basePath('/api/v1')
const clientRedis = createClient({
    socket: {
        port: 6379,
        host: 'localhost' 
    }
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

            const ttl = await clientRedis.ttl(cacheKey)

            // * re cache for renewing ttl if ttl less than threshold
            if(ttl < ttl_threshold) {
                await clientRedis.set(
                    cacheKey, 
                    JSON.stringify({links, total_data}), {
                        EX: 60 * 5 // 5 min
                    }
                )
            }
            
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
            
            const ttl = await clientRedis.ttl('url-short:'+c.req.param('id'))

            // * re cache for renewing ttl if ttl less than threshold
            if(ttl < ttl_threshold) {
                await clientRedis.set(
                    'url-short:'+c.req.param('id'),
                    JSON.stringify(links), {
                        EX: 60 * 5 // 5 min 
                    }    
                )
            }

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