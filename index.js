'use strict';

const Aigle = require('aigle');
const redis = require('redis');
Aigle.promisifyAll(redis);


const POLL_SCRIPT = `
local task = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)[1]
if (task) then
  redis.call('ZREM', KEYS[1], task)
end
return task
`;

// emit error?
class Queque {

    constructor(config) {
        this.namespace = config.namespace || 'testnamesapce';
        this.handler = config.handler;
        this.pollingInterval = config.pollingInterval || 1000;

        this.logger = config.logger || {
            debug: () => {},
            info: () => {},
            error: () => {},
        };

        this.timer = null;
        this.pollScriptSha = null;
        this.redis = redis.createClient(config.redis);
    }

    getMainTaskQueueKeyname() {
        return `${this.namespace}:taskQueueMain`;
    }

    getTaskDetailKeyName(taskKey) {
        return `${this.namespace}:task:${taskKey}`;
    }

    async initialize() {
        console.log('loading script');
        const resp = await this.redis.scriptAsync('load', POLL_SCRIPT);
        console.log(resp);
        this.pollScriptSha = resp;
    }

    async push(jobKey, runAt, data) {
        const now = Date.now();
        const expiry = runAt - now + 60 * 60 * 1000;
        console.log(jobKey);
        await this.redis.multi()
            .zadd(this.getMainTaskQueueKeyname(), runAt, jobKey)
            .set(this.getTaskDetailKeyName(jobKey), JSON.stringify(data), 'PX', expiry)
            .execAsync();
    }

    start() {
        this.logger.debug(`Start polling for new task`);
        this.timer = setInterval(() => {
            this.poll()
            .catch(console.log);
        }, this.pollingInterval);
    }

    stop() {
        clearInterval(this.timer);
        this.timer = null;
    }

    async poll() {
        const checkTime = Date.now();
        for(let i = 0; i < 100; i++) {
            const taskKey = await this.redis.evalshaAsync(this.pollScriptSha, 1, this.getMainTaskQueueKeyname(), checkTime);

            if (!taskKey) {
                // no more task for now
                return;
            }

            console.log(taskKey);
            this.handle(taskKey);
        }
    }

    handle(taskKey) {
        this.redis.getAsync(this.getTaskDetailKeyName(taskKey))
        .then(info => {
            console.log(info);
            return this.handler(info);
        })
        .catch(e => {
            console.log(e);
        });
    }
}

function getQ() {
    const qq = new Queque({
        pollingInterval: 1000,
        handler: async (job) => {
            console.log(`job came here: ${job}`);
            return;
        },
        redis: {
            host: '127.0.0.1',
            port: '6379',
            db: 0,
            prefix: 'q',
        },
        logger: {
            debug: console.log,
            info: console.log,
            error: console.log,
        },
    });
    return qq;
}

async function main() {
    const qq = getQ();

    await qq.initialize();
    qq.start();
}

async function push() {
    const qq = getQ();
    await qq.push(
        'testkey',
        Date.now() + 5 * 1000,
        {
            test: true,
            time: new Date().toISOString(),
        });
}

if (process.argv.slice(2)[0] === 'push') {
    push().catch(console.log);
}
else {
    main().catch(console.log);
}

