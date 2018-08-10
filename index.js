'use strict';

const Aigle = require('aigle');
const redis = require('redis');
Aigle.promisifyAll(redis);

const POLL_SCRIPT = `
local queueKey = KEYS[1]
local baseTime = ARGV[1]

local taskKey = redis.call('ZRANGEBYSCORE', queueKey, '-inf', baseTime, 'LIMIT', 0, 1)[1]
if (taskKey == nil) then
  return taskKey
end

local taskDetail = redis.call('GET', taskKey)

redis.call('DEL', taskKey)
redis.call('ZREM', queueKey, taskKey)
return taskDetail
`;

const PUSH_SCRIPT = `
local queueKey = KEYS[1]
local taskTime = ARGV[1]
local taskKey = KEYS[2]
local taskData = ARGV[2]

redis.call('ZADD', queueKey, taskTime, taskKey)
redis.call('SET', taskKey, taskData)
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
        this.pushScriptSha = null;
        this.redis = redis.createClient(config.redis);
    }

    async runEval(sha, keys, args) {
        return await this.redis.evalshaAsync(sha, keys.length, ...keys, ...args);
    }

    getMainTaskQueueKeyname() {
        return `${this.namespace}:taskQueueMain`;
    }

    getTaskDetailKeyName(taskKey) {
        return `${this.namespace}:task:${taskKey}`;
    }

    async initialize() {
        console.log('loading script');
        const pollSha = await this.redis.scriptAsync('load', POLL_SCRIPT);
        this.pollScriptSha = pollSha;
        console.log(pollSha);
        const pushSha = await this.redis.scriptAsync('load', PUSH_SCRIPT);
        this.pushScriptSha = pushSha;
        console.log(pushSha);
    }

//    async push(jobKey, runAt, data) {
//        const now = Date.now();
//        const expiry = runAt - now + 60 * 60 * 1000;
//        console.log(jobKey);
//        await this.redis.multi()
//            .zadd(this.getMainTaskQueueKeyname(), runAt, jobKey)
//            .set(this.getTaskDetailKeyName(jobKey), JSON.stringify(data), 'PX', expiry)
//            .execAsync();
//    }
    async push(jobKey, runAt, data) {
        const dataStr = JSON.stringify(data);
        await this.runEval(this.pushScriptSha,
            [this.getMainTaskQueueKeyname(), this.getTaskDetailKeyName(jobKey)],
            [runAt, dataStr]
        );
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
        for(let i = 0; i < 50; i++) {
            const task = await this.redis.evalshaAsync(this.pollScriptSha, 1, this.getMainTaskQueueKeyname(), checkTime);

            if (!task) {
                // no more task for now
                return;
            }

            console.log(task);
            this.handleAsync(task);
        }
    }

    async handleAsync(task) {
        try {
            await this.handler(task);
        }
        catch(e) {
        }
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
    await qq.initialize();
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

