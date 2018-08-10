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
        this.pollingInterval = config.pollingInterval || 1000;
        this.namespace = 'namespace';
        this.handler = () => {};

        this.timer = null;
        this.pollScriptSha = null;
        this.redis = redis.createClient(config.redis);
    }

    getMainTaskQueueKeyname() {
        return `${this.namespace}:taskQueueMain`;
    }

    async initialize() {
        console.log('loading script');
        const resp = await this.redis.scriptAsync('load', POLL_SCRIPT);
        console.log(resp);
        this.pollScriptSha = resp;
    }

    push(jobKey, runAt, jsonable) {
    }

    start() {
        console.log(`Start polling for new task`);
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
        for(let = 0; i < 100; i++) {
            const taskKey = await this.redis.evalshaAsync(this.pollScriptSha, 1, this.getMainTaskQueueKeyname(), checkTime);
            console.log(taskKey);

            if (!taskKey) {
                // no more task for now
                return;
            }

            this.handle(taskKey);
        }
    }

    handle() {
        this.redis.getAsync(
    }
}


async function main() {
    const qq = new Queque({ pollingInterval: 1000,
        handler: async (job) => {
            return;
        },
        redis: {
            host: '127.0.0.1',
            port: '6379',
            db: 0,
            prefix: 'q',
        },
    });

    await qq.initialize();
    qq.start();
}

main().catch(console.log);

