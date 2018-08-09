

// emit error?
class Queque {

    constructor() {
        this.pollingInterval = 1000;
        this.namespace = 'namespace';
        this.handler = () => {};

        this.timer = null;
        this.redis = Redis.createInstance();
    }

    push(jobKey, runAt, jsonable) {
    }

    start() {
        this.timer = setInterval(() => {
            this.poll();
        }, this.pollingInterval);
    }

    stop() {
        clearInterval(this.timer);
        this.timer = null;
    }

    poll() {
        this.redis.zrange...
    }

    handle() {
    }
}


const qq = new Queque({
    pollingInterval: 1000,
    handler: async (job) => {
        return;
    },
});

qq.push({

});

