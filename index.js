

// emit error?
class Queque {

    constructor() {
        this.pollingInterval = 1000;
        this.namespace = 'namespace';
        this.handler = () => {};

        this.timer = null;
    }

    push() {
    }

    start() {
        this.timer = setInterval(() => {
            this.poll();
        }, this.pollingInterval);
    }

    stop() {
    }

    poll() {
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

