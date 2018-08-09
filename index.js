

class Queque {

    constructor() {
        this.pollingInterval = 1000;
    }

    push() {
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

