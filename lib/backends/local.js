var _ = require('underscore'),
    cluster = require('cluster');

function ProducerBackend(frontend) {
    this.frontend = frontend;
    this.pending = {};
    this.active = {};
    this.queue = [];
}

_.extend(ProducerBackend.prototype, {
    setupWorker: function (worker) {
        worker.on('message', _.bind(function (response) {
            var message = this.active[worker.id];
            delete this.active[worker.id];
            if (message) {
                delete this.pending[message.payload.index];
                this.frontend.onResponse(message, response);
            }
            this.processQueue();
        }, this));
        this.processQueue();
    },

    queueMessage: function (message) {
        message.timestamp = new Date();
        this.queue.push(message);
        this.processQueue();
    },

    processQueue: function () {
        if (this.queue.length == 0) {
            return;
        }

        var message = this.queue[0];

        for (var id in cluster.workers) {
            if (!this.active[id]) {
                cluster.workers[id].send(message.payload);

                this.active[id] = message;
                message.worker = id;
                break;
            }
        }

        if (!message.worker) {
            return;
        }

        this.queue.shift();
        this.pending[message.payload.index] = message;
    },

    onTimeout: function (id) {
        var pending = this.pending[id];
        if (pending) {
            delete this.pending[id];
            if (pending.worker) {
                delete this.active[pending.worker];
            }

            var worker = cluster.workers[pending.worker];
            if (worker) {
                worker.kill();
            }
        } else {
            this.queue = _.filter(this.queue, function (message) {
                return message.payload.index != id;
            });
        }
        this.processQueue();
    }
});

function ConsumerBackend(frontend) {
    this.frontend = frontend;
}

_.extend(ConsumerBackend.prototype, {

    run: function () {
        process.on('message', _.bind(function (message) {
            this.frontend.onPayload(message);
        }, this));
    },

    onResponse: function (message, response) {
        process.send({
            error: response,
            index: message.index
        });
    }
});

exports.ProducerBackend = ProducerBackend;
exports.ConsumerBackend = ConsumerBackend;
