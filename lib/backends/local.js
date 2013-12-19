var _ = require('underscore'),
    cluster = require('cluster');

function ProducerBackend(options) {
    options = options || {};

    _.defaults(options, {
        workers: require('os').cpus().length
    });

    this.index = 0;
    this.pending = {};
    this.active = {};
    this.queue = [];
    this.debugMode = _.some(process.execArgv, function (s) {
        return s.indexOf('--debug-brk') !== -1;
    });
    this.debugIndex = 59000; // TODO: grab debug port

    if (this.debugMode) {
        cluster.setupMaster({
            execArgv: process.execArgv.filter(function (s) {
                return s.indexOf('--debug-brk') === -1;
            })
        });
    }

    for (var i = 0; i < options.workers; ++i) {
        this.startWorker();
    }
}

_.extend(ProducerBackend.prototype, {
    post: function (type, options, callback) {
        var message = {
            payload: {
                type: type,
                options: options,
                index: this.index++
            },
            callback: callback
        };

        this.queueMessage(message);
        this.processQueue();

        return message.payload.index;
    },

    setupMessageListener: function (worker) {
        worker.on('message', _.bind(function (message) {
            this.onMessage(worker.id, message);
        }, this));

        worker.on('exit', _.bind(function (worker, code, signal) {
            this.startWorker();
            this.processQueue();
        }, this));
    },

    onMessage: function (id, message) {
        var pending = this.pending[message.index];

        delete this.pending[message.index];
        delete this.active[id];

        clearTimeout(pending.timeout);
        pending.callback(message.error);
        this.processQueue();
    },

    queueMessage: function (message) {
        this.queue.push(message);
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

        message.timestamp = new Date();

        this.pending[message.payload.index] = message;

        message.timeout = setTimeout(_.bind(function () {
            delete this.pending[message.payload.index];
            delete this.active[message.worker];
            message.callback("timeout");

            var worker = cluster.workers[message.worker];
            worker.kill();
        }, this), 10000);
    },

    startWorker: function () {
        var worker;
        if (this.debugMode) {
            cluster.settings.execArgv.push('--debug=' + (this.debugIndex++));
            worker = cluster.fork();
            cluster.settings.execArgv.pop();
        } else {
            worker = cluster.fork();
        }
        this.setupMessageListener(worker);
    }
});

function ConsumerBackend() {
    this.methods = {};
}

_.extend(ConsumerBackend.prototype, {
    registerMethod: function (type, method) {
        this.methods[type] = method;
    },

    run: function () {
        process.on('message', _.bind(this.onMessage, this));
    },

    onMessage: function (message) {
        var method = this.methods[message.type];
        if (!method) {
            process.send({
                error: 'method not found',
                index: message.index
            });
            return;
        }

        method(message.options, function (err) {
            process.send({
                error: err,
                index: message.index
            });
        });
    }
});

exports.ProducerBackend = ProducerBackend;
exports.ConsumerBackend = ConsumerBackend;
