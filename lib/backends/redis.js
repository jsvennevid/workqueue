var _ = require('underscore'),
    cluster = require('cluster'),
    redis = require('redis');

function ProducerBackend(frontend, options) {
    options = _.clone(options) || {};

    _.defaults(options, {
        host: "localhost",
        port: 6379,
        prefix: "workqueue:",
    });

    this.frontend = frontend;
    this.options = options;
    this.pending = {};
    this.active = {};
    this.queue = [];
    this.producerId = -1;
    this.choked = false;

    var producerClient = this.producerClient = redis.createClient(options.port, options.host);
    var responseClient = this.responseClient = redis.createClient(options.port, options.host);

    producerClient.on('ready', _.bind(function () {
        if (this.producerId < 0) {
            producerClient.incr(options.prefix + "producers", _.bind(function (err, data) {
                this.producerId = data;
                this.scheduleResponse();
                this.processQueue();
            }, this));
        } else {
            this.processQueue();
        }
    }, this));
    producerClient.on('close', _.bind(function () {
        this.producerId = -1;
    }));
    producerClient.on('error', _.bind(function () {
        this.producerId = -1;
    }, this));
    producerClient.on('drain', _.bind(function () {
        this.choked = false;
        this.processQueue();
    }, this));
    responseClient.on('ready', _.bind(function () {
        this.scheduleResponse();
    }, this));
    responseClient.on('error', _.bind(function () {
    }));
}

_.extend(ProducerBackend.prototype, {
    setupWorker: function (worker) {
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

        if (this.choked) {
            if (this.producerClient.command_queue.length > 0) {
                return;
            }
            this.choked = false;
        }

        if (!this.producerClient.connected || (this.producerId < 0)) {
            return;
        }

        if (this.producerClient.command_queue.length > 200) {
            this.choked = true;
            return;
        }

        var message = this.queue.shift();
        this.pending[message.payload.index] = message;
        message.payload.sender = this.producerId;

        var jobQueue = this.options.prefix + "jobs";
        this.producerClient.multi().rpush(jobQueue, JSON.stringify(message.payload), _.bind(function (err) {
            if (err) {
                if (message.payload.index) {
                    delete this.pending[message.payload.index];
                    this.queue.unshift(message);
                }
                delete message.payload.sender;

            } else {
                this.processQueue();
            }
        }, this)).expire(jobQueue, 20).exec();
    },

    scheduleResponse: function () {
        if (this.producerId < 0) {
            return;
        }

        var responseQueue = this.options.prefix + "response." + this.producerId;
        this.responseClient.blpop(responseQueue, 5, _.bind(function (err, data) {
            if (!err && data) {
                var response = JSON.parse(data[1]);
                var message = this.pending[response.index];
                if (message) {
                    delete this.pending[response.index];
                    this.frontend.onResponse(message, response);
                    delete message.payload.index;
                }
            }

            process.nextTick(_.bind(function () {
                this.scheduleResponse();
            }, this));
        }, this));
    },

    onTimeout: function (id) {
        var pending = this.pending[id];
        if (pending) {
            delete this.pending[id];
            delete pending.payload.index;
        } else {
            this.queue = _.filter(this.queue, function (message) {
                return message.payload.index != id;
            });
        }
        this.processQueue();
    }
});

function ConsumerBackend(frontend, options) {
    options = _.clone(options) || {};

    _.defaults(options, {
        host: "localhost",
        port: 6379,
        prefix: "workqueue:"
    });

    this.frontend = frontend;
    this.options = options;
}

_.extend(ConsumerBackend.prototype, {
    run: function () {
        var redisClient = this.redisClient = redis.createClient(this.options.port, this.options.host);
        redisClient.on('ready', _.bind(function () {
            this.reschedule();
        }, this));
        redisClient.on('error', _.bind(function () {
        }, this));
    },

    reschedule: function () {
        var jobQueue = this.options.prefix + "jobs";
        this.redisClient.blpop(jobQueue, 0, _.bind(function (err, data) {
            if (err || !data) {
                this.reschedule();
            }
            this.frontend.onPayload(JSON.parse(data[1]));
        }, this));
    },

    onResponse: function (message, response) {
        var responseQueue = this.options.prefix + "response." + message.sender;
        this.redisClient.multi().rpush(responseQueue, JSON.stringify({
            error: response,
            index: message.index
        })).expire(responseQueue, 20).exec();
        this.reschedule();
    }
});

exports.ProducerBackend = ProducerBackend;
exports.ConsumerBackend = ConsumerBackend;