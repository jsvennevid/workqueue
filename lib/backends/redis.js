var _ = require('underscore'),
    cluster = require('cluster'),
    redis = require('redis');

var redis_lock = "" +
    "local result = redis.call('set', KEYS[1], ARGV[1], 'nx', 'ex', ARGV[2])\n" +
    "if result then\n" +
    " return 1\n" +
    "else\n" +
    " if redis.call('get', KEYS[1]) == ARGV[1] then\n" +
    "  redis.call('expire', KEYS[1], ARGV[2])\n" +
    "  return 2\n" +
    " else\n" +
    "  return 0\n" +
    " end\n" +
    "end";

var redis_unlock = "" +
    "if redis.call('get', KEYS[1]) == ARGV[1]\n" +
    "then\n" +
    " return redis.call('del', KEYS[1])\n" +
    "else\n" +
    " return 0\n" +
    "end";

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
    this.isMaster = false;
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

        var run = _.bind(function () {
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

                    process.nextTick(_.bind(function () {
                        this.processQueue();
                    }, this));
                } else {
                    this.processQueue();
                }
            }, this)).expire(jobQueue, 20).exec();
        }, this);

        var message = this.queue.shift();
        if ("lock" in message.payload.options) {
            var lockName = this.options.prefix + "locks." + message.payload.options.lock;
            var lockData = this.producerId + '.' + message.payload.index;
            this.producerClient.eval(redis_lock, 1, lockName, lockData, Math.floor(message.lock_timeout * 2 / 1000), _.bind(function (err, response) {
                if (err) {
                    this.queue.unshift(message);
                    return;
                }

                if (response > 0) {
                    run();
                } else {
                    this.frontend.onResponse(message, { error: "locked" });
                    process.nextTick(_.bind(function () {
                        this.processQueue();
                    }, this));
                }
            }, this));
        } else {
            run();
        }
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
                    var complete = _.bind(function () {
                        delete this.pending[response.index];
                        this.frontend.onResponse(message, response);
                        delete message.payload.index;
                    }, this);

                    if ("lock" in message.payload.options) {
                        var lockName = this.options.prefix + "locks." + message.payload.options.lock;
                        var lockData = this.producerId + '.' + message.payload.index;
                        this.producerClient.eval(redis_unlock, 1, lockName, lockData, function (err) {
                            complete();
                        });
                    } else {
                        complete();
                    }
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
    },

    acquireMaster: function (timeout, callback) {
        if (this.producerId < 0) {
            callback(false);
            return;
        }

        var masterLock = this.options.prefix + "master";
        this.producerClient.eval(redis_lock, 1, masterLock, this.producerId, Math.floor(timeout / 1000), _.bind(function (err, response) {
            if (err) {
                callback(false);
                return;
            }

            this.isMaster = response > 0;
            callback(this.isMaster);
        }, this));
    },

    releaseMaster: function () {
        this.isMaster = false;

        if (this.producerId < 0) {
            return;
        }

        var masterLock = this.options.prefix + "master";
        this.producerClient.eval(redis_unlock, 1, masterLock, this.producerId);
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