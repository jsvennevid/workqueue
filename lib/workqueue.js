var local_backend = require('./backends/local'),
    redis_backend = require('./backends/redis'),
    _ = require('underscore'),
    cluster = require('cluster');

function Producer(options) {
    options = _.clone(options) || {};

    _.defaults(options,{
        type: 'local',
        workers: require('os').cpus().length,
        timeout: 10000,
        masterTimeout: 10000,
        redis: {},
        local: {}
    });

    this.options = options;

    this.initialize();
}

_.extend(Producer.prototype, {
    initialize: function () {
        this.index = 0;
        this.debugPort = 0;
        this.debugMode = _.some(process.execArgv, _.bind(function (s) {
            var offset = s.indexOf('--debug');
            if (offset !== -1) {
                var valueOffset = s.indexOf('=', offset);
                if (valueOffset !== -1) {
                    this.debugPort = parseInt(s.slice(valueOffset+1));
                }
                return true;
            }
            return false;
        }, this));

        if (this.debugMode) {
            cluster.setupMaster({
                execArgv: process.execArgv.filter(function (s) {
                    return s.indexOf('--debug') === -1;
                })
            });
        }

        switch (this.options.type) {
            default:case 'local': this.backend = new local_backend.ProducerBackend(this, this.options.local); break;
            case 'redis': this.backend = new redis_backend.ProducerBackend(this, this.options.redis); break;
        }

        for (var i = 0; i < this.options.workers; ++i) {
            this.startWorker();
        }

        this.acquireMaster();
    },

    destroy: function () {
        this.releaseMaster();
    },

    post: function (type, options, callback) {
        var message = {
            payload: {
                type: type,
                options: _.clone(options),
                index: this.index++
            },
            callback: callback
        };

        if ("lock" in message.payload.options) {
            message.lock_timeout = this.options.timeout;
        }

        message.timeout = setTimeout(_.bind(function () {
            this.backend.onTimeout(message.payload.index);
            delete message.payload.timeout;
            this.onResponse(message, {
                error: "timeout",
                index: message.payload.index
            });
        }, this), this.options.timeout);

        this.backend.queueMessage(message);
        return message.payload.index;
    },

    startWorker: function () {
        var worker;
        if (this.debugMode) {
            cluster.settings.execArgv.push('--debug=' + (++ this.debugPort));
            worker = cluster.fork();
            cluster.settings.execArgv.pop();
        } else {
            worker = cluster.fork();
        }

        worker.on('exit', _.bind(function (worker, code, signal) {
            this.startWorker();
        }, this));

        this.backend.setupWorker(worker);
    },

    onResponse: function (message, response) {
        if (message.timeout) {
            clearTimeout(message.timeout);
        }
        message.callback(response.error);
    },

    acquireMaster: function () {
        this.backend.acquireMaster(this.options.masterTimeout * 2, _.bind(function (success) {
            this.masterTimer = setTimeout(_.bind(this.acquireMaster, this), this.options.masterTimeout + Math.floor(Math.random() * this.options.masterTimeout * 0.5));
        }, this));
    },

    releaseMaster: function () {
        clearTimeout(this.masterTimer);
        delete this.masterTimer;

        this.backend.releaseMaster();
    },

    isMaster: function () {
        return this.backend.isMaster;
    }
});

function Consumer(options) {
    options = _.clone(options) || {};

    _.defaults(options,{
        type: 'local',
        redis: {}
    });

    this.methods = {};

    switch (options.type) {
        default:case 'local': this.backend = new local_backend.ConsumerBackend(this); break;
        case 'redis': this.backend = new redis_backend.ConsumerBackend(this, options.redis); break;
    }
}

_.extend(Consumer.prototype, {
    registerMethod: function (type, method) {
        this.methods[type] = method;
    },

    run: function () {
        this.backend.run();
    },

    onPayload: function (payload) {
        var method = this.methods[payload.type];
        if (!method) {
            this.backend.onResponse(payload, "method not found");
            return;
        }

        method(payload.options, _.bind(function (err) {
            this.backend.onResponse(payload, err);
        }, this));
    }
});

exports.Producer = Producer;
exports.Consumer = Consumer;
