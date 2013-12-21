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
        redis: {},
        local: {}
    });

    this.index = 0;
    this.options = options;
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

    switch (options.type) {
        default:case 'local': this.backend = new local_backend.ProducerBackend(this, options.local); break;
        case 'redis': this.backend = new redis_backend.ProducerBackend(this, options.redis); break;
    }

    for (var i = 0; i < options.workers; ++i) {
        this.startWorker();
    }
}

_.extend(Producer.prototype, {
    post: function (type, options, callback) {
        var message = {
            payload: {
                type: type,
                options: options,
                index: this.index++
            },
            callback: callback
        };

        this.backend.queueMessage(message);

        message.timeout = setTimeout(_.bind(function () {
            this.backend.onTimeout(message.payload.index);
            delete message.payload.timeout;
            this.onResponse(message, {
                error: "job timeout",
                index: message.payload.index
            });
        }, this), this.options.timeout);

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
