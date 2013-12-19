var local_backend = require('./backends/local'),
    redis_backend = require('./backends/redis'),
    _ = require('underscore');

function Producer(options) {
    options = _.clone(options) || {};

    _.defaults(options,{
        type: 'local',
        redis: {},
        local: {}
    });

    switch (options.type) {
        default:case 'local': this.backend = new local_backend.ProducerBackend(options.local); break;
        case 'redis': this.backend = new redis_backend.ProducerBackend(options.redis); break;
    }
}

_.extend(Producer.prototype, {
    post: function (type, id, options, callback) {
        this.backend.post(type, id, options);
    }
});

function Consumer(options) {
    options = _.clone(options) || {};

    _.defaults(options,{
        type: 'local',
        redis: {}
    });

    switch (options.type) {
        default:case 'local': this.backend = new local_backend.ConsumerBackend(); break;
        case 'redis': this.backend = new redis_backend.ConsumerBackend(options.redis); break;
    }
}

_.extend(Consumer.prototype, {
    registerMethod: function (type, method) {
        this.backend.registerMethod(type, method);
    },

    run: function () {
        this.backend.run();
    }
});

exports.Producer = Producer;
exports.Consumer = Consumer;
