var _ = require('underscore');

function ProducerBackend(options) {
    options = options || {};

    _.defaults(options, {
        host: "localhost",
        port: 6379,
        prefix: "workqueue"
    });
}

_.extend(ProducerBackend.prototype, {
});

function ConsumerBackend(options) {
    options = options || {};

    _.defaults(options, {
        host: "localhost",
        port: 6379,
        prefix: "workqueue"
    });
}

_.extend(ConsumerBackend.prototype, {
});