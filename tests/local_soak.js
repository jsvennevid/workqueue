var workqueue = require('../lib/workqueue'),
    cluster = require('cluster');

if (cluster.isMaster) {
    var producer = new workqueue.Producer();
    var queued = 0, executed = 0, errors = 0;
    var schedule = function () {
        console.log("queued: %d, executed: %d, errors: %d", queued, executed, errors);
        console.log("queue length: %d", producer.backend.queue.length);
        var jobs = Math.round(Math.random() * 10000);
        for (var i = 0; i < jobs; ++i) {
            producer.post('job', { test: 'hello ' + queued }, function (err) {
                if (err) {
                    ++ errors;
                }
                ++ executed;
            });
            ++ queued;
        }

        setTimeout(schedule, 1000);
    }

    schedule();
} else if (cluster.isWorker) {
    var consumer = new workqueue.Consumer();

    consumer.registerMethod('job', function (options, callback) {
        callback(null);
    });

    consumer.run();
}
