var workqueue = require('../lib/workqueue'),
    cluster = require('cluster');

if (cluster.isMaster) {
    var producer = new workqueue.Producer({
        type: 'redis'
    });

    producer.post('job', { test: 'hello 1' }, function (err) {
        console.log("result 1: " + err);
    });
} else if (cluster.isWorker) {
    var consumer = new workqueue.Consumer({
        type: 'redis'
    });

    consumer.registerMethod('job', function (options, callback) {
        console.log("job");
        console.log(options);
        callback(null);
    });

    consumer.run();
}
