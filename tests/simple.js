var workqueue = require('../lib/workqueue'),
    cluster = require('cluster');

if (cluster.isMaster) {
    var producer = new workqueue.Producer();

    producer.post('job', { test: 'hello' }, function (err) {
        console.log("result: %s", err);
    });
} else if (cluster.isWorker) {
    var consumer = new workqueue.Consumer();

    consumer.registerMethod('job', function (options, callback) {
        console.log("job");
        console.log(options);
        callback(null);
    });

    consumer.registerMethod('job2', function (options, callback) {
        console.log("job2");
        console.log(options);
//        callback(null);
    });

    consumer.run();
}