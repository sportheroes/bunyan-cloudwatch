
var util = require('util');
var Writable = require('stream').Writable;
var AWS = require('aws-sdk');
var safeJsonStringify = require('safe-json-stringify');

var jsonStringify = safeJsonStringify ? safeJsonStringify : JSON.stringify;

module.exports = createCloudWatchStream;

function createCloudWatchStream(opts) {
  return new CloudWatchStream(opts);
}

var cloudwatch = null;
var queuedLogs = [];
var sequenceToken = null;
var writeQueued = false;

util.inherits(CloudWatchStream, Writable);
function CloudWatchStream(opts) {
  Writable.call(this, {objectMode: true});
  this.logGroupName = opts.logGroupName;
  this.logStreamName = `pid-${process.pid}`;
  this.writeInterval = opts.writeInterval || 0;
  this.onError = opts.onError || null;

  if (opts.AWS) {
    AWS = opts.AWS;
  }

  if (!cloudwatch) {
    cloudwatch = new AWS.CloudWatchLogs(opts.cloudWatchLogsOptions);
  }
}

CloudWatchStream.prototype._write = function _write(record, _enc, cb) {
  queuedLogs.push(record);
  if (!writeQueued) {
    writeQueued = true;
    setTimeout(this._writeLogs.bind(this), this.writeInterval);
  }
  cb();
};

CloudWatchStream.prototype._writeLogs = function _writeLogs() {
  if (sequenceToken === null) {
    return this._getSequenceToken(this._writeLogs.bind(this));
  }
  var log = {
    logGroupName: this.logGroupName,
    logStreamName: this.logStreamName,
    sequenceToken: sequenceToken,
    logEvents: queuedLogs.map(createCWLog)
  };
  queuedLogs = [];
  var obj = this;
  writeLog();

  function writeLog() {
    cloudwatch.putLogEvents(log, function (err, res) {
      if (err) {
        if (err.retryable) return setTimeout(writeLog, obj.writeInterval);
        if (err.code === 'DataAlreadyAcceptedException' || err.code === 'InvalidSequenceTokenException') {
          const token = err.message.split('is: ')[1];
          sequenceToken = token;

          setTimeout(writeLog, 0);
          return;
        }
        return obj._error(err);
      }

      sequenceToken = res.nextSequenceToken;
      if (queuedLogs.length) {
        return setTimeout(obj._writeLogs.bind(obj), obj.writeInterval);
      }
      writeQueued = false;
    });
  }
};

CloudWatchStream.prototype._getSequenceToken = function _getSequenceToken(done, force = false) {
  var params = {
    logGroupName: this.logGroupName,
    logStreamNamePrefix: this.logStreamName
  };
  var obj = this;

  if (!force) {
    // Missing stream;
    createLogStream(cloudwatch, obj.logGroupName, obj.logStreamName, err => {
      if (err && err.name === 'ResourceNotFoundException') {
        // Missing group & stream:
        return createLogGroupAndStream(cloudwatch, obj.logGroupName, obj.logStreamName, done);
      }

      sequenceToken = undefined;

      done();
    });
  }

  cloudwatch.describeLogStreams(params, function (err, data) {
    if (err) {
      if (err.name === 'ResourceNotFoundException') {
        createLogGroupAndStream(cloudwatch, obj.logGroupName, obj.logStreamName, createStreamCb);
        return;
      }
      obj._error(err);
      return;
    }
    if (data.logStreams.length === 0) {
      createLogStream(cloudwatch, obj.logGroupName, obj.logStreamName, createStreamCb);
      return;
    }
    sequenceToken = data.logStreams[0].uploadSequenceToken;

    done();
  });

  function createStreamCb(err) {
    if (err) return obj._error(err);
    // call again to verify stream was created - silently fails sometimes!
    obj._getSequenceToken(done);
  }
};

CloudWatchStream.prototype._error = function _error(err) {
  if (this.onError) return this.onError(err);
};

function createLogGroupAndStream(cloudwatch, logGroupName, logStreamName, cb) {
  cloudwatch.createLogGroup({
    logGroupName: logGroupName
  }, function (err) {
    if (err) return err;
    createLogStream(cloudwatch, logGroupName, logStreamName, cb);
  });
}

function createLogStream(cloudwatch, logGroupName, logStreamName, cb) {
  cloudwatch.createLogStream({
    logGroupName: logGroupName,
    logStreamName: logStreamName
  }, cb);
}

function createCWLog(bunyanLog) {
  var message = {};
  for (var key in bunyanLog) {
    if (key === 'time') continue;
    message[key] = bunyanLog[key];
  }

  var log = {
    message: jsonStringify(message),
    timestamp: new Date(bunyanLog.time).getTime()
  };

  return log;
}
