// npm install coffee-resque
// npm install config
// npm install underscore

var _und = require("underscore");
var config = require('config').config;
var fs = require('fs');

//////////
// JOBS //
//////////

var jobs = [];

function save_jobs() {
  fs.writeFile(config.jobs.filename, JSON.stringify(jobs));
}

function load_jobs() {

  fs.readFile(config.jobs.filename, 'utf8', function (err, data) {
    if (err) {
      console.log('BOOT: -- no saved jobs to read in');
    }
    else {
      console.log('BOOT: -- reading in saved jobs');
      console.log(data);
      jobs = JSON.parse(data);
    }
  });

}

load_jobs();

function add_job(new_job) {
  var dup_job = _und(jobs).find(function (job) { return !job.executed && job.queue == new_job.queue && job.name == new_job.name && _und.isEqual(job.params, new_job.params); });

  if (dup_job == null) {
    jobs.push(new_job);
    save_jobs();
  }
  else if (new_job.scheduled >= dup_job.force_by) {
    dup_job.scheduled = dup_job.force_by;
    save_jobs();
  }
  else {
    dup_job.scheduled = new Date(Math.max(dup_job.scheduled, new_job.scheduled)) * 1;
    save_jobs();
  }
  
}

////////////
// RESQUE //
////////////

var resque = require('coffee-resque').connect(config.resque.connect);
var resque_jobs = {};

resque_jobs[config.resque.job] = function(delay, queue, job, params, callback) { 
    // console.log(queue + ':' + job + ' [' + delay + ']');

    // no params passed so default them to []
    if (callback == null) {
      callback = params;
      params = [];
    }

    var scheduled = new Date();
    scheduled.setSeconds(scheduled.getSeconds() + delay);

    var force_by = new Date();
    force_by.setSeconds(force_by.getSeconds() + config.jobs.force_by);

    add_job({"delay":     delay,
             "scheduled": scheduled * 1,
             "force_by":  force_by * 1,
             "queue":     queue, 
             "name":      job, 
             "params":    params,
             "executed":  false});

    callback(); 
  };

resque_jobs['ResqueLaterTest'] = function(message, callback) {
  console.log(message);
  callback();
}

console.log('BOOT: -- resque connected to redis on namespace ' + config.resque.connect.namespace);
console.log('BOOT: -- setup queue ' + config.resque.queue);

// setup a worker
var resque_worker = resque.worker(config.resque.queue, resque_jobs);
console.log('BOOT: -- resque worker started');

resque_worker.start();

function check_jobs() {
  var now = new Date();
  var jobs_run = 0;

  _und(jobs).each(function (job, index) {
    if (!job.executed && (job.scheduled < now || job.force_by < now)) {
      //console.log('executing job: ' + job.name);
      job.executed = true;
      resque.enqueue.apply(resque, [job.queue, job.name, job.params]);
      jobs_run += 1;
    }
  });

  if (jobs_run > 0) {
    jobs = _und(jobs).reject(function (job) { return job.executed; });
    save_jobs();
  }

  setTimeout(check_jobs, 1000);
}

setTimeout(check_jobs, 1000);

console.log('BOOT: -- jobs will be forced after ' + config.jobs.force_by + ' seconds');