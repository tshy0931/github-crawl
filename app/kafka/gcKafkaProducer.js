"use strict";

const util = require('util');
const Kafka = require('node-rdkafka');
const EventEmitter = require('events');

var self, producer, pooler, ready=false, activeCount=0;

function GCKafkaProducer() {
	EventEmitter.call(this);
	self = this;
}
util.inherits(GCKafkaProducer, EventEmitter);

var mod = new GCKafkaProducer();

mod.topicMap = {
  user: 'gitcrawl-user',
  repo: 'gitcrawl-repo',
  followers: 'gitcrawl-followers',
  following: 'gitcrawl-following',
  starred: 'gitcrawl-starred',
  subscriptions: 'gitcrawl-subscriptions',
  organizations: 'gitcrawl-organizations',
  reposOfUser: 'gitcrawl-repos',
  forks: 'gitcrawl-forks',
  collaborators: 'gitcrawl-collaborators',
  assignees: 'gitcrawl-assignees',
  languages: 'gitcrawl-languages',
  stargazers: 'gitcrawl-stargazers',
  contributors: 'gitcrawl-contributors',
  subscribers: 'gitcrawl-subscribers',
  issues: 'gitcrawl-issues'
};

// returns a new instance of Kafka producer with given configuration
mod.getNewProducerInstance = (config) => {
  config = config || {
    'client.id': 'gitcrawl.kafka',
    'metadata.broker.list': 'localhost:9092',
    'compression.codec': 'gzip',
    'retry.backoff.ms': 200,
    'message.send.max.retries': 10,
    'socket.keepalive.enable': true,
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.ms': 1000,
    'batch.num.messages': 10000,
    'dr_cb': true
  };

  return new Kafka.Producer(config);
};

// Start the producer, return a promise which resolved when connection is ready,
// otherwise reject if failed to connect.
mod.start = (config) => {
	return new Promise((resolve, reject) => {
		try{
			producer = mod.getNewProducerInstance(config);
		  	producer.on('ready', (args) => {
				ready = true;
				pooler = setInterval(() => {
					producer.poll();
				}, 1000);
				resolve(util.format('Producer %s ready.', args.name));
			});
		    producer.on('delivery-report', (report) => {
				// console.log('Received delivery report: '+JSON.stringify(report));
		    	activeCount--;
				//TODO: if report says failure, retry by emit again
		    });
		    producer.on('disconnected', (args) => {
		    	console.log('producer disconnected: '+JSON.stringify(args));
		    });
		    producer.on('error', (err) => {
		    	console.error(JSON.stringify(err));
		    	console.error(err);
		    });
		    producer.on('event.error', (err) => {
		    	console.error(err);
		    });
		    producer.connect();
		}catch(err){
			reject(err);
		}		
	});
};

mod.produce = (topic, msg, key, partition) => {
	if(!producer){
		throw new Error('Producer not initialized. Call producer.start() before produce()');
	}
	self.emit('produce', topic, msg, key, partition);
};

mod.on('produce', (topic, msg, key, partition) => {
	if(!ready){
		return self.emit('produce', topic, msg, key, partition);
	}
	// console.log(util.format('producing %s:%s for topic %s', key,JSON.stringify(msg),topic));
	activeCount++;
	try {
		var topicObj = producer.Topic(topic, {
			'request.required.acks': 1
		});
		producer.produce(
			topicObj,
			partition ? partition : -1,
			new Buffer(JSON.stringify(msg)),
			key
			// Date.now()
		);
	} catch (e) {
		console.error(util.format('Problem occurred when sending %s:%s to topic %s', key,msg,topic));
		console.error(e);
		console.log('Retrying by emit the event again.');
		self.emit('produce', topic, msg, key, partition);
	}
});

mod.close = () => {
	self.emit('close', 1);
};
mod.on('close', (calledTimes) => {
	if(calledTimes > 100){
		console.error(util.format('Still having %s active producers, not able to close.', activeCount));
	}
	if(activeCount > 0){
		return setTimeout(()=>{
			console.log('Waiting for active producing processes: '+activeCount+' remaining...');
			self.emit('close', calledTimes+1);
		}, 1000);
	}else{
		console.log('All producer done, disconnecting...');
		clearInterval(pooler);
		producer.disconnect();
	}
});

//NOTE:
// 1. getWriteStream create new stream on every call, try cache it per topic.
// 2. not performant as standard API

module.exports = mod;
