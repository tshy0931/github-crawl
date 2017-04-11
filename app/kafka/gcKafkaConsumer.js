'use strict';

const util = require('util');
const Kafka = require('node-rdkafka');
const EventEmitter = require('events');
const mongo = require('../mongodb/gcMongoDB.js');

var self, consumer, buffer={};
const bufferSize = 100;

function GCKafkaConsumer() {
	EventEmitter.call(this);
	self = this;
}
util.inherits(GCKafkaConsumer, EventEmitter);

var mod = new GCKafkaConsumer();

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
}

// returns a new instance of Kafka producer with given configuration
mod.getNewConsumerInstance = (config) => {
  config = config || {
    'group.id': 'gitcrawl.kafka',
    'metadata.broker.list': 'localhost:9092'
  };

  return new Kafka.KafkaConsumer(config, {});
};

const initMongoDB = (mongoDBUrl) => {
	mongo.connect(mongoDBUrl || 'mongodb://localhost:27017/github-mining', () => {
		mongo.createUserIndex((err, indexName)=>{
			err && console.error(err);
			console.log(util.format('Created index %s on users collection', indexName));
		});
		mongo.createRepoIndex((err, indexName)=>{
			err && console.error(err);
			console.log(util.format('Created index %s on repos collection', indexName));
		});
	});
};

mod.start = (config) => {
  return new Promise((resolve, reject) => {
    try{
      initMongoDB();
      consumer = mod.getNewConsumerInstance();
      consumer.connect();
      consumer.on('ready', (args) => {
        var topicList = [];
        for (let topic in mod.topicMap) {
          topicList.push(mod.topicMap[topic]);
        }
        consumer.subscribe(topicList);
        setInterval(() => {
          consumer.consume(10);
        }, 1000);
        resolve(util.format('Consumer %s ready.', args.name));
      });
      consumer.on('data', (data) => {
        var doc = JSON.parse(data.value.toString('utf8').trim());
        if(buffer[data.topic]){
          buffer[data.topic].push(doc);
        }else{
          buffer[data.topic] = [doc];
        }
        if(buffer[data.topic].length === bufferSize){
          mongo.bulkUpdate(mongo.collections[data.topic], buffer[data.topic], (err, res) => {
            err && console.error(err);
            console.log(JSON.stringify({
              ok: res.ok,
              writeErrors: res.writeErrors,
              writeConcernErrors:res.writeConcernErrors,
              insertedIds:res.insertedIds,
              nInserted:res.nInserted,
              nUpserted:res.nUpserted,
              nMatched:res.nMatched,
              nModified:res.nModified,
              nRemoved:res.nRemoved
            }));
          });
          buffer[data.topic] = [];
        }
        // {
        //   value: new Buffer('hi'), // message contents as a Buffer
        //   size: 2, // size of the message, in bytes
        //   topic: 'librdtesting-01', // topic the message comes from
        //   offset: 1337, // offset the message was read from
        //   partition: 1, // partition the message was on
        //   key: 'someKey' // key of the message if present
        // }

        // console.log("-----------\n"+JSON.stringify({
        //   topic: data.topic,
        //   key: data.key,
        //   value: data.value.toString(),
        //   offset: data.offset,
        //   size: data.size
        // }));
      });
      consumer.on('error', (err) => {
        console.error(JSON.stringify(err));
      });
      consumer.on('disconnected', () => {
        console.log('Kafka consumer disconnected.');
      });
    }catch(err){
      reject(err);
    }
  });
};

mod.close = () => {
	console.log('flushing buffers');
	for (let topic in buffer) {
		var docs = buffer[topic];
		if(docs && docs.length > 0){
			mongo.bulkUpdate(mongo.collections[topic], docs, (err, res) => {
				err && console.error(err);
				console.log(JSON.stringify(res));
			});
			buffer[topic] = [];
		}
	}
	self.emit('close');
}
mod.on('close', () => {
	mongo.disconnect(()=>{
		console.log('MongoDB disconnected.');
	});
  consumer.disconnect();
});

module.exports = mod;
