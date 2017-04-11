'use strict';

const util = require('util');
const EventEmitter = require('events');
const mongo = require('mongodb').MongoClient;

var self, database;

const defaultDBUrl = 'mongodb://localhost:27017/github-mining';

function GCMongoDB() {
	EventEmitter.call(this);
	self = this;
}
util.inherits(GCMongoDB, EventEmitter);

var mod = new GCMongoDB();

mod.collections = {
  'gitcrawl-user': 'users',
  'gitcrawl-repo': 'repos',
  'gitcrawl-organizations': 'orgs'
};

mod.connect = (url, cb) => {
  if(!url){
    throw new Error('No MongoDB URL provided.');
  }
  mongo.connect(url, (err, db) => {
    if(!err){
      database = db;
      self.emit('connected', db, url);
    }else{
      console.error('error connecting to mongodb at '+url);
      console.error(err);
      throw new Error(err);
    }
    cb();
  });
};

mod.disconnect = (cb) => {
  database.close(false, cb);
};

mod.createIndex = (collection, options, cb) => {
  let col = database.collection(collection);
  col.createIndex(
    {id:1},
    options,
    (err, indexName) => {
      cb(err, indexName);
    }
  );
};
mod.createUserIndex = mod.createIndex.bind(null,mod.collections['gitcrawl-user'],{unique:true,background:true,w:1});
mod.createRepoIndex = mod.createIndex.bind(null,mod.collections['gitcrawl-repo'],{unique:true,background:true,w:1});

mod.bulkUpdate = (collection, docs, cb) => {
  let col = database.collection(collection);
  // console.log(JSON.stringify(docs));
  col.bulkWrite(
    docs.map((doc)=>{
      return {replaceOne: {
        filter: {id:doc.id},
        replacement: doc,
        upsert:true
      }};
    }),
    {ordered:false, w:1},
    (err, res) => {
      cb(err, res);
    }
  );
};
mod.updateUsers = mod.bulkUpdate.bind(null, mod.collections['gitcrawl-user']);
mod.updateRepos = mod.bulkUpdate.bind(null, mod.collections['gitcrawl-repo']);

mod.updateOne = (collection, doc, cb) => {
  let col = database.collection(collection);
  col.updateOne({id:doc.id}, doc, {upsert:true, w:1}, (err, res) => {
    cb(err, res);
  });
};
mod.updateUser = mod.updateOne.bind(null, mod.collections['gitcrawl-user']);
mod.updateRepo = mod.updateOne.bind(null, mod.collections['gitcrawl-repo']);

mod.inserMany = (collection, docs, cb) => {
  let col = database.collection(collection);
  col.insertMany(docs, (err, res) => {
    cb(err, res);
  });
};

mod.findAll = (collection, cb) => {
  let col = database.collection(collection);
  col.find({}).toArray((err, docs) => {
    cb(err, docs);
  });
};
mod.findAllUsers = mod.findAll.bind(null, mod.collections['gitcrawl-user']);
mod.findAllRepos = mod.findAll.bind(null, mod.collections['gitcrawl-repo']);

mod.updateOne = (collection, query, delta, cb) => {
  let col = database.collection(collection);
  col.updateOne(query, {$set: delta}, (err, res) => {
    cb(err, res);
  });
};

//=========================
// Events
// connected: this indicates that DB connection is successfully established.
mod.on('connected', (db, url) => {
  console.log(util.format('Connected to MongoDB at %s', url));
});


module.exports = mod;
