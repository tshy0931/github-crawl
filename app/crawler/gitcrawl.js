//-------------------------------------
// This module defines the GitHub API crawler.
// Supporting functionalities:
// 1. get a range of user/repo
// 2. for each user/repo in the list, call GitHub API to get details.
//    get relations and store it as embeded array in the user/repo.
// 3. persist the user/repo information to Kafka or directly to MongoDB
// 4. Use optional check to update existing user/repo
// 5. Pause crawling when reached rate limit

'use strict';

const https = require('https');
const EventEmitter = require('events');
const util = require('util');
const consts = require('../constants/gc-consts');
const qs = require('querystring');
const producer = require('../kafka/gcKafkaProducer');
const consumer = require('../kafka/gcKafkaConsumer');
const mongo = require('../mongodb/gcMongoDB');
const model = require('../models/models');

var self, startId, endId, clientId, clientSecret, userAgent, debug;
const links2Crawl = {
	user: ['followers','following','starred','subscriptions','organizations','reposOfUser'],
	repo: ['forks','collaborators','assignees','languages','stargazers','contributors','subscribers']
};

function GitCrawler() {
	EventEmitter.call(this);
	self = this;
}
util.inherits(GitCrawler, EventEmitter);

var gc = new GitCrawler();

gc.debugMode = () => {
	debug = true;
	gc.waitUntilRateLimitReset = (resetTime, now) => {
		return Math.max(0,parseInt(resetTime,10)*1000 + 1000 - now);
	};
	gc.setEndId = (id) => {
		endId = id;
	}
}

gc.getItems = (type, since, perPage) => {
	if(!type){
		throw new Error(util.format('type is undefined; type=%s', type));
	}
	isEndIdReached(since) || self.emit('get-items', type, since, perPage || 100);
};
gc.getUsers = gc.getItems.bind(self, 'user');
gc.getRepos = gc.getItems.bind(self, 'repo');

gc.getItem = (type, id) => {
	if(!type || !id){
		throw new Error(util.format('type or id not defined: type=%s, id=%s', type, id));
	}
	id<=endId && self.emit('get-item', type, id);
};
gc.getUser = gc.getItem.bind(self, 'user');
gc.getRepo = gc.getItem.bind(self, 'repo');

gc.getLinks = (linkType, itemType, page, perPage, id) => {
	if(!linkType || !itemType || !page || !perPage || !id){
		throw new Error(util.format('invalid input: %s, %s, %s, %s, %s', arguments));
	}
	id<=endId && self.emit('get-links', linkType, itemType, page, perPage, id);
};

// start to crawl.
// ops: object containing arguments:
// type: time type to crawl, user or repo.
// startId: the id of user/repo to start crawling from, excl.
// endId: the id of user/repo to end crawling, incl.
// perPage: items per page
// userAgent: value for header User-Agent
gc.crawl = function(ops){
	if(!ops){
		throw new Error('Missing input argument.');
	}
	if(!ops.clientId || !ops.clientSecret || !ops.userAgent){
		throw new Error('Missing client credentials.');
	}
	if(!ops.type){
		throw new Error('Missing type to crawl, use "user" or "repo"');
	}
	if(!ops.userAgent){
		throw new Error('Missing userAgent');
	}
	startId = ops.startId || 0;
	endId = ops.endId || 5000;
	clientId = ops.clientId;
	clientSecret = ops.clientSecret;
	userAgent = ops.userAgent,
	debug = ops.debug || false

	Promise.all([producer.start(), consumer.start()])
	.then(values => {
		for(let msg of values){
			console.log(msg);
		}
		gc.getItems(ops.type, ops.startId, ops.perPage);
	})
	.catch(err => {
		throw new Error(err);
	});
};

// args:
//     evtName: event name
//     res: respobse object.
//     resBody: full response body.
//     evtArgs: object describing the event in which this function is called.
//          Will pass this to rate-limit event for retry.
// return: Boolean true if rate limit reached, otherwise false
gc.isRateLimit = (res, resBody) => {
	return (403 == res.statusCode &&
		/403 Forbidden/i.test(res.headers.status) &&
		0 == res.headers['x-ratelimit-remaining'] &&
		/API rate limit exceeded for/i.test(resBody)
	);
};

//-------------------------------------
// Helper functions

// call Github API and use callback to process received data
// api: api path
// params: object containing query parameters
const get = (args, cb) => {
	args.params.client_id = clientId;
	args.params.client_secret = clientSecret;
	let options = {
		hostname: consts.githubHost,
		port: consts.githubPort,
		path: util.format('%s?%s', args.api, qs.stringify(args.params)
		),
		method: 'GET',
		headers: {
			'User-Agent':userAgent || 'tshy0931'
		}
	};
	let req;
	try {
		req = https.request(options, function(res){
			cb(res);
		});
		req.on('error', (e) => {
			process.stdout.write(JSON.stringify(e));
		})
	} catch (e) {
		console.error(options);
		console.error(e);
		throw new Error(e);
	} finally {
		req && req.end();
	}
};

// Return API for given type of link
// linkType: the type of link
const getAPIForType = (linkType) => {
	return consts.githubAPIs[linkType];
};

const isEndIdReached = (id) => {
	if(id >= endId){
		setTimeout(()=>{
			self.emit('crawl-done');
		}, 10000);
		return true;
	}
	return false;
}

// execute middleware functions
// preproc: array of {fn: function, args: arguments} objects
const middlewareHandler = (preproc, ...addons) => {
	preproc = preproc || [];
	if(addons){
		preproc.concat(addons);
	}
	for (let proc of preproc) {
		if(typeof proc.fn !== 'function'){
			throw new Error('preproc only accept [function, arguments], but getting: '+JSON.stringify(proc));
		}
		try {
			proc.fn.apply(null, proc.args);
		} catch (e) {
			console.log(util.format('Error executing function %s with args %s',proc.fn.name, proc.args.toString()));
			throw e;
		}
	}
};

// pause the caller thread until rate limit reset time.
// args:
//     resetTime: Epoch time in seconds when rate limit will be resetTime
//     cb: callback to trigger when reaching reset time.
//     cbArgs: arguments to pass into callback cb.
const waitUntilRateLimitReset = (resetTime, cb) => {
	var time2Wait = Math.max(0,parseInt(resetTime,10)*1000 + 1000 - new Date().getTime());
	console.log(util.format('%s Hitting rate limit, resume crawling after %s millisceonds.',cb.name,time2Wait));
	setTimeout(cb, time2Wait);
};

//-------------------------------------
// Events

// Get a range of users/repos,
// recursively emit itself with setImmediate(),
// in order to process next range of usrs/repos
// after current range is all done.
// Stop recursive emit when since >= endId
// args:
//     type: user or repo
//     since: start id (excl.) of range to get
//     perPage: how many items to contain in the result. Default to the maximum, 100.
gc.on('get-items', function onGetItems(type, since, perPage){
	if(!type){
		throw new Error("{type} undefined, set to either user or repo.");
	}
	if(since === undefined || since < 0){
		throw new Error("{since} not given or negative.");
	}
	var nextSince, route;
	switch(type){
		case 'user':
			route = consts.githubAPIs.users;
			break;
		case 'repo':
			route = consts.githubAPIs.repos;
			break;
	}
	let args4Get = {
		api: type === 'user' ? consts.githubAPIs.users : consts.githubAPIs.repos,
		params:{
			since: since || 0,
			per_page: perPage || 100
		}
	};
	get(args4Get, (res) => {
		var content = '';
		res.on('data', function(data){
			content += data.toString('utf8');
		});
		res.on('end', () => {
			if(gc.isRateLimit(res, content)){
				return waitUntilRateLimitReset(
					res.headers['x-ratelimit-reset'], () => {
						gc.getItems(type, since, perPage);
					}
				);
			}
			let list = JSON.parse(content);
			for(let item of list){
				gc.getItem(type, item.id);
			}
			if (res.headers.link) {
				nextSince = /(?:since=)(\d+)/.exec(res.headers.link)[1];
				setTimeout((next) => {
					gc.getItems(type, next, perPage);
				}, 30000, nextSince);
			}
		});
	});
});

// Get a single user/repo,
// args:
//     type: user or repo
//     id: id of the user or repo
gc.on('get-item', function onGetItem(type, id){
	if(!type || !id){
		throw new Error(util.format('Invalid input: type=%s, id=%s',type,id));
	}
	get({
		api: type === 'user' ?
			util.format(consts.githubAPIs.user, id) :
			util.format(consts.githubAPIs.repo, id),
		params:{}
	}, (res) => {
		var content = '';
		res.on('data', (data) => {
			content += data.toString('utf8');
		});
		res.on('end', () => {
			if(gc.isRateLimit(res, content)){
				return waitUntilRateLimitReset(res.headers['x-ratelimit-reset'], () => {
					gc.getItem(type, id);
				});
			}
			let obj = JSON.parse(content),
				value = model[type](obj, res, id);
			if(value){
				producer.produce(
				producer.topicMap[type],
				model[type](obj, res),
				obj['id'],
				-1
				);
				for (let link of links2Crawl[type]) {
					// gc.getLinks(link, type, 1, 100, id);
				}
			}
		})
	});
});

// Get relations of given type,
// and store as a list in the user/repo.
// args:
//     linkType: type of relation, such as star, follower, following etc.
//     itemType: user or repo
//     page: page number
//     perPage: items per page. default to 100.
//     id: id of the user or repo
gc.on('get-links', function onGetLinks(linkType, itemType, page, perPage, id){
	var linkUrl = getAPIForType(linkType);
	get({
		api: util.format(linkUrl, id),
		params:{
			per_page: perPage,
			page: page
		}
	}, (res) => {
		var content = '';
		res.on('data', (data) => {
			content += data.toString('utf8');
		});
		res.on('end', () => {
			if(gc.isRateLimit(res, content)){
				return waitUntilRateLimitReset(res.headers['x-ratelimit-reset'], () => {
					gc.getLinks(linkType, itemType, page, perPage, id);
				});
			}
			let items = JSON.parse(content);
			var result = [];
			for (let item of items) {
				result.push(item.id);
			}
			producer.produce(
				producer.topicMap[linkType],
				result,
				id,
				-1
			);
			if (res.headers.link) {
 				if( -1 === res.headers.link.search('rel="next"')){
					return;
				}
				setTimeout(() => {
					gc.getLinks(linkType, itemType, page+1, perPage, id);
				}, 60000);
			}
		})
	});
});

gc.on('crawl-done', () => {
	console.log('Crawling done.');
	consumer.close();
});

module.exports = gc;
