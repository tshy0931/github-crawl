'use strict';

const expect = require('chai').expect;
const gitcrawl = require('../app/crawler/gitcrawl');
gitcrawl.debugMode();

describe('When rate limit has been reached', () => {

	var resBody = '{"message": "API rate limit exceeded for xxx.xxx.xxx.xxx. (But here\'s the good news: Authenticated requests get a higher rate limit. Check out the documentation for more details.)","documentation_url": "https://developer.github.com/v3/#rate-limiting"',
		res = {
			statusCode: 403,
			headers:{
				'status': '403 Forbidden',
				'x-ratelimit-remaining': '0',
				'x-ratelimit-reset': (new Date().getTime() / 1000 + 3600).toString()
			}
		}

	it('should detect the rate limit error in http response', ()=>{
		expect(gitcrawl.isRateLimit(res, resBody)).to.be.true;
	});

	it('should pause crawling until rate limit reset time and then resume', () => {
		let now = new Date().getTime(),
			resetTime = (now + 1800000)/1000,
			time2Wait = gitcrawl.waitUntilRateLimitReset(resetTime.toString(), now);

		expect(time2Wait).to.be.above(1800000);
	});
});

describe('When endId, the last id to crawl, is met', function(){
	it('should emit event "crawl-done", and ')
});