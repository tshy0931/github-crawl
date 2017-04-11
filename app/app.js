var gc = require("./crawler/gitcrawl.js");
var argv = require('minimist')(process.argv.slice(2));

if(argv['help'] || argv['h']){
	console.log('Usage: node app/app.js --type user --start-id 0 --end-id 1000 --client-id *** --client-secret *** --user-agent agentname [--debug]');
	console.log('--type: type of data to crawl, either "user" or "repo"');
	console.log('--start-id: id to start crawling from.');
	console.log('--end-id: id where to stop crawling, incl.');
	console.log('--client-id: github client id.');
	console.log('--client-secret: github client secret.');
	console.log('--user-agent: value for User-Agent header of github APIs.');
	return;
}

gc.crawl({
	type: argv['type'],
	startId: argv['start-id'],
	endId: argv['end-id'],
	clientId: argv['client-id'],
	clientSecret: argv['client-secret'],
	userAgent: argv['user-agent'],
	debug: argv['debug'] || false
});