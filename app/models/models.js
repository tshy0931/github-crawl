const util = require('util');

module.exports = {
	user: (obj, res, id) => {
		return {
			login: obj['login'],
			id: obj['id'],
			company: obj['company'],
			location: obj['location'],
			hireable: obj['hireable'],
			public_repos: obj['public_repos'],
			public_gists: obj['public_gists'],
			followers: obj['followers'],
			following: obj['following'],
			created_at: obj['created_at'],
			updated_at: res.headers['last-modified'],
			etag: res.headers['etag']
		};
	},
	repo: (obj, res, id) => {
		if(obj.message && /Repository access blocked/i.test(obj.message)){
			console.error(util.format('Error on id=%s, %s',id,JSON.stringify(obj)));
			return null;
		}
		return {
			id: obj['id'],
			name: obj['name'],
			ownername: obj.owner.login,
			ownerid: obj.owner.id,
			orgname: obj.organization ? obj.organization['login'] : null,
			orgid: obj.organization ? obj.organization['id'] : null,
			forks: obj['forks'],
			collaborators: [],
			assignees: [],
			languages: [],
			stargazers: [],
			contributors: [],
			subscribers: [],
			language: obj['language'],
			size: obj['size'],
			starCount: obj['stargazers_count'],
			forkCount: obj['forks_count'],
			issueCount: obj['open_issues_count'],
			networkCount: obj['network_count'],
			suberCount: obj['subscribers_count'],
			created_at: obj['created_at'],
			updated_at: res.headers['last-modified'],
			pushed_at: obj['pushed_at'],
			etag: res.headers['etag']
		};
	}
};