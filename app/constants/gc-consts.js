//-------------------------------------
// Constant values

exports.githubHost = 'api.github.com';
exports.githubPort = 443;

var githubAPIs = {
  users : '/users',
  user : '/user/%s',
  followers : '/user/%s/followers',
  following : '/user/%s/following',
  starred : '/user/%s/starred',
  subscriptions : '/user/%s/subscriptions',
  organizations : '/user/%s/orgs',
  reposOfUser : '/user/%s/repos',
  repos : '/repositories',
  repo : '/repositories/%s',
  forks : '/repositories/%s/forks',
  collaborators : '/repositories/%s/collaborators',
  assignees : '/repositories/%s/assignees',
  languages : '/repositories/%s/languages',
  stargazers : '/repositories/%s/stargazers',
  contributors : '/repositories/%s/contributors',
  subscribers : '/repositories/%s/subscribers',
  issues : '/repositories/%s/issues'
};

exports.githubAPIs = githubAPIs;
