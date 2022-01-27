"""

"""


__author__ = "Ethan Bellmer"
__version__ = "0.1"


from decouple import config
from github import Github


REPO = config('GITHUB_REPO')
BRANCH = config('GITHUB_REPO_BRANCH')


def get_ttn_data():
	SQL = '''
		SELECT 
	'''


def get_all_ttn_data():
	SQL = '''
		SELECT 
	'''


def get_monnit_data():
	SQL = '''
		SELECT 
	'''


def get_all_monnit_data():
	SQL = '''
		SELECT 
	'''


def get_sel_data():
	SQL = '''
		SELECT s.[sensorName]
		, u.[common_name]
		, mu.[mUnitName]
		, r.[readingValue]
		, up.[lastUpdate]
		FROM [dbo].[SEL_READINGS] as r
		JOIN [dbo].[SEL_SENSORS] as s
			ON (r.[sensorGUID] = s.[sensorGUID])
		JOIN [dbo].[SEL_UNITS] as u
			ON r.[unitGUID] = u.[unitGUID]
		JOIN [dbo].[SEL_MEASURE_UNITS] as mu
			ON r.[mUnitGUID] = mu.[mUnitGUID]
		JOIN [dbo].[SEL_UPDATES] as up
			ON r.[readingGUID] = up.[readingGUID]
		/* WHERE s.[sensorName] = ? AND u.[common_name] = ? AND up.[lastUpdate] > ? AND up.[lastUpdate] < ? */
		ORDER BY up.[lastUpdate] DESC 
	'''


def get_all_sek_data():
	SQL = '''
		SELECT 
	'''


def get_novus_data():
	SQL = '''
		SELECT 
	'''


def get_all_novus_data():
	SQL = '''
		SELECT 
	'''


def get_github_instance():
	TOKEN = config('GITHUB_TOKEN')
	
	g = Github(TOKEN)
	repo = g.get_repo(REPO)
	
	return repo


def create_github_file(repo, filepath, message, content):
	repo.create_file(filepath, message, content, branch=BRANCH)


def update_github_file(repo, ref, filepath, message, content):
	contents = repo.get_contents("filepath", ref="test")
	repo.update_file(contents.path, "message", "content", contents.sha, branch=BRANCH)



if __name__ == '__main__':
	print('')

	repo_access = get_github_instance()
	create_github_file(repo_access)