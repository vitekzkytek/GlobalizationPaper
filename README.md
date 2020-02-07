# Globalization of Science

Evidence from Authors in Academic Journals by Country of Origin
By Vítek Macháček and Martin Srholec

see [http://www.globalizationofscience.com/](http://www.globalizationofscience.com/)


## Repo Contents:

### `/Phase1_DownloadData/`

contains downloading scripts for getting the journal data from the Scopus API

Downloads country distribution, affiliation and language distribution for each journal indexed in the [Scopus Source List](/Phase1_DownloadData/ext_list_April_2018_2017_Metrics.xlsx) in each year between 2005 and 2017.
Stores the output in the SQLite database in folder this folder. Unfortunately, these data cannot be made publicly accessible.

Launch download with `downloadAll()`  method in the `Download.py` file. Necessary to run with Scopus API key with appropriate bottleneck (approx. 0.5M requests per week) needed.


### `/Phase2_CalcGlobalization/`

contains various methods to calculate globalization from data stored in SQLite in files:
`20181218_AllFieldsCountriesMethods_bot_all.csv` contains data for bottom level of disciplines (Scopus subject areas)
`20181218_AllFieldsCountriesMethods_TOP.xlsx` contains data for top level of disciplines (Life Sciences, Physical Sciences, Medical Sciences, Social Sciences)

Detailed methodology is described in the interactive application.

The calculation processes is 
 
 ### `/Phase3_TransformToWeb/`
 Transform data from previous section into a web readable data that can be feeded into the database
 Main method: `processDataForWeb()` in `transform.py`
 
 ### `/Phase4_DeployWeb/`
 Deployment-ready Docker container to launch the app anywhere.
 
 For now the database is feeded from a simple dump and therefore Docker would not reflect changes in the computational part.
 
Run container:

1. Install Docker [see here](https://phoenixnap.com/kb/how-to-install-docker-on-ubuntu-18-04)
2. `sudo apt install docker-compose`
3. ` cd /srv/`
4. `git clone https://github.com/vitekzkytek/GlobalizationScience.git`
2. `cd /srv/GlobalizationScience/InteractiveWeb/`
3. `docker-compose up -d`

### `/Phase5_Paper/`
writing and computing results for the paper