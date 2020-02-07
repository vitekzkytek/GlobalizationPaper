import pandas as pd
import datetime
import sqlite3
from tqdm import tqdm
from DownloadData.Journals_DB import *
from DownloadData.ScopusRequest import *
from IPython.core.debugger import Tracer

def getISSNList():

    data = pd.read_excel('DownloadData/ext_list_April_2018_2017_Metrics.xlsx',sheet_name='Scopus Sources April 2018')

    data = data[data.ISSNCount > 0]

    ### bacha nektera ISSN se zjevne vztahuji k ruznym casopisum! Ty bych asi ve vysledku uplne vyradil, zatim je ale stahuju
    data = data[data['Source Type'] == 'Journal']

    issns = data.PrimaryISSN.unique()

    return list(issns)


def downloadAll(API_KEY):
    doctype = 'ar or re or cp'

    ts = datetime.datetime.now().strftime("%y%m%d_%H%M")

    #issns = list(pd.read_excel('test_issn.xlsx')['ISSN']) ### FOR TESTING - only cca 30 journals
    issns = getISSNList() # FULL SAMPLE

    facets = ['country','language','affil','doctype']
    strFacets = generateFacetsString(facets)

    periods = range(2016,1999,-1)
    journalDB,c = Prepare_JournalDB(ts,[str(x+1) for x in periods],issns)
    for yr in periods:
        endyr = yr+2
        for issn in tqdm(issns,desc=str(yr+1)):
            d = ProcessISSN(issn,doctype,yr,endyr,ts,API_KEY,strFacets)
            count = d['Articles']
            DB_SaveTotalArticles(issn, str(yr+1), count, ts,c)
            if count > 0:
                DB_SaveWholeRequestFacet(issn, 'ArticleCountries', 'countries', [x['name'] for x in d['country']],[x['value'] for x in d['country']], yr+1,
                                         [x['hitCount'] for x in d['country']], ts,c)

                DB_SaveWholeRequestFacet(issn, 'ArticleDocTypes', 'docTypes', [x['name'] for x in d['doctype']],[x['value'] for x in d['doctype']], yr+1,
                                         [x['hitCount'] for x in d['doctype']], ts,c)

                DB_SaveWholeRequestFacet(issn, 'ArticleLanguages', 'languages', [x['name'] for x in d['language']],[x['value'] for x in d['language']], yr+1,
                                         [x['hitCount'] for x in d['language']], ts,c)

                DB_SaveWholeRequestFacet(issn, 'ArticleAffils', 'affils', [x['name'] for x in d['af-id']],[x['value'] for x in d['af-id']], yr+1,
                                         [x['hitCount'] for x in d['af-id']], ts,c)

