import pandas as pd
from sqlalchemy import create_engine
import os
import matplotlib.pyplot as plt
 
def DB_joinJournals(path=None):
    if path is None:
        path = 'sqlite:///../db/180802_1611_AllJournals_ArReCp_2001_2017.sqlite'
    engine = create_engine(path)
    return engine
 
 
 
def getLocalDocumentsForCountries(con,after=2014):
    query = '''
    SELECT
       c.name as Country,
       Sum(A.Articles) AS Documents
    FROM ArticleCountries as A
    INNER JOIN countries c on A.FacetID = c.ID
    INNER JOIN periods p on A.PeriodID = p.ID
    INNER JOIN issns i on A.ISSNID = i.ID
     
    WHERE p.name > {}
    AND i.PublisherCountryID = c.ID
    GROUP BY Country
    '''.format(after)
 
    return pd.read_sql_query(query,index_col='Country',con=con)
 
def getEnglishDocumentsForCountries(con,after=2014):
    query = '''
    SELECT
       c.name as Country,
       Sum(A.Articles) AS Documents
    FROM ArticleCountries as A
    INNER JOIN countries c on A.FacetID = c.ID
    INNER JOIN periods p on A.PeriodID = p.ID
    INNER JOIN issns i on A.ISSNID = i.ID
 
    WHERE p.name > {}
    GROUP BY Country
    '''.format(after)
 
 
def getTotalDocumentsForCountries(con,after=2014):
    query = '''
    SELECT
       c.name as Country,
       Sum(A.Articles) AS Documents
    FROM ArticleCountries as A
    INNER JOIN countries c on A.FacetID = c.ID
    INNER JOIN periods p on A.PeriodID = p.ID
    INNER JOIN issns i on A.ISSNID = i.ID
 
    WHERE p.name > {}
    GROUP BY Country
    '''.format(after)
 
    return pd.read_sql_query(query, index_col='Country',con=con)
 
def GetLocalDocumentsForJournals(con,after=2014):
    query = '''
    SELECT
       i.name as Journal,
       Sum(A.Articles) AS LocalDocuments
    FROM ArticleCountries as A
    INNER JOIN countries c on A.FacetID = c.ID
    INNER JOIN periods p on A.PeriodID = p.ID
    INNER JOIN issns i on A.ISSNID = i.ID
    WHERE p.name > {}
    AND i.PublisherCountryID = c.ID
    GROUP BY Journal
    '''.format(after)
 
    return pd.read_sql_query(query, index_col='Journal',con=con)
 
def GetJournalsLocation(con):
    query = '''
    SELECT
       i.name as Journal,
       c.name as PublisherCountry
    FROM issns as i
    INNER JOIN countries c on i.PublisherCountryID = c.ID
    '''
 
    return pd.read_sql_query(query,index_col='Journal',con=con)
 

def GetTotalDocumentsForJournals(con,after=2014):
    query = '''
    SELECT
       i.name as Journal,
       Sum(A.Articles) AS TotalDocuments
    FROM ArticleCountries as A
    INNER JOIN countries c on A.FacetID = c.ID
    INNER JOIN periods p on A.PeriodID = p.ID
    INNER JOIN issns i on A.ISSNID = i.ID
    WHERE p.name > {}
    GROUP BY Journal
    '''.format(after)
 
    return pd.read_sql_query(query, index_col='Journal',con=con)
#%%
 
def calcDomesticJournalsEuropePlot():
    conn = DB_joinJournals()
    jrns = pd.DataFrame({'locals':GetLocalDocumentsForJournals(conn).LocalDocuments,
                         'totals':GetTotalDocumentsForJournals(conn).TotalDocuments,
                         'locations':GetJournalsLocation(conn).PublisherCountry})

    jrns['share'] = jrns.locals/jrns.totals
    local_jrns = jrns[jrns.share > 1/3]

    cntrs = getTotalDocumentsForCountries(conn)
    cntrs['localDocs'] = local_jrns.groupby('locations').sum()['locals']
    cntrs['localShare'] = (cntrs.localDocs/cntrs.Documents).fillna(0)
    cntrs.drop('Undefined')

    europe = ['Andorra','Austria','Belgium','Albania','Croatia','Cyprus','Czech Republic','Denmark',
             'Estonia','Finland','France','Germany','Greece','Hungary','Iceland','Ireland','Italy','Belarus',
              'Bosnia and Herzegovina','Bulgaria','Latvia','Liechtenstein','Lithuania','Luxembourg','Malta',
            'Monaco','Netherlands','Norway','Poland','Macedonia','Moldova','Portugal','Montenegro','Romania',
            'Russian Federation','Serbia','San Marino','Slovakia','Slovenia','Spain','Ukraine','Sweden',
            'Switzerland','United Kingdom']
    df = cntrs.loc[europe,:].sort_values('localShare')
    df = df[df.Documents > 10000]
    return df
 
# 
#import pandas as pd
#conn = DB_joinJournals()
#query = '''select
#    Sum(ac.Articles) as Articles,
#    p.name as Period,
#    i.name as ISSN
#from ArticleCountries ac
#inner join countries c ON ac.FacetID = c.ID
#inner join periods p ON ac.PeriodID = p.ID
#inner join issns i ON ac.ISSNID = i.ID
#where c.name = 'Russian Federation'
#and i.bot_EconomicsEconometricsFinance = 1
#and (Period = 2016 or Period = 2017)
#group by ISSN, Period
#'''
 
#df =  pd.read_sql_query(query,conn)
#df = df.set_index(['ISSN','Period']).unstack('Period')
#df['both'] = df.sum(axis=1)
#df = df.sort_values('both',ascending=False)