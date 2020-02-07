#%%
from InternationalityData import *
#from InternationalityIndex.plotting import *
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from tqdm import tqdm
methods = ['euclid', 'cosine', 'maxdif', 'cityblock', 'GiniSimpson', 'instTOP3', 'top3', 'shareEnglish', 'localShare','weightGini']

def CalculateEverything(path,fields_level,excludeMultiDiscipline=False):

    methods = ['euclid']#,'cosine','maxdif','cityblock','GiniSimpson','instTOP3','top3','shareEnglish','localShare','weightGini']

    periods = range(2001,2018)
    conn = DB_joinJournals('sqlite:///../db/180802_1611_AllJournals_ArReCp_2001_2017.sqlite')
    countries = list(pd.read_sql_query('SELECT name FROM countries',conn).loc[:,'name'])
    countries.remove('Russia')
    countries.remove('Yugoslavia')
    countries.remove('Undefined')
    fields = DB_GetListOfFields(fields_level,conn) # or bottom
    if fields_level == 'TOP':
        fields.append('All')

    #mindex = pd.MultiIndex(levels=[periods,methods, fields,countries], labels=[[], [], [], []],names=['period', 'method', 'field','country'])
    mdx = pd.MultiIndex.from_product([periods,methods,fields,countries],names=['Period','Method','Field','Country'])
    df = pd.DataFrame(index=mdx,columns=['Internationality'])
    idx = pd.IndexSlice

    for method in methods:
        print('starting: {}'.format(method))
        for field in tqdm(fields):
            field2 = field if field is not None else 'All'
            for yr in periods:
                cntrs = CalcAverageInternationalityOfCountries(field,yr,method,True,excludeMultiDiscipline,conn)
                df.loc[idx[yr,method,field2,list(cntrs.index)],'Internationality'] = cntrs.values

    #df = NormalizeInternationality(df)

    df.to_csv(path)
    return df


def NormalizeInternationality(df):
    tbl = df.unstack('Method')
    normed = pd.DataFrame(index=tbl.index,columns=tbl.columns)

    for col in tbl.columns:
        ser = tbl.loc[:,col]
        #if col[1] in ['instTOP3','top3','localShare','weightGini']:
        if maxOrMin[col[1]] == 'min':
            ser = ser * -1
        normed.loc[:,col] = (ser - ser.mean())/ser.std()

    return normed.stack().reorder_levels(['Period', 'Method', 'Field', 'Country'])

def CalcEuclid(d): # Zitt and Bassecoulard 1998
    journalDist = d['countries'].div(d['total'], axis=0)
    if d['CalcFieldDistAllYears']:
        fieldDist = d['fieldAllYears']/d['fieldAllYears'].sum()
    else:
        fieldDist = d['countries'].sum(axis=0).div(d['total'].sum())

    return ((journalDist - fieldDist) ** 2).sum(axis=1,min_count=1) ** (1/2)

def CalcCosine(d):  # Zitt and Bassecoulard 1998
    journalDist = d['countries'].div(d['total'], axis=0)
    if d['CalcFieldDistAllYears']:
        fieldDist = d['fieldAllYears']/d['fieldAllYears'].sum()
    else:
        fieldDist = d['countries'].sum(axis=0).div(d['total'].sum())

    return (journalDist*fieldDist).sum(axis=1,min_count=1)/(
            ((journalDist**2).sum(axis=1,min_count=1)*(fieldDist**2).sum())**(1/2))

def CalcMaxDif(d): # Zitt and Bassecoulard
    journalDist = d['countries'].div(d['total'], axis=0)
    if d['CalcFieldDistAllYears']:
        fieldDist = d['fieldAllYears']/d['fieldAllYears'].sum()
    else:
        fieldDist = d['countries'].sum(axis=0).div(d['total'].sum())

    return (journalDist - fieldDist).max(axis=1)

def CalcCityBlock(d): # Zitt and Bassecoulard
    journalDist = d['countries'].div(d['total'], axis=0)
    if d['CalcFieldDistAllYears']:
        fieldDist = d['fieldAllYears']/d['fieldAllYears'].sum()
    else:
        fieldDist = d['countries'].sum(axis=0).div(d['total'].sum())

    return (journalDist - fieldDist).abs().sum(axis=1,min_count=1)

def CalcWeightedGini(d):
    #df = pd.DataFrame({'journalDist':d['countries'].div(d['total'], axis=0),
     #                  'fieldDist':d['countries'].sum(axis=0).div(d['total'].sum())})
    result = pd.Series(index=d['total'].index)
    if d['CalcFieldDistAllYears']:
        fieldDist = d['fieldAllYears']/d['fieldAllYears'].sum()
    else:
        fieldDist = d['countries'].sum(axis=0).div(d['total'].sum())

    for issn in d['total'].index:
        df = pd.DataFrame({'journalDist':d['countries'].div(d['total'], axis=0).loc[issn, :],'fieldDist':fieldDist}).fillna(0).sort_values('journalDist',ascending=True)
        df = df / df.sum(axis=0)
        n = df.shape[0]
        df['cumWeight'] = df.fieldDist.cumsum()
        df['cumWeightVal'] = (df.journalDist * df.cumWeight).cumsum()
        sum1 = np.sum(df.cumWeightVal[1:].values * df.cumWeight[:n-1].values)
        sum2 = np.sum(df.cumWeightVal[:n-1].values * df.cumWeight[1:].values)
        idx = sum1 - sum2
        result[issn] = idx

    return result


def CalcGiniSimpson(d): # Aman 2016 # Beware full counting
    journalDist = d['countries']#.div(d['total'], axis=0)
    sumJrn = d['countries'].sum(axis=1)

    return 1 - ((journalDist ** 2).div(sumJrn**2,axis=0).sum(axis=1,min_count=1))


def CalcLocalShare(d):
    return d['locals']/d['total']

def CalcTOP3(d):
    result = pd.Series()
    if d['CalcFieldDistAllYears']:
        fieldDist = d['fieldAllYears']/d['fieldAllYears'].sum()
    else:
        fieldDist = d['countries'].sum(axis=0).div(d['total'].sum())

    for issn in d['total'].index:
        topContrs = d['countries'].loc[issn,:].nlargest(3) / d['total'].loc[issn]
        fieldContrs = fieldDist.reindex(topContrs.index)
        result[issn] = topContrs.sum() - fieldContrs.sum()
    return result

def CalcShareOfEnglish(d):
    return d['eng']/d['totalUndefined']

def CalcAffilsTOP3(d):
    return d['AffilSums']/d['total']



def CalcAverageInternationalityOfCountries(field,period,method,fieldDistAllYears,excludeMultiDiscipline,conn=None):
    if conn is None:
        conn = DB_joinJournals()

    d = DB_GetInternationalityData(field,period,fieldDistAllYears,excludeMultiDiscipline,conn)
    d = SubsetJournalsByMinDocuments(d, 30)

    d['method'] = method

    if method == 'localShare':
        unknownPubCountry = ['1696-2737', '1881-8366', '1604-7982', '1735-4331', '0367-5793', '1738-3102', '1790-8140',
                         '1813-8586', '0478-3522', '1732-8705', '2084-3925', '1897-1059']
        d['total'] = d['total'].drop(unknownPubCountry, axis='index', errors='ignore')
        d['countries'] = d['countries'].drop(unknownPubCountry, axis='index', errors='ignore')

    jrnInts = CalcJournalInternationality(d,method)

    result = (d['countries'] / d['countries'].sum()).multiply(jrnInts, axis=0).sum()
    return result

def CalcJournalInternationality(d,method):
    methods = {'euclid': CalcEuclid,
               'cosine': CalcCosine,
               'maxdif': CalcMaxDif,
               'shareEnglish':CalcShareOfEnglish,
               'localShare':CalcLocalShare,
               'instTOP3':CalcAffilsTOP3,
               'top3':CalcTOP3,
               'cityblock':CalcCityBlock,
               'GiniSimpson': CalcGiniSimpson,
               'weightGini':CalcWeightedGini
               }

    return methods[method](d)

def SubsetCountriesByMinDocuments(d,threshold=30):
    pass

def SubsetJournalsByMinDocuments(d,threshold = 30):
    issns = d['total'][d['total'] >= threshold].index

    for key in d:
        if isinstance(d[key],pd.DataFrame) or isinstance(d[key],pd.Series):
            if not key in ['fieldAllYears']:
                d[key] = d[key].reindex(issns)

    #d['countries'] = d['countries'].loc[issns,]
    #d['total'] = d['total'].loc[issns,]
    return d


 #%%
#
#CalculateEverything('20201221_AllFieldsCountriesMethods_bot.csv','bottom')
#CalculateEverything('20200127_AllFieldsCountriesMethods_TOP.csv','TOP')
#
