#%%
BundleID=1
from sqlalchemy import create_engine
import pandas as pd
from tqdm import tqdm
import os
maxOrMin = {
        'euclid':'min',
        'cosine':'max',
        'maxdif':'min',
        'cityblock':'min',
        'GiniSimpson':'max',
        'shareEnglish':'max',
        'weightGini':'min',
        'instTOP3':'min',
        'top3':'min',
        'localShare':'min'
    }

MULTI_DISCIPLINE_LIMIT_BROAD = '''
AND
    i.broadFieldsNum = 1
'''

MULTI_DISCIPLINE_LIMIT_NARROW = '''
AND
    i.narrowFieldsNum = 1
'''

def DB_GetInternationalityData(field,period,fieldDistAllYears,excludeMultiDiscipline,conn=None):
    if conn is None:
        conn = DB_joinJournals()

    def CleanCountriesDF(df):
        df = df.drop('Undefined',axis='columns')

        if 'Yugoslavia' in df.columns:  # Can be a bit misleading, but most of more than 2 000 results are actually coming from Serbian affiliations in Beograd, Nis, Novi Sad etc.
            ### See AFFILCOUNTRY ( yugoslavia )  AND  DOCTYPE ( ar  OR  re  OR  cp )  AND  PUBYEAR  >  2000 in Scopus
            if 'Serbia' in df.columns:
                df.Serbia = df.Serbia + df.Yugoslavia
            else:
                df.loc[:, 'Serbia'] = df.Yugoslavia
            df = df.drop('Yugoslavia', axis='columns')

        if 'Russia' in df.columns:
            if 'Russian Federation' in df.columns:  # only 4 documents in 2004 and 2005
                df['Russian Federation'] = df['Russian Federation'] + df.Russia
            else:
                df.loc[:, 'Russian Federation'] = df.Russia
            df = df.drop('Russia', axis='columns')

        return df
    def CleanCountriesSeries(df):
        df = df.drop('Undefined')

        if 'Yugoslavia' in df.index:  # Can be a bit misleading, but most of more than 2 000 results are actually coming from Serbian affiliations in Beograd, Nis, Novi Sad etc.
            ### See AFFILCOUNTRY ( yugoslavia )  AND  DOCTYPE ( ar  OR  re  OR  cp )  AND  PUBYEAR  >  2000 in Scopus
            if 'Serbia' in df.index:
                df.loc['Serbia'] = df.loc['Serbia'] + df.loc['Yugoslavia']
            else:
                df.loc['Serbia'] = df.loc['Yugoslavia']
            df = df.drop('Yugoslavia')

        if 'Russia' in df.index:
            if 'Russian Federation' in df.index:  # only 4 documents in 2004 and 2005
                df.loc['Russian Federation'] = df.loc['Russian Federation'] + df.loc['Russia']
            else:
                df.loc['Russian Federation'] = df.loc['Russia']
            df = df.drop('Russia')

        return df

    # Get from DB

    if excludeMultiDiscipline == 'broad':
        MULTI_DISCIPLINE_LIMIT = MULTI_DISCIPLINE_LIMIT_BROAD 
    elif excludeMultiDiscipline == 'narrow':
        MULTI_DISCIPLINE_LIMIT = MULTI_DISCIPLINE_LIMIT_NARROW
    else:
        MULTI_DISCIPLINE_LIMIT = '' 
    
    total = DB_GetTotalArticlesOfField(field,period,MULTI_DISCIPLINE_LIMIT,conn)
    countries = DB_GetJournalCountriesOfField(field, period, MULTI_DISCIPLINE_LIMIT,conn)
    eng = DB_GetEnglishDocuments(field,period,MULTI_DISCIPLINE_LIMIT, conn)
    locales = DB_GetLocalDocuments(field, period, MULTI_DISCIPLINE_LIMIT,conn)
    fieldAllYears = DB_GetFieldDistributionAllYears(field,MULTI_DISCIPLINE_LIMIT,conn)

    issns = total.index
    # Remove Undefined
    DefinedTotal = total - countries.Undefined

    # Clean country columns

    DefinedCountries = CleanCountriesDF(countries)
    fieldAllYears = CleanCountriesSeries(fieldAllYears)

    # load precalculated
    AffilSums = pd.read_excel('Sums/AffilSums_N_3.xlsx')
    AffilSums = AffilSums.reindex(DefinedTotal.index).loc[:,period]


    # return clean data
    d = {}
    d['countries'] = DefinedCountries
    d['total'] = DefinedTotal
    d['totalUndefined'] = total
    d['field'] = field
    d['CalcFieldDistAllYears'] = fieldDistAllYears
    d['fieldAllYears'] = fieldAllYears
    d['period'] = period
    d['eng'] = eng.reindex(issns,fill_value=0)
    d['locals'] = locales.reindex(issns,fill_value=0)
    d['AffilSums'] = AffilSums

    return d


def DB_joinJournals(path=None):
    if path is None:
        path = 'sqlite:///../db/180802_1611_AllJournals_ArReCp_2001_2017.sqlite'#.format(os.getcwd())
    engine = create_engine(path)
    return engine

def DB_GetListOfFields(level,conn= None):
    if conn is None:
        DB_joinJournals()
    fields = pd.read_sql_table('fields',conn)

    return list(fields[fields.level == level].DB_Shortcut)



def DB_GetLocalDocuments(field,period,MULTI_DISCIPLINE_LIMIT,conn = None):
    if conn is None:
        conn = DB_joinJournals()

    if field == 'All':
        query = '''
        SELECT
            i.name as ISSN,
            ArticleCountries.Articles as Documents
        FROM ArticleCountries
        INNER JOIN countries ON countries.ID = ArticleCountries.FacetID
        INNER JOIN periods ON ArticleCountries.PeriodID = periods.ID
        INNER JOIN v_issns i on ArticleCountries.ISSNID = i.ID
        WHERE BundleID = {}
        AND
            periods.name = {}
        AND
            i.PublisherCountryID = ArticleCountries.FacetID
        {}
        '''.format(BundleID,period,MULTI_DISCIPLINE_LIMIT)
    else:
        query = '''
        SELECT
            i.name as ISSN,
            ArticleCountries.Articles as Documents
        FROM ArticleCountries
        INNER JOIN countries ON countries.ID = ArticleCountries.FacetID
        INNER JOIN periods ON ArticleCountries.PeriodID = periods.ID
        INNER JOIN v_issns i on ArticleCountries.ISSNID = i.ID
        WHERE BundleID = {}
        AND
            periods.name = {}
        AND
            i.PublisherCountryID = ArticleCountries.FacetID
        AND 
            i.{} = 1
        {}
        '''.format(BundleID,period,field,MULTI_DISCIPLINE_LIMIT)


    locales = pd.read_sql_query(query,conn,index_col='ISSN')
    return locales.Documents

def CalcAndSaveNSums(tbl,n,conn=None):
    if conn is None:
        conn = DB_joinJournals()
    years = range(2001, 2018)
    dfs = []
    for year in years:
        query = '''
            SELECT i.name as ISSN,
                   Articles as Documents

            FROM {} AS T
            INNER JOIN issns i on T.ISSNID = i.ID
            INNER JOIN periods p on T.PeriodID = p.ID
            WHERE BundleID = {}
            AND p.name = {}
        '''.format(tbl,BundleID,year)
        df = pd.read_sql_query(query, conn)
        issns = df.ISSN.unique()
        result = pd.DataFrame()
        for issn in tqdm(issns, str(year)):
            sum = df.loc[df.ISSN == issn].Documents.sort_values(ascending=False).iloc[:n].sum()
            result.loc[issn, year] = sum
        dfs.append(result)

    total = pd.concat(dfs, axis=1)
    total.to_excel('Sums/AffilSums_N_{}.xlsx'.format(n))



def DB_GetJournalCountriesOfField(field, period, MULTI_DISCIPLINE_LIMIT,conn):
    if field == 'All':
        data = pd.read_sql_query('''
            SELECT
                Articles as Documents,
                countries.name AS Country,
                i.name as ISSN
            FROM ArticleCountries
                INNER JOIN countries ON countries.ID = ArticleCountries.FacetID
                INNER JOIN v_issns i ON i.ID = ArticleCountries.ISSNID
                INNER JOIN periods ON periods.ID = ArticleCountries.PeriodID
            WHERE BundleID={}
                AND periods.name = {}
            {}
            ORDER BY ISSN DESC, Documents DESC
       '''.format(BundleID,period,MULTI_DISCIPLINE_LIMIT), conn)
    else:
        data = pd.read_sql_query('''
            SELECT
                Articles as Documents,
                countries.name AS Country,
                i.name as ISSN
            FROM ArticleCountries
                INNER JOIN countries ON countries.ID = ArticleCountries.FacetID
                INNER JOIN v_issns i ON i.ID = ArticleCountries.ISSNID
                INNER JOIN periods ON periods.ID = ArticleCountries.PeriodID
            WHERE BundleID={}
                AND periods.name = {}
                AND i.{} = 1
            {}
            ORDER BY ISSN DESC, Documents DESC
        '''.format(BundleID,period,field,MULTI_DISCIPLINE_LIMIT),conn)
    pivot = data.pivot(index='ISSN',columns='Country',values='Documents')
    return pivot.fillna(0).astype(int)#.div(total,axis=0)

def DB_GetFieldCountries(field, period, conn=None):
    if conn is None:
        conn = DB_joinJournals()

    if field == 'All':
        data = pd.read_sql_query('''
                SELECT
                    Sum(Articles) as Documents,
                    countries.name AS Country
                FROM ArticleCountries
                    INNER JOIN countries ON countries.ID = ArticleCountries.FacetID
                    INNER JOIN v_issns i ON i.ID = ArticleCountries.ISSNID
                    INNER JOIN periods ON periods.ID = ArticleCountries.PeriodID
                WHERE BundleID={}
                AND periods.name = {}
                {}
                GROUP BY Country
                ORDER BY Documents DESC
            '''.format(BundleID,period,MULTI_DISCIPLINE_LIMIT), conn, index_col='Country')
    else:
        data = pd.read_sql_query('''
            SELECT
                Sum(Articles) as Documents,
                countries.name AS Country
            FROM ArticleCountries
                INNER JOIN countries ON countries.ID = ArticleCountries.FacetID
                INNER JOIN v_issns i ON i.ID = ArticleCountries.ISSNID
                INNER JOIN periods ON periods.ID = ArticleCountries.PeriodID
            WHERE BundleID={}
            AND periods.name = {}
            AND i.{} = 1
            {}
            GROUP BY Country
            ORDER BY Documents DESC
        '''.format(BundleID,period,field,MULTI_DISCIPLINE_LIMIT),conn,index_col='Country')
        if 'Undefined' in data.index:
            data.drop('Undefined',axis=0)
    return data.Documents#/total.sum()

def DB_GetEnglishDocuments(field,period,MULTI_DISCIPLINE_LIMIT,conn):
    if field == 'All':
        query = '''
        SELECT
            i.name as ISSN,
            Articles as English
        FROM ArticleLanguages
            INNER JOIN languages ON languages.ID = ArticleLanguages.FacetID
            INNER JOIN v_issns i ON i.ID = ArticleLanguages.ISSNID
            INNER JOIN periods ON periods.ID = ArticleLanguages.PeriodID
        WHERE BundleID={}
            AND periods.name = {}
            AND languages.name = 'English'
        {}
        ORDER BY ISSN DESC

        '''.format(BundleID,period,MULTI_DISCIPLINE_LIMIT)
    else:
        query = '''
        SELECT
            i.name as ISSN,
            Articles as English
        FROM ArticleLanguages
            INNER JOIN languages ON languages.ID = ArticleLanguages.FacetID
            INNER JOIN v_issns i ON i.ID = ArticleLanguages.ISSNID
            INNER JOIN periods ON periods.ID = ArticleLanguages.PeriodID
        WHERE BundleID={}
            AND periods.name = {}
            AND i.{}= 1
            AND languages.name = 'English'
        {}
        ORDER BY ISSN DESC
        '''.format(BundleID,period,field,MULTI_DISCIPLINE_LIMIT)
    return pd.read_sql_query(query,conn,index_col='ISSN').English

def DB_GetTotalArticlesOfField(field,period,MULTI_DISCIPLINE_LIMIT,conn):
    if field == 'All':
        totalArticles = pd.read_sql_query('''
        SELECT Articles as Documents,
            i.name as ISSN
        FROM totalArticles
            INNER JOIN v_issns i ON i.ID = totalArticles.ISSNID
            INNER JOIN periods ON periods.ID = totalArticles.PeriodID
        WHERE
            BundleID = {}
          AND
            periods.name = {}
        {}
        ORDER BY ISSN ASC
        '''.format(BundleID,period,MULTI_DISCIPLINE_LIMIT),conn,index_col='ISSN')
    else:
        totalArticles = pd.read_sql_query('''
        SELECT Articles as Documents,
            i.name as ISSN
        FROM totalArticles
            INNER JOIN v_issns i ON i.ID = totalArticles.ISSNID
            INNER JOIN periods ON periods.ID = totalArticles.PeriodID
        WHERE
            BundleID = {}
          AND
            periods.name = {}
          AND
            i.{} = 1
        {}
        ORDER BY ISSN ASC
        '''.format(BundleID,period,field,MULTI_DISCIPLINE_LIMIT),conn,index_col='ISSN')

    return totalArticles[totalArticles.Documents > 0].Documents



def DB_GetFieldDistributionAllYears(field,MULTI_DISCIPLINE_LIMIT,conn):
    if field == 'All':
        fieldDist = pd.read_sql_query('''
        SELECT c.name as Country, sum(Articles) as Documents
        FROM ArticleCountries
        inner join countries c on ArticleCountries.FacetID = c.ID
        inner join v_issns i on ArticleCountries.ISSNID = i.ID
        where
            BundleID = {} 
        {}
        group by c.name
        '''.format(BundleID,MULTI_DISCIPLINE_LIMIT),conn,index_col='Country')

    else:
        fieldDist = pd.read_sql_query('''
        SELECT c.name as Country, sum(Articles) as Documents
        FROM ArticleCountries
        inner join countries c on ArticleCountries.FacetID = c.ID
        inner join v_issns i on ArticleCountries.ISSNID = i.ID
        where
            BundleID = {}
          and
            i.{} = 1
        {}
        group by c.name
        '''.format(BundleID,field,MULTI_DISCIPLINE_LIMIT),conn,index_col='Country')

    return fieldDist[fieldDist.Documents > 0].Documents

def DB_CreateView_v_issns(db_path):
    import sqlite3

    try:
        sqliteConnection = sqlite3.connect(db_path)
        cursor = sqliteConnection.cursor()
        print("Database created and Successfully Connected to SQLite")

        sqlite_select_Query = "select sqlite_version();"

        query = '''
        	CREATE VIEW v_issns
            as
            select
                *,
                (top_Life + top_Social + top_Physical + top_Health) as broadFieldsNum,
                (bot_General + bot_AgriculturalAndBiological + bot_ArtsHumanities +
                bot_BiochemistryGeneticsMolecularBiology + bot_BusinessManagementAccounting + 
                bot_ChemicalEngineering + bot_Chemistry + bot_ComputerScience + 
                bot_DecisionSciences + bot_EarthPlanetarySciences + bot_EconomicsEconometricsFinance +
                bot_Energy + bot_Engineering + bot_EnvironmentalScience + bot_ImmunologyMicrobiology +
                bot_Materials + bot_Mathematics + bot_Medicine + bot_Neuroscience + bot_Nursing + 
                bot_PharmacologyToxicologyPharmaceutics + bot_PhysicsAstronomy + bot_Psychology + 
                bot_SocialSciences + bot_Veterinary + bot_Dentistry + bot_HealthProfessions) as narrowFieldsNum
            from issns i
        '''
        cursor.execute(query)
        record = cursor.fetchall()
        print("SQLite Database Version is: ", record)
        cursor.close()

    except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("The SQLite connection is closed")
