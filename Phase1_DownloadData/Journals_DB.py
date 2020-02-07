import sqlite3
import pandas as pd

def Prepare_JournalDB(ts,periods,issns):

    conn = sqlite3.connect('180802_1611_AllJournals_ArReCp_2001_2017.sqlite')
    c = conn.cursor()

    # get complete list of unique ISSNs
    c.execute('''
    INSERT OR IGNORE INTO issns(name)
    VALUES ('{}') 
    '''.format("'),('".join(issns)))

    # get complete list of periods
    c.execute('''
    INSERT OR IGNORE INTO periods(name)
    VALUES ('{}') 
    '''.format("'),('".join(periods)))

    c.execute('''
    INSERT OR IGNORE INTO bundles(name)
    VALUES ('{}')
    '''.format(ts))

    conn.commit()
    return (conn,c)


def DB_InsertOrIgnoreValuesToTable(table,nameColumn,valColumn,names,values):

    query = '''
    INSERT OR IGNORE INTO {} ({}, {})
    VALUES {};
    '''.format(table,nameColumn,valColumn,'{}'.format(','.join(['("{}","{}")'.format(x[0].replace('"',"'"),x[1].replace('"',"'")) for x in zip(names,values)])))

    return query

def DB_FacetInsertClause(issn, FacetMainTbl, FacetListTbl, facetVal, period, ArticleCount, ts):
    query = '''        
        INSERT INTO {} (FacetID, ISSNID, PeriodID, BundleID, Articles) VALUES
          (
              (SELECT ID FROM {} WHERE name = "{}"),
              (SELECT ID FROM issns WHERE name = "{}"),
              (SELECT ID FROM periods WHERE name = "{}"),
              (SELECT ID FROM bundles WHERE name ="{}"),
              ({})
          );
          
        '''.format(FacetMainTbl,FacetListTbl,facetVal,issn,period,ts,ArticleCount)

    return query


def DB_SaveTotalArticles(issn, period, ArticleCount, ts,c):
    query = '''        
        INSERT INTO totalArticles (ISSNID, PeriodID, BundleID, Articles) VALUES
          (
              (SELECT ID FROM issns WHERE name = "{}"),
              (SELECT ID FROM periods WHERE name = "{}"),
              (SELECT ID FROM bundles WHERE name ="{}"),
              ({})
          )

        '''.format(issn, period, ts, ArticleCount)
    c.execute(query)
    return query


def DB_SaveWholeRequestFacet(issn,FacetMainTbl,FacetListTbl,facetNames,facetVals,period,ArticleCounts,ts,c):
    if len(facetVals) > 0:
        query = 'BEGIN  TRANSACTION;'

        query += DB_InsertOrIgnoreValuesToTable(FacetListTbl, 'name' ,'value', facetVals,facetNames)

        for i in range(len(facetVals)):
            query += DB_FacetInsertClause(issn, FacetMainTbl, FacetListTbl, facetVals[i], period, ArticleCounts[i], ts)

        query += 'COMMIT;'
        try:
            c.executescript(query)
        except:
            print('')



