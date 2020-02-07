import requests
from time import sleep
from DownloadData.Exceptions import ScopusRequestFailedException,ScopusRequestProcessingException
def generateFacetsString(facets):
    result = ''
    if 'country' in facets:
        result += 'country(count=250)'

    if 'language' in facets:
        if result:
            result += ';'
        result += 'language(count=30)'

    if 'doctype' in facets:
        if result:
            result += ';'
        result += 'doctype(count=30)'

    if 'subjarea' in facets:
        if result:
            result += ';'
        result += 'subjarea(count=30)'

    if 'srctype' in facets:
        if result:
            result += ';'
        result += 'srctype(count=30)'

    if 'pubyear' in facets:
        if result:
            result += ';'
        result += 'pubyear(count=30)'

    if 'affil' in facets:
        if result:
            result += ';'
        result += 'af-id(count=250)'
    return result

def requestScopus(sLink,ts,API_KEY):
    try:
        answer = requests.get(sLink, headers={'Accept':'application/json','X-ELS-APIKey': API_KEY})
    except:
        print('Scopus request {} failed. Sleeping 30s and trying again. '.format(sLink))
        sleep(30)
        answer = requests.get(sLink, headers={'Accept': 'application/json', 'X-ELS-APIKey': API_KEY})
    return answer.json()

def ProcessISSN(issn,doctype,startyr,endyr,ts,API_KEY,strFacets):
    sLink = "http://api.elsevier.com/content/search/scopus?query=ISSN%28{}%29%20AND%20DOCTYPE%28{}%29%20AND%20PUBYEAR%20%3E%20{}%20AND%20PUBYEAR%20%3C%20{}&facets={}&count=1".format(issn, doctype, startyr, endyr, strFacets)
    d = ProcessRequest(sLink,ts,API_KEY,strFacets)

    return d

def ProcessAffilID(AffilID,ts,API_KEY):
    sLink = "http://api.elsevier.com/content/search/scopus?query=ISSN%28{}%29%20AND%20DOCTYPE%28{}%29%20AND%20PUBYEAR%20%3E%20{}%20AND%20PUBYEAR%20%3C%20{}&facets={}&count=1"
    d = ProcessRequest(sLink, ts, API_KEY, '')

    return d


def ProcessRequest(sLink,ts,API_KEY,strFacets):
    try:
        d = ProcessRequestAttempt(sLink,ts,API_KEY,strFacets)
    except:
        print('Processing Scopus request {} failed. Sleeping 15s and trying again. '.format(sLink))
        sleep(15)
        d = ProcessRequest(sLink,ts,API_KEY,strFacets)
    return d


def ProcessRequestAttempt(sLink,ts,API_KEY,strFacets):
    resp = requestScopus(sLink, ts, API_KEY)
    d = {}
    count = int(resp['search-results']['opensearch:totalResults'])
    d['Articles'] = count
    facetresp = resp['search-results']['facet']
    # IF THE ONLY ONE ELEMENT RETURNS DICTIONARY INSTEAD OF LIST
    if type(facetresp) == type({}):
        facetresp = [facetresp]
    for fac in facetresp:
        facName = fac['name']
        if 'category' in fac:
            singleFacet = fac['category']

            if type(singleFacet) == type({}):
                singleFacet = [singleFacet]
            d[facName] = singleFacet
        else:
            d[facName] = []
    return d
