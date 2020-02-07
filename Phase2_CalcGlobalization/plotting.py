#%%
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from InternationalityIndex.InternationalityData import *
from InternationalityIndex.InternationalityCalculations import SubsetJournalsByMinDocuments,CalcJournalInternationality


idx = pd.IndexSlice
methods = ['euclid', 'GiniSimpson', 'top3', 'localShare','weightGini']
fields1 = DB_GetListOfFields('TOP', DB_joinJournals())
fields2 = DB_GetListOfFields('bottom',DB_joinJournals())# or bottom
fields = fields1 + fields2
fields.append('All')

def readSavedInternationalityData():
    tops = pd.read_excel('20181218_AllFieldsCountriesMethods_TOP.xlsx',index_col=[0,1,2,3]).Internationality
    bottoms = pd.read_excel('20181218_AllFieldsCountriesMethods_bot.xlsx',index_col=[0,1,2,3]).Internationality
    df = pd.concat([tops,bottoms])
    return df

def addCountryAverages(df):
    tbl = df.unstack('Country')
    tbl.loc[:, '_AVERAGE'] = tbl.mean(axis=1)

    return tbl.stack().reorder_levels(['Period', 'Method', 'Field', 'Country'])
def plotFieldsInCountryTime(df,method,country):
    data = df.loc[idx[:, method, :, country]].unstack('Field')
    data.plot(ylim=(-2, 2),title='{} in {}'.format(method,country))
    plt.show()

def plotMethodsCountryFieldsTime(df,field,country):
    data = df.loc[idx[:,:,field,country]].unstack('Method')
    data.plot(ylim=(-2,2),title = '{} in {}'.format(field,country))
    plt.show()

def plotCorrMatrixOfMethods(df):
    corr = df.unstack('Method').corr()
    corr.index = corr.index.droplevel(0)
    corr.columns = corr.columns.droplevel(0)

    sns.heatmap(corr,
                yticklabels=corr.columns.values,
                xticklabels=corr.columns.values,
                vmin=-1,vmax=1)
    plt.yticks(rotation=0)
    plt.xticks(rotation=0.5)
    plt.show()

def createGDF(ser):
    import geopandas as gpd
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    d = {c:c for c in world['name']}
    d['Syria'] = 'Syrian Arab Republic'
    d['Vietnam'] = 'Viet Nam'
    d['W. Sahara'] = 'Western Sahara'
    d['Korea'] = 'South Korea'
    d['Russia'] = 'Russian Federation'
    d['Dem. Rep. Korea'] = 'North Korea'
    d['Lao PDR'] = 'Laos'
    d['Libya'] = 'Libyan Arab Jamahiriya'
    d['Dominican Rep.'] = 'Dominican Republic'
    d['Czech Rep.'] = 'Czech Republic'
    d["Côte d'Ivoire"] = "Cote d\\'Ivoire"
    d['Falkland Is.'] = 'Falklan Islands (Malvinas)'
    d['Eq. Guinea'] = 'Equatorial Guinea'
    d['Central African Rep.'] = 'Central African Republic'
    d['Bosnia and Herz.'] = 'Bosnia and Herzegovina'

    world['name2'] = world['name'].map(d)
    world = world.merge(ser.to_frame(),left_on='name2',right_index=True)
    return world.dropna()

def plotMap(df,field,period,method,vmin=-3,vmax=3,cmap='OrRd'):
    #Create GeoPandas DF
    ser = df.loc[idx[period,method,field]]
    #ser.index = ser.index.droplevel(['Period','Method','Field'])
    ser.drop('_AVERAGE')
    gdf = createGDF(ser)

    # Set matplotlib
    fig,ax = plt.subplots(figsize=(10,6))
    ax.set_aspect('equal')
    from matplotlib import colors, cm
    # norm = colors.Normalize(vmin,vmax)
    # n_cmap = cm.ScalarMappable(norm=norm,cmap=cmap)
    ax.set_axis_off()
    gdf.plot(ax=ax,column=0,cmap=cmap,vmin=vmin,vmax=vmax)
    fig.suptitle('Internationality measured by {} in {} ({})'.format(method,field,period))
    #n_cmap.set_array([])
    #cbar = fig.colorbar(cmap,ax=ax,ticks=[0,.2,.4,.6,.8,1])
    plt.show()



def plotStdsOfFieldsAndMethods(df):
    stds = df.unstack(['Method','Field','Country']).std().unstack('Field').groupby('Method').mean()
    ax = sns.heatmap(stds, annot=True)

    ax.set_title('Average of Standard Deviations for countries')
    plt.yticks(rotation=1)
    plt.show()


def plotMeansOfFieldsAndMethods(df):
    stds = df.unstack(['Method','Field','Country']).mean().unstack('Field').groupby('Method').mean()
    ax = sns.heatmap(stds, annot=True)

    ax.set_title('Average of Means for countries')
    plt.yticks(rotation=1)
    plt.show()


def plotScatterMatrix(df,c = None):
    df = df.unstack('Method')
    df.columns = df.columns.droplevel(None)
    from pandas.plotting import scatter_matrix
    if c is None:
        axs = scatter_matrix(df,alpha=0.1,figsize=(12,12),diagonal='kde')
    else:
        axs = scatter_matrix(df, alpha=0.1, figsize=(12, 12), diagonal='kde',c=c)

    for i in range(df.columns.shape[0]):
        for j in range(df.columns.shape[0]):
            axs[i,j].set_xlim(-3,3)
            axs[i, j].set_ylim(-3, 3)


    plt.yticks(rotation=0)
    plt.xticks(rotation=1)

    plt.show()

def plotMainHeatMap(df,period,method):
    heat = df.loc[idx[period, method, :, :]].unstack('Field').reset_index(['Period', 'Method']).drop(['Period', 'Method'],axis=1)
    heat.columns = heat.columns.droplevel(0)

    from InternationalityIndex.InternationalityData import DB_GetFieldCountries
    inclCntrs = DB_GetFieldCountries('All',2017).iloc[:100].index
    heat = heat.loc[inclCntrs, :].sort_values('All',ascending=False)

    plt.figure(figsize=(10,20))
    ax = sns.heatmap(heat,annot=True)

    ax.set_title('Internationality ({}) in {}'.format(method,period))
    plt.yticks(rotation=1)
    plt.show()

def plotJournalDistsCountries(field, period,method,fieldDistAllYears,savePNG=False,NoCountries=100,quantiles=4,sortBy=2):
    d = DB_GetInternationalityData(field, period,fieldDistAllYears, DB_joinJournals())
    d = SubsetJournalsByMinDocuments(d, 30)


    qu = pd.qcut(CalcJournalInternationality(d, method), quantiles, labels=False)

    if maxOrMin[method] == 'min':
        qu = quantiles - qu

    df = d['countries']
    df.loc[:, 'qu'] = qu
    df2 = df.groupby('qu').sum() / df.groupby('qu').sum().sum()
    df2.index = ['Q{}'.format(x) for x in range(quantiles, 0, -1)]

    inclCntrs = DB_GetFieldCountries(field, period).iloc[:NoCountries].index
    df2 = df2.reindex(inclCntrs, axis=1)

    df2.loc['Sum', :] = df2.iloc[quantiles - sortBy:, :].sum()
    df2 = df2.sort_values('Sum', axis=1)
    df2 = df2.drop('Sum', axis=0).T
    df2 = df2.reindex(df2.columns.sort_values(), axis=1)

    ax = df2.plot(kind='barh', stacked=True, width=.9, cmap='Blues', figsize=(10, 30))
    ax.legend(loc='upper center', ncol=quantiles, bbox_to_anchor=(0.5, 1.01))
    plt.suptitle('Distribution of articles to journals by Internationality ({}, {}, {})'.format(field,period,method))

    if savePNG:
        plt.savefig('InternationalityIndex/savedFigs/Countries_FullDist_{}_{}_{}.png'.format(method,field,period))
    plt.show()
    return df2



def plotScatterOfJournalMethods(field,period,fieldDistAllYears):
    d = DB_GetInternationalityData(field, period,fieldDistAllYears)
    d = SubsetJournalsByMinDocuments(d)

    result = {}
    for method in methods:
        result[method] = CalcJournalInternationality(d, method)

    df = pd.DataFrame(result)

    from pandas.plotting import scatter_matrix
    import numpy as np

    # Country size dependency - USshare proxy
    axs = scatter_matrix(df.dropna(), alpha=0.1, figsize=(12, 12), diagonal='kde',
                         c=(d['countries'].loc[:, 'United States'] / d['total']).reindex(df.dropna().index),
                         cmap='OrRd')
    plt.show()
    # Journal size dependency
    axs = scatter_matrix(df.dropna(), alpha=0.1, figsize=(12, 12), diagonal='kde',c=np.log((d['total']).reindex(df.dropna().index)),cmap='OrRd')
    plt.show()

    # Cross-collaborations dependency
    axs = scatter_matrix(df.dropna(), alpha=0.1, figsize=(12, 12), diagonal='kde',c=(d['countries'].sum(axis=1)/d['total']).reindex(df.dropna().index),cmap='OrRd')
    plt.show()

#%%
#df = readSavedInternationalityData()

adv = ['Hong Kong','Iceland','Ireland','Israel','Italy','Japan','Liechtenstein','Luxembourg','Monaco','Netherlands','New Zealand',
       'Norway','Portugal','San Marino','Singapore','South Korea','Spain','Sweden','Switzerland','Taiwan','United Kingdom',
       'United States']

tra = ['Albania','Armenia','Croatia','Czech Republic','Estonia','Azerbaijan','Hungary','Belarus','Bosnia and Herzegovina',
       'Bulgaria','Latvia','Lithuania','Georgia','Kazakhstan','Kyrgyzstan','Poland','Macedonia','Moldova','Mongolia','Montenegro',
       'Romania','Russian Federation','Serbia','Slovakia','Slovenia','Tajikistan','Turkmenistan','Ukraine','Uzbekistan']

dists = plotJournalDistsCountries('top_Social',2017,'euclid',True,False,NoCountries=250)
dists['H1'] = dists.Q1 + dists.Q2
dists = dists.sort_values('H1')
df_adv = dists.loc[adv, 'H1']
df_tra = dists.loc[tra, 'H1']

#%%
methods = ['euclid','weightGini','localShare','shareEnglish','top3','GiniSimpson']

for method in methods:
    df.loc[idx[2017, method, 'All', :]].hist(bins=40, range=(0, 1))
    plt.title(method)
    plt.show()
#%%
#df = readSavedInternationalityData()
from tqdm import tqdm
cntrs = pd.read_excel('D:\Dropbox\Hugo\science-internationality-index\main\public\data\populateAmazon.xlsx',sheet_name='country')
adv = cntrs.loc[cntrs.imf2003 == 'Developed Countries'].name
transition = pd.DataFrame(index=fields,columns=methods)
for field in tqdm(['All'] + fields1):
    for method in methods:
        print('Field: {}, Method: {}'.format(field,method))
        dists = plotJournalDistsCountries(field,2017,method,True,False)
        df2 = dists.reindex(list(adv))
        transition.loc[field,method] = (df2.Q1+df2.Q2).mean()

#%%
from tqdm import tqdm
cntrs = pd.read_excel('D:\Dropbox\Hugo\science-internationality-index\main\public\data\populateAmazon.xlsx',sheet_name='country')
adv = cntrs.loc[cntrs.imf2003 == 'Developed Countries'].name
developed= pd.DataFrame(index=fields,columns=methods)
for field in tqdm(['All'] + fields1):
    for method in methods:
        print('Field: {}, Method: {}'.format(field,method))
        dists = plotJournalDistsCountries(field,2017,method,True,False)
        df2 = dists.reindex(list(adv))
        developed.loc[field,method] = (df2.Q1+df2.Q2).mean()


#%%
#
# #%%
# for method in methods:
#     plotMainHeatMap(df,2017,method)
#
# #%%
# method = 'weightGini'
# for field in fields:
#     plotJournalDistsCountries(field,2017,method,True,True)
#
# #%%
# plotMeansOfFieldsAndMethods(df)
# #%%
# plotStdsOfFieldsAndMethods(df)
#
#
# #%%
# for method in methods:
#     plotMap(df,'top_Social',2017,method)
#
# #%%
# for field in fields:
#     #plotMap(normed,'top_Social',2017,method)
#     plotMethodsCountryFieldsTime(df, field, 'CZE')
#
# for method in methods:
#     plotFieldsInCountryTime(df, method, 'CZE')
#
# # %%
# d = DB_GetInternationalityData('All',2017,True)
#
#
#
#
#
#
# #%%
# import wbdata
# import datetime
# import pandas as pd
#
# countries = pd.DataFrame({
#     'iso3code':[i['id'] for i in wbdata.get_country(display=False)],
#     'capital':[i['capitalCity'] for i in wbdata.get_country(display=False)],
#     'incomeLevel':[i['incomeLevel']['value'] for i in wbdata.get_country(display=False)],
#     'region':[i['region']['value'] for i in wbdata.get_country(display=False)],
#     'lendingType':[i['lendingType']['value'] for i in wbdata.get_country(display=False)],
#     'iso2code':[i['iso2Code']for i in wbdata.get_country(display=False)],
#     'longitude':[i['longitude'] for i in wbdata.get_country(display=False)],
#     'latitude':[i['latitude'] for i in wbdata.get_country(display=False)],
#     'pop2017':wbdata.get_data('SP.POP.TOTL',pandas=True,data_date=datetime.date(2017,1,1)),
#     'gdppc2017':wbdata.get_data('NY.GDP.PCAP.PP.CD',pandas=True,data_date=datetime.date(2017,1,1))
# },index=[i['name'] for i in wbdata.get_country(display=False)])
#
# countries.loc[:,'wb_name'] = countries.index
# countries.index = countries.iso3code
#
#
