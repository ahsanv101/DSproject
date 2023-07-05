
from pprint import pprint
from prometheus_client import pushadd_to_gateway
from  tabulate import *
from pandas import concat
from rdflib import Graph
from pandas import read_csv, Series, DataFrame
from rdflib import RDF
from pandas import Series
from pandas import merge
from rdflib import Literal
from json import load
from rdflib import URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get
import pandas as pd
from pandas import concat
from Datamodel import *
from csv import reader
from sqlite3 import connect
from sqlite3 import OperationalError
from pprint import pprint
from pandas import DataFrame
from pandas import Series
from pandas import merge
from json import load
from pandas import read_csv, Series
from pandas import read_sql
import pandas as pd
from urllib.request import pathname2url
import os
from sqlite3 import connect
from pandas import read_sql
import pandas as pd


# Query Processor
class QueryProcessor(object):
    pass

#The general idea of generic query processor is to iterate over all the query proccessors and run the function
# of that very query processor. In case of getMostCitedVenue and getMostCitedPublication. I am return the result of the first
#object that is created since it should return only 1 object.

#For each of the functions. We clean the data and combine the results in 1 dataframe and then make objects.
#For Venues, Person, Organization we just simply extract the informations from the dataframes and create the objects

#For publications we do the same thing but in this case also run an additional fucntion called getCitesdoi of each of the query
#proccessors that returns us Publication object which recursively creates publications objects for cites. For more comments, please take
# a look into getpublicationspublishedinyear function to get more insight. All publications related function work similarly
class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessor = []

    def cleanQueryProcessors(self):
       self.queryProcessor.clear()

    def addQueryProcessor(self, processor):
        self.queryProcessor.append(processor)

    def remove_dotzero(self,s):
        # print(s,type(s))
        if type(s)==int :
            return s.replace(".0", "")
        else:
            return s

    def getPublicationsPublishedInYear(self, year):
        column_names = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume', 'pubchapter',
                        'authorinternalid', 'authorid', 'authorgivenName',
                        'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle', 'pubvenueevent',
                        'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername', 'citesdoi']

        df_empty = pd.DataFrame(columns=column_names)
        pub_obj = []
        for df in self.queryProcessor:
            current = df.getPublicationsPublishedInYear(year)
            current.columns = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume', 'pubchapter',
                               'authorinternalid', 'authorid', 'authorgivenName',
                               'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle',
                               'pubvenueevent',
                               'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername',
                               'citesdoi']
            current["pubissue"] = current["pubissue"].astype("string")
            current["pubvolume"] = current["pubvolume"].astype("string")
            current["pubvenueevent"] = current["pubvenueevent"].astype("string")
            current["pubissue"] = current["pubissue"].apply(self.remove_dotzero)
            current["pubvolume"] = current["pubvolume"].apply(self.remove_dotzero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()

            #extracting authors data
            authors = {}
            for i in df_final:

                if authors.get(i[0], None) == None:
                    authors[i[0]] = [[i[7], i[8], i[9], i[10]]]
                else:
                    if [i[7], i[8], i[9], i[10]] not in authors[i[0]]:
                        authors[i[0]].append([i[7], i[8], i[9], i[10]])

            # extracting venue data
            venue = {}
            for i in df_final:

                if venue.get(i[0], None) == None:
                    venue[i[0]] = [[i[11], i[12], i[13], i[14]]]
                else:
                    if [i[11], i[12], i[13], i[14]] not in venue[i[0]]:
                        venue[i[0]].append([i[11], i[12], i[13], i[14]])

            # extracting publisher data
            publisher = {}
            for i in df_final:

                if publisher.get(i[0], None) == None:
                    publisher[i[0]] = [[i[15], i[16], i[17]]]
                else:
                    if [i[15], i[16], i[17]] not in publisher[i[0]]:
                        publisher[i[0]].append([i[15], i[16], i[17]])

            # extracting cites data
            cites = {}
            for i in df_final:

                if cites.get(i[0], None) == None:
                    cites[i[0]] = [[i[18]]]
                else:
                    if [i[18]] not in cites[i[0]]:
                        cites[i[0]].append([i[18]])

            # extracting publication data
            pubs = {}
            for i in df_final:
                if pubs.get(i[0], None) == None:
                    pubs[i[0]] = [[i[1], i[2], i[3], i[4], i[5], i[6]]]
                else:
                    if [i[1], i[2], i[3], i[4], i[5], i[6]] not in pubs[i[0]]:
                        pubs[i[0]].append([i[1], i[2], i[3], i[4], i[5], i[6]])

            # extracting now relaoding all the data and making the objects. I use pubinternalID as a key for all the
            #dictionaries
            for item in pubs:
                lst_author = []
                lst_ven_id = []
                for a in authors[item]:
                    lst_author.append(Person([a[1]], a[2], a[3]))

                for b in venue[item]:
                    lst_ven_id.append(b[1])


                publish = Organization([publisher[item][0][1]], publisher[item][0][2])

                lst_cit = []
                for c in cites[item]:
                    doi = c[0]
                    s = list()
                    dic = dict()
                    if doi != 'NA':
                        lst_cit.append(df.getCitesDoi(doi, s, dic))

                f_pub = pubs[item][0]

                if (f_pub[3] != 'NA' or f_pub[4] != 'NA' and f_pub[5] == 'NA'):
                    ven = Journal(lst_ven_id, venue[item][0][2], publish)
                    pub_obj.append(
                        JournalArticle([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author, f_pub[3], f_pub[4]))
                elif f_pub[5] != 'NA':
                    ven = Book(lst_ven_id, venue[item][0][2], publish)

                    pub_obj.append(
                        BookChapter([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author, f_pub[5]))
                else:

                    ven = Proceedings(lst_ven_id, venue[item][0][2], publish, venue[item][0][3])
                    pub_obj.append(ProceedingsPaper([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author))

        return pub_obj

    def getPublicationsByAuthorId(self, id):
        column_names = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume', 'pubchapter',
                        'authorinternalid', 'authorid', 'authorgivenName',
                        'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle', 'pubvenueevent',
                        'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername', 'citesdoi']

        df_empty = pd.DataFrame(columns=column_names)
        pub_obj = []
        for df in self.queryProcessor:
            current = df.getPublicationsByAuthorId(id)
            current.columns = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume', 'pubchapter',
                               'authorinternalid', 'authorid', 'authorgivenName',
                               'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle',
                               'pubvenueevent',
                               'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername',
                               'citesdoi']
            # print(current.dtypes)
            current["pubissue"] = current["pubissue"].astype("string")
            current["pubvolume"] = current["pubvolume"].astype("string")
            current["pubvenueevent"] = current["pubvenueevent"].astype("string")
            current["pubissue"] = current["pubissue"].apply(self.remove_dotzero)
            current["pubvolume"] = current["pubvolume"].apply(self.remove_dotzero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()
            authors = {}
            for i in df_final:

                if authors.get(i[0], None) == None:
                    authors[i[0]] = [[i[7], i[8], i[9], i[10]]]
                else:
                    if [i[7], i[8], i[9], i[10]] not in authors[i[0]]:
                        authors[i[0]].append([i[7], i[8], i[9], i[10]])

            venue = {}
            for i in df_final:

                if venue.get(i[0], None) == None:
                    venue[i[0]] = [[i[11], i[12], i[13], i[14]]]
                else:
                    if [i[11], i[12], i[13], i[14]] not in venue[i[0]]:
                        venue[i[0]].append([i[11], i[12], i[13], i[14]])

            publisher = {}
            for i in df_final:

                if publisher.get(i[0], None) == None:
                    publisher[i[0]] = [[i[15], i[16], i[17]]]
                else:
                    if [i[15], i[16], i[17]] not in publisher[i[0]]:
                        publisher[i[0]].append([i[15], i[16], i[17]])

            cites = {}
            for i in df_final:

                if cites.get(i[0], None) == None:
                    cites[i[0]] = [[i[18]]]
                else:
                    if [i[18]] not in cites[i[0]]:
                        cites[i[0]].append([i[18]])

            pubs = {}
            for i in df_final:
                if pubs.get(i[0], None) == None:
                    pubs[i[0]] = [[i[1], i[2], i[3], i[4], i[5], i[6]]]
                else:
                    if [i[1], i[2], i[3], i[4], i[5], i[6]] not in pubs[i[0]]:
                        pubs[i[0]].append([i[1], i[2], i[3], i[4], i[5], i[6]])
            # print(pubs)
            # print("\n")
            # publication_lst = []
            for item in pubs:
                lst_author = []
                lst_ven_id = []
                for a in authors[item]:
                    lst_author.append(Person([a[1]], a[2], a[3]))

                for b in venue[item]:
                    lst_ven_id.append(b[1])

                # print(authors[item])
                publish = Organization([publisher[item][0][1]], publisher[item][0][2])

                lst_cit = []
                for c in cites[item]:
                    doi = c[0]
                    s = list()
                    dic = dict()
                    if doi != 'NA':
                        lst_cit.append(df.getCitesDoi(doi, s, dic))

                f_pub = pubs[item][0]
                # print(venue[item][0])
                if (f_pub[3] != 'NA' or f_pub[4] != 'NA' and f_pub[5] == 'NA'):
                    ven = Journal(lst_ven_id, venue[item][0][2], publish)
                    pub_obj.append(
                        JournalArticle([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author, f_pub[3], f_pub[4]))
                elif f_pub[5] != 'NA':
                    ven = Book(lst_ven_id, venue[item][0][2], publish)

                    pub_obj.append(
                        BookChapter([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author, f_pub[5]))
                else:
                    # ven = Proceedings(lst_ven_id, venue[item][0][2], publish)
                    ven = Proceedings(lst_ven_id, venue[item][0][2], publish, venue[item][0][3])
                    pub_obj.append(ProceedingsPaper([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author))

        return pub_obj

    def getMostCitedPublication(self):
        column_names = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume', 'pubchapter',
                        'authorinternalid', 'authorid', 'authorgivenName',
                        'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle', 'pubvenueevent',
                        'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername', 'citesdoi']

        df_empty = pd.DataFrame(columns=column_names)
        pub_obj = []
        for df in self.queryProcessor:
            current = df.getMostCitedPublication()
            current.columns = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume', 'pubchapter',
                               'authorinternalid', 'authorid', 'authorgivenName',
                               'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle',
                               'pubvenueevent',
                               'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername',
                               'citesdoi']
            # print(current.dtypes)
            current["pubissue"] = current["pubissue"].astype("string")
            current["pubvolume"] = current["pubvolume"].astype("string")
            current["pubvenueevent"] = current["pubvenueevent"].astype("string")
            current["pubissue"] = current["pubissue"].apply(self.remove_dotzero)
            current["pubvolume"] = current["pubvolume"].apply(self.remove_dotzero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()
            authors = {}
            for i in df_final:

                if authors.get(i[0], None) == None:
                    authors[i[0]] = [[i[7], i[8], i[9], i[10]]]
                else:
                    if [i[7], i[8], i[9], i[10]] not in authors[i[0]]:
                        authors[i[0]].append([i[7], i[8], i[9], i[10]])

            venue = {}
            for i in df_final:

                if venue.get(i[0], None) == None:
                    venue[i[0]] = [[i[11], i[12], i[13], i[14]]]
                else:
                    if [i[11], i[12], i[13], i[14]] not in venue[i[0]]:
                        venue[i[0]].append([i[11], i[12], i[13], i[14]])

            publisher = {}
            for i in df_final:

                if publisher.get(i[0], None) == None:
                    publisher[i[0]] = [[i[15], i[16], i[17]]]
                else:
                    if [i[15], i[16], i[17]] not in publisher[i[0]]:
                        publisher[i[0]].append([i[15], i[16], i[17]])

            cites = {}
            for i in df_final:

                if cites.get(i[0], None) == None:
                    cites[i[0]] = [[i[18]]]
                else:
                    if [i[18]] not in cites[i[0]]:
                        cites[i[0]].append([i[18]])

            pubs = {}
            for i in df_final:
                if pubs.get(i[0], None) == None:
                    pubs[i[0]] = [[i[1], i[2], i[3], i[4], i[5], i[6]]]
                else:
                    if [i[1], i[2], i[3], i[4], i[5], i[6]] not in pubs[i[0]]:
                        pubs[i[0]].append([i[1], i[2], i[3], i[4], i[5], i[6]])
            # print(pubs)
            # print("\n")
            # publication_lst = []
            for item in pubs:
                lst_author = []
                lst_ven_id = []
                for a in authors[item]:
                    lst_author.append(Person([a[1]], a[2], a[3]))

                for b in venue[item]:
                    lst_ven_id.append(b[1])

                # print(authors[item])
                publish = Organization([publisher[item][0][1]], publisher[item][0][2])


                lst_cit = []
                for c in cites[item]:
                    doi = c[0]
                    s = list()
                    dic = dict()
                    if doi != 'NA':
                        lst_cit.append(df.getCitesDoi(doi, s, dic))

                f_pub = pubs[item][0]
                # print(venue[item][0])
                if (f_pub[3] != 'NA' or f_pub[4] != 'NA' and f_pub[5] == 'NA'):
                    ven = Journal(lst_ven_id, venue[item][0][2], publish)
                    pub_obj.append(
                        JournalArticle([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author, f_pub[3], f_pub[4]))
                elif f_pub[5] != 'NA':
                    ven = Book(lst_ven_id, venue[item][0][2], publish)

                    pub_obj.append(
                        BookChapter([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author, f_pub[5]))
                else:
                    # ven = Proceedings(lst_ven_id, venue[item][0][2], publish)
                    ven = Proceedings(lst_ven_id, venue[item][0][2],publish,venue[item][0][3])
                    pub_obj.append(ProceedingsPaper([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author))


        return pub_obj[0]

    def getMostCitedVenue(self):

        column_names = ["internalID", "id", "title", "publisherinternal", "publisherid", "publishertitle", "event"]

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            # print(df)
            current = df.getMostCitedVenue()
            current.columns =["internalID", "id", "title", "publisherinternal", "publisherid","publishertitle","event"]
            df_empty = concat([df_empty, current], ignore_index=True)


        df_final = df_empty.fillna('NA')

        df_final = df_final.values.tolist()
        # print(df_final)
        dic = {}
        for i in df_final:
            if dic.get(i[1], None) == None:
                dic[i[0]] = [i[1]]
            else:
                dic[i[0]].append(i[1])

        venues = []
        # print(df_final)
        for item in df_final:
            publ = Organization(item[4], item[5])
            if item[6] != 'NA':
                v = Proceedings(dic[item[0]], item[2], publ, item[6])
            else:
                v = Venue(dic[item[0]], item[2], publ)
            venues.append(v)
        # print(len(venues))
        return venues[0]

    def getVenuesByPublisherId(self, id):
        column_names = ["internalID", "id", "title", "publisherinternal", "publisherid","publishertitle","event"]

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getVenuesByPublisherId(id)
            current.columns =["internalID", "id", "title", "publisherinternal", "publisherid","publishertitle","event"]
            df_empty = concat([df_empty, current], ignore_index=True)

        df_final=df_empty.fillna("NA")

        df_final = df_final.values.tolist()
        dic={}
        for i in df_final:
            if dic.get(i[1],None)==None:
                dic[i[0]]=[i[1]]
            else:
                dic[i[0]].append(i[1])

        venues=[]
        for item in df_final:
            publ=Organization(item[4],item[5])
            if item[6] != 'NA':
                v = Proceedings(dic[item[0]], item[2], publ, item[6])
            else:
                v = Venue(dic[item[0]], item[2], publ)
            venues.append(v)

        return venues

    def getPublicationInVenue(self, venueId):
        column_names = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume', 'pubchapter',
                        'authorinternalid', 'authorid', 'authorgivenName',
                        'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle', 'pubvenueevent',
                        'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername', 'citesdoi']

        df_empty = pd.DataFrame(columns=column_names)
        pub_obj = []
        for df in self.queryProcessor:
            current = df.getPublicationInVenue(venueId)
            current.columns = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume', 'pubchapter',
                               'authorinternalid', 'authorid', 'authorgivenName',
                               'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle',
                               'pubvenueevent',
                               'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername',
                               'citesdoi']
            # print(current.dtypes)
            current["pubissue"] = current["pubissue"].astype("string")
            current["pubvolume"] = current["pubvolume"].astype("string")
            current["pubvenueevent"] = current["pubvenueevent"].astype("string")
            current["pubissue"] = current["pubissue"].apply(self.remove_dotzero)
            current["pubvolume"] = current["pubvolume"].apply(self.remove_dotzero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()
            authors = {}
            for i in df_final:

                if authors.get(i[0], None) == None:
                    authors[i[0]] = [[i[7], i[8], i[9], i[10]]]
                else:
                    if [i[7], i[8], i[9], i[10]] not in authors[i[0]]:
                        authors[i[0]].append([i[7], i[8], i[9], i[10]])

            venue = {}
            for i in df_final:

                if venue.get(i[0], None) == None:
                    venue[i[0]] = [[i[11], i[12], i[13], i[14]]]
                else:
                    if [i[11], i[12], i[13], i[14]] not in venue[i[0]]:
                        venue[i[0]].append([i[11], i[12], i[13], i[14]])

            publisher = {}
            for i in df_final:

                if publisher.get(i[0], None) == None:
                    publisher[i[0]] = [[i[15], i[16], i[17]]]
                else:
                    if [i[15], i[16], i[17]] not in publisher[i[0]]:
                        publisher[i[0]].append([i[15], i[16], i[17]])

            cites = {}
            for i in df_final:

                if cites.get(i[0], None) == None:
                    cites[i[0]] = [[i[18]]]
                else:
                    if [i[18]] not in cites[i[0]]:
                        cites[i[0]].append([i[18]])

            pubs = {}
            for i in df_final:
                if pubs.get(i[0], None) == None:
                    pubs[i[0]] = [[i[1], i[2], i[3], i[4], i[5], i[6]]]
                else:
                    if [i[1], i[2], i[3], i[4], i[5], i[6]] not in pubs[i[0]]:
                        pubs[i[0]].append([i[1], i[2], i[3], i[4], i[5], i[6]])
            # print(pubs)
            # print("\n")
            # publication_lst = []
            for item in pubs:
                lst_author = []
                lst_ven_id = []
                for a in authors[item]:
                    lst_author.append(Person([a[1]], a[2], a[3]))

                for b in venue[item]:
                    lst_ven_id.append(b[1])

                # print(authors[item])
                publish = Organization([publisher[item][0][1]], publisher[item][0][2])

                lst_cit = []
                for c in cites[item]:
                    doi = c[0]
                    s = list()
                    dic = dict()
                    if doi != 'NA':
                        lst_cit.append(df.getCitesDoi(doi, s, dic))

                f_pub = pubs[item][0]
                # print(venue[item][0])
                if (f_pub[3] != 'NA' or f_pub[4] != 'NA' and f_pub[5] == 'NA'):
                    ven = Journal(lst_ven_id, venue[item][0][2], publish)
                    pub_obj.append(
                        JournalArticle([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author, f_pub[3], f_pub[4]))
                elif f_pub[5] != 'NA':
                    ven = Book(lst_ven_id, venue[item][0][2], publish)

                    pub_obj.append(
                        BookChapter([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author, f_pub[5]))
                else:
                    # ven = Proceedings(lst_ven_id, venue[item][0][2], publish)
                    ven = Proceedings(lst_ven_id, venue[item][0][2], publish, venue[item][0][3])
                    pub_obj.append(ProceedingsPaper([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author))

        return pub_obj

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        column_names = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume',
                        'authorinternalid', 'authorid', 'authorgivenName',
                        'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle',
                        'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername', 'citesdoi']

        df_empty = pd.DataFrame(columns=column_names)
        pub_obj = []
        for df in self.queryProcessor:
            current = df.getJournalArticlesInIssue(issue, volume, journalId)
            current.columns = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume',
                               'authorinternalid', 'authorid', 'authorgivenName',
                               'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle',
                               'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername',
                               'citesdoi']
            # print(current.dtypes)
            current["pubissue"] = current["pubissue"].astype("string")
            current["pubvolume"] = current["pubvolume"].astype("string")

            current["pubissue"] = current["pubissue"].apply(self.remove_dotzero)
            current["pubvolume"] = current["pubvolume"].apply(self.remove_dotzero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()
            authors = {}
            for i in df_final:
                # print(i)
                if authors.get(i[0], None) == None:
                    authors[i[0]] = [[i[6], i[7], i[8], i[9]]]
                else:
                    if [i[6], i[7], i[8], i[9]] not in authors[i[0]]:
                        authors[i[0]].append([i[6], i[7], i[8], i[9]])

            venue = {}
            for i in df_final:
                # print(i[12])
                if venue.get(i[0], None) == None:
                    venue[i[0]] = [[i[10], i[11], i[12]]]
                else:
                    if [i[10], i[11], i[12], i[13]] not in venue[i[0]]:
                        venue[i[0]].append([i[10], i[11], i[12]])

            publisher = {}

            for i in df_final:
                # print(i[15])
                if publisher.get(i[0], None) == None:
                    publisher[i[0]] = [[i[13],i[14], i[15]]]
                else:
                    if [i[13],i[14], i[15]] not in publisher[i[0]]:
                        publisher[i[0]].append([i[13],i[14], i[15]])

            cites = {}
            for i in df_final:
                # print(i)
                if cites.get(i[0], None) == None:
                    cites[i[0]] = [[i[16]]]
                else:
                    if [i[16]] not in cites[i[0]]:
                        cites[i[0]].append([i[16]])

            pubs = {}
            for i in df_final:
                if pubs.get(i[0], None) == None:
                    pubs[i[0]] = [[i[1], i[2], i[3], i[4], i[5], i[6]]]
                else:
                    if [i[1], i[2], i[3], i[4], i[5], i[6]] not in pubs[i[0]]:
                        pubs[i[0]].append([i[1], i[2], i[3], i[4], i[5], i[6]])
            # print(pubs)
            # print("\n")
            # publication_lst = []
            for item in pubs:
                lst_author = []
                lst_ven_id = []
                for a in authors[item]:
                    # print(a)
                    lst_author.append(Person([a[0]], a[1], a[2]))

                for b in venue[item]:
                    lst_ven_id.append(b[1])

                # print(publisher[item][0][1])
                publish = Organization([publisher[item][0][0]], publisher[item][0][1])



                lst_cit = []
                for c in cites[item]:
                    doi = c[0]
                    s = list()
                    dic = dict()
                    if doi != 'NA':
                        lst_cit.append(df.getCitesDoi(doi, s, dic))

                f_pub = pubs[item][0]
                # print(lst_ven_id)
                ven = Journal(lst_ven_id, venue[item][0][1], publish)
                pub_obj.append(JournalArticle([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author, f_pub[3], f_pub[4]))


        # print(pub_obj)
        # pub_obj.append(publication_lst[0])
        return pub_obj

    def getJournalArticlesInVolume(self, volume, journalId):
        column_names = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume',
                        'authorinternalid', 'authorid', 'authorgivenName',
                        'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle',
                        'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername', 'citesdoi']

        df_empty = pd.DataFrame(columns=column_names)
        pub_obj = []
        for df in self.queryProcessor:
            current = df.getJournalArticlesInVolume( volume, journalId)
            current.columns = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume',
                               'authorinternalid', 'authorid', 'authorgivenName',
                               'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle',
                               'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername',
                               'citesdoi']
            # print(current.dtypes)
            current["pubissue"] = current["pubissue"].astype("string")
            current["pubvolume"] = current["pubvolume"].astype("string")

            current["pubissue"] = current["pubissue"].apply(self.remove_dotzero)
            current["pubvolume"] = current["pubvolume"].apply(self.remove_dotzero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()
            authors = {}
            for i in df_final:
                # print(i)
                if authors.get(i[0], None) == None:
                    authors[i[0]] = [[i[6], i[7], i[8], i[9]]]
                else:
                    if [i[6], i[7], i[8], i[9]] not in authors[i[0]]:
                        authors[i[0]].append([i[6], i[7], i[8], i[9]])

            venue = {}
            for i in df_final:
                # print(i[12])
                if venue.get(i[0], None) == None:
                    venue[i[0]] = [[i[10], i[11], i[12]]]
                else:
                    if [i[10], i[11], i[12], i[13]] not in venue[i[0]]:
                        venue[i[0]].append([i[10], i[11], i[12]])

            publisher = {}

            for i in df_final:
                # print(i[15])
                if publisher.get(i[0], None) == None:
                    publisher[i[0]] = [[i[13], i[14], i[15]]]
                else:
                    if [i[13], i[14], i[15]] not in publisher[i[0]]:
                        publisher[i[0]].append([i[13], i[14], i[15]])

            cites = {}
            for i in df_final:
                # print(i)
                if cites.get(i[0], None) == None:
                    cites[i[0]] = [[i[16]]]
                else:
                    if [i[16]] not in cites[i[0]]:
                        cites[i[0]].append([i[16]])

            pubs = {}
            for i in df_final:
                if pubs.get(i[0], None) == None:
                    pubs[i[0]] = [[i[1], i[2], i[3], i[4], i[5], i[6]]]
                else:
                    if [i[1], i[2], i[3], i[4], i[5], i[6]] not in pubs[i[0]]:
                        pubs[i[0]].append([i[1], i[2], i[3], i[4], i[5], i[6]])
            # print(pubs)
            # print("\n")
            # publication_lst = []
            for item in pubs:
                lst_author = []
                lst_ven_id = []
                for a in authors[item]:
                    # print(a)
                    lst_author.append(Person([a[0]], a[1], a[2]))

                for b in venue[item]:
                    lst_ven_id.append(b[1])

                # print(publisher[item][0][1])
                publish = Organization([publisher[item][0][0]], publisher[item][0][1])

                lst_cit = []
                for c in cites[item]:
                    doi = c[0]
                    s = list()
                    dic = dict()
                    if doi != 'NA':
                        lst_cit.append(df.getCitesDoi(doi, s, dic))

                f_pub = pubs[item][0]
                # print(lst_ven_id)
                ven = Journal(lst_ven_id, venue[item][0][1], publish)
                pub_obj.append(
                    JournalArticle([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author, f_pub[3], f_pub[4]))

        # print(pub_obj)
        # pub_obj.append(publication_lst[0])
        return pub_obj

    def getJournalArticlesInJournal(self, journalId):
        column_names = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume',
                        'authorinternalid', 'authorid', 'authorgivenName',
                        'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle',
                        'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername', 'citesdoi']

        df_empty = pd.DataFrame(columns=column_names)
        pub_obj = []
        for df in self.queryProcessor:
            current = df.getJournalArticlesInJournal( journalId)
            current.columns = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume',
                               'authorinternalid', 'authorid', 'authorgivenName',
                               'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle',
                               'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername',
                               'citesdoi']
            # print(current.dtypes)
            current["pubissue"] = current["pubissue"].astype("string")
            current["pubvolume"] = current["pubvolume"].astype("string")

            current["pubissue"] = current["pubissue"].apply(self.remove_dotzero)
            current["pubvolume"] = current["pubvolume"].apply(self.remove_dotzero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()
            authors = {}
            for i in df_final:
                # print(i)
                if authors.get(i[0], None) == None:
                    authors[i[0]] = [[i[6], i[7], i[8], i[9]]]
                else:
                    if [i[6], i[7], i[8], i[9]] not in authors[i[0]]:
                        authors[i[0]].append([i[6], i[7], i[8], i[9]])

            venue = {}
            for i in df_final:
                # print(i[12])
                if venue.get(i[0], None) == None:
                    venue[i[0]] = [[i[10], i[11], i[12]]]
                else:
                    if [i[10], i[11], i[12], i[13]] not in venue[i[0]]:
                        venue[i[0]].append([i[10], i[11], i[12]])

            publisher = {}

            for i in df_final:
                # print(i[15])
                if publisher.get(i[0], None) == None:
                    publisher[i[0]] = [[i[13], i[14], i[15]]]
                else:
                    if [i[13], i[14], i[15]] not in publisher[i[0]]:
                        publisher[i[0]].append([i[13], i[14], i[15]])

            cites = {}
            for i in df_final:
                # print(i)
                if cites.get(i[0], None) == None:
                    cites[i[0]] = [[i[16]]]
                else:
                    if [i[16]] not in cites[i[0]]:
                        cites[i[0]].append([i[16]])

            pubs = {}
            for i in df_final:
                if pubs.get(i[0], None) == None:
                    pubs[i[0]] = [[i[1], i[2], i[3], i[4], i[5], i[6]]]
                else:
                    if [i[1], i[2], i[3], i[4], i[5], i[6]] not in pubs[i[0]]:
                        pubs[i[0]].append([i[1], i[2], i[3], i[4], i[5], i[6]])
            # print(pubs)
            # print("\n")
            # publication_lst = []
            for item in pubs:
                lst_author = []
                lst_ven_id = []
                for a in authors[item]:
                    # print(a)
                    lst_author.append(Person([a[0]], a[1], a[2]))

                for b in venue[item]:
                    lst_ven_id.append(b[1])

                # print(publisher[item][0][1])
                publish = Organization([publisher[item][0][0]], publisher[item][0][1])

                lst_cit = []
                for c in cites[item]:
                    doi = c[0]
                    s = list()
                    dic = dict()
                    if doi != 'NA':
                        lst_cit.append(df.getCitesDoi(doi, s, dic))

                f_pub = pubs[item][0]
                # print(lst_ven_id)
                ven = Journal(lst_ven_id, venue[item][0][1], publish)
                pub_obj.append(
                    JournalArticle([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author, f_pub[3], f_pub[4]))

        # print(pub_obj)
        # pub_obj.append(publication_lst[0])
        return pub_obj

    def getProceedingsByEvent(self, eventPartialName):
        column_names = ["internalID", "id", "title", "publisherinternal","publishertitle","event"]

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current= df.getProceedingsByEvent(eventPartialName)
            current.columns = ["internalID", "id", "title", "publisherinternal","publisherid","publishertitle","event"]
            df_empty = concat([df_empty, current], ignore_index=True)

        df_final = df_empty.fillna("NA")
        df_final = df_final.values.tolist()
        dic = {}
        for i in df_final:
            if dic.get(i[1], None) == None:
                dic[i[0]] = [i[1]]
            else:
                dic[i[0]].append(i[1])

        venues = []
        for item in df_final:
            publ = Organization(item[4], item[5])
            v = Proceedings(dic[item[0]], item[2], publ, item[6])
            venues.append(v)

        return venues

    def getPublicationAuthors(self, publicationId):

        column_names = ["internalID", "id", "givenname", "familyname"]

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getPublicationAuthors(publicationId)
            current.columns = ["internalID", "id", "givenname", "familyname"]
            df_empty = concat([df_empty, current], ignore_index=True)


        df_final = df_empty.fillna("NA")
        df_final = df_final.values.tolist()

        authors = []
        for item in df_final:
            # print([item[1]],item[2],item[3])
            a = Person([item[1]],item[2],item[3])
            authors.append(a)

        return authors

    def getPublicationsByAuthorsName(self, authorPartialName):
        column_names = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume', 'pubchapter',
                        'authorinternalid', 'authorid', 'authorgivenName',
                        'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle', 'pubvenueevent',
                        'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername', 'citesdoi']

        df_empty = pd.DataFrame(columns=column_names)
        pub_obj = []
        for df in self.queryProcessor:
            current = df.getPublicationsByAuthorsName(authorPartialName)
            current.columns = ['internalid', 'doi', 'title', 'pubyear', 'pubissue', 'pubvolume', 'pubchapter',
                               'authorinternalid', 'authorid', 'authorgivenName',
                               'authorfamilyName', 'pubvenueinternalid', 'pubvenueissn', 'pubvenuetitle',
                               'pubvenueevent',
                               'pubvenuepublisherinternalid', 'pubvenuepublisherid', 'pubvenuepublishername',
                               'citesdoi']
            # print(current.dtypes)
            current["pubissue"] = current["pubissue"].astype("string")
            current["pubvolume"] = current["pubvolume"].astype("string")
            current["pubvenueevent"] = current["pubvenueevent"].astype("string")
            current["pubissue"] = current["pubissue"].apply(self.remove_dotzero)
            current["pubvolume"] = current["pubvolume"].apply(self.remove_dotzero)
            current = current.fillna("NA")
            df_empty = concat([df_empty, current], ignore_index=True)
            df_final = df_empty
            df_final = df_final.values.tolist()
            authors = {}
            for i in df_final:

                if authors.get(i[0], None) == None:
                    authors[i[0]] = [[i[7], i[8], i[9], i[10]]]
                else:
                    if [i[7], i[8], i[9], i[10]] not in authors[i[0]]:
                        authors[i[0]].append([i[7], i[8], i[9], i[10]])

            venue = {}
            for i in df_final:

                if venue.get(i[0], None) == None:
                    venue[i[0]] = [[i[11], i[12], i[13], i[14]]]
                else:
                    if [i[11], i[12], i[13], i[14]] not in venue[i[0]]:
                        venue[i[0]].append([i[11], i[12], i[13], i[14]])

            publisher = {}
            for i in df_final:

                if publisher.get(i[0], None) == None:
                    publisher[i[0]] = [[i[15], i[16], i[17]]]
                else:
                    if [i[15], i[16], i[17]] not in publisher[i[0]]:
                        publisher[i[0]].append([i[15], i[16], i[17]])

            cites = {}
            for i in df_final:

                if cites.get(i[0], None) == None:
                    cites[i[0]] = [[i[18]]]
                else:
                    if [i[18]] not in cites[i[0]]:
                        cites[i[0]].append([i[18]])

            pubs = {}
            for i in df_final:
                if pubs.get(i[0], None) == None:
                    pubs[i[0]] = [[i[1], i[2], i[3], i[4], i[5], i[6]]]
                else:
                    if [i[1], i[2], i[3], i[4], i[5], i[6]] not in pubs[i[0]]:
                        pubs[i[0]].append([i[1], i[2], i[3], i[4], i[5], i[6]])
            # print(pubs)
            # print("\n")
            # publication_lst = []
            for item in pubs:
                lst_author = []
                lst_ven_id = []
                for a in authors[item]:
                    lst_author.append(Person([a[1]], a[2], a[3]))

                for b in venue[item]:
                    lst_ven_id.append(b[1])

                # print(authors[item])
                publish = Organization([publisher[item][0][1]], publisher[item][0][2])

                lst_cit = []
                for c in cites[item]:
                    doi = c[0]
                    s = list()
                    dic = dict()
                    if doi != 'NA':
                        lst_cit.append(df.getCitesDoi(doi, s, dic))

                f_pub = pubs[item][0]
                # print(venue[item][0])
                if (f_pub[3] != 'NA' or f_pub[4] != 'NA' and f_pub[5] == 'NA'):
                    ven = Journal(lst_ven_id, venue[item][0][2], publish)
                    pub_obj.append(
                        JournalArticle([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author, f_pub[3], f_pub[4]))
                elif f_pub[5] != 'NA':
                    ven = Book(lst_ven_id, venue[item][0][2], publish)

                    pub_obj.append(
                        BookChapter([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author, f_pub[5]))
                else:
                    # ven = Proceedings(lst_ven_id, venue[item][0][2], publish)
                    ven = Proceedings(lst_ven_id, venue[item][0][2], publish, venue[item][0][3])
                    pub_obj.append(ProceedingsPaper([f_pub[0]], f_pub[2], f_pub[1], ven, lst_cit, lst_author))

        return pub_obj

    def getDistinctPublishersOfPublications(self, pubIdList):
        column_names = ["internalID", "id", "name"]

        df_empty = pd.DataFrame(columns=column_names)

        for df in self.queryProcessor:
            current = df.getDistinctPublishersOfPublications(pubIdList)
            current.columns = ["internalID", "id", "name"]
            df_empty = concat([df_empty, current], ignore_index=True)

        df_final = df_empty.fillna("NA")
        df_final = df_final.values.tolist()
        org = []
        for item in df_final:
            # print([item[1]],item[2])
            o = Organization([item[1]],item[2])
            org.append(o)

        return org
# Relational Database

class RelationalProcessor(object):
    def __init__(self):
        self.dbPath = ''

    def getDbPath(self):
        return self.dbPath

    def setDbPath(self, path):
        self.dbPath = path


#The upload data function in both triplestore and relational data processors are  divided into 2 parts.
# One is for json files and one is for csv file
#For json file we create Organizations and Authors. For references and venuesids, it will link references to
#publications if they already exist in the database. It will link venueids to venues if they alreaady exist in the
# database. It also links authors and organizations to publications and venues if they exist

#For csv files we create Publications and Venues. If authors exist, we link them to the created publications. If organizations
# exist we link them to the created venues.

class RelationalDataProcessor(RelationalProcessor, QueryProcessor):
    def __init__(self):
        super().__init__()


    def uploadData(self,path):

        if path.split(".")[1] == 'json':
            #checking if database exists or not
            if os.path.exists(self.getDbPath()):

                #checking all the publications and venues and making a dataframe from them. If they dont exist we just create an empty df
                con = connect(self.getDbPath(), uri=True)
                query = """
                        SELECT  a.id, a.title, a.type,
                        a.publication_year , a.issue, a.volume, a.chapter,  a.venue_type, a.publication_venue,
                        a.publisher, a.event
                        FROM
                        (
                        Select PubID.id as id, JournalArticle.title as title, 'journal-article' as type,
                        publication_year , issue, volume, 'NA' as chapter, 'journal' as venue_type, Journal.title as publication_venue,
                        OrgID.id as publisher, 'NA' as event
                        FROM JournalArticle
                        JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                        Left JOIN Journal ON JournalArticle.PublicationVenue == Journal.internalID
                        Left JOIN OrgID ON Journal.Publisher == OrgID.OrgID

                        UNION
                        SELECT PubID.id as id, BookChapter.title as title, 'book-chapter' as type, publication_year , 'NA' as issue,'NA' as volume, chapternumber, 'book' as venue_type, Book.title as publication_venue,OrgID.id as publisher, 'NA' as event
                        FROM BookChapter
                        JOIN PubID ON BookChapter.internalID == PubID.PublicationID
                        Left JOIN Book ON BookChapter.PublicationVenue == Book.internalID
                        Left JOIN OrgID ON Book.Publisher == OrgID.OrgID

                        UNION
                        SELECT PubID.id as id, ProceedingPaper.title as title, 'proceedings-paper' as type, publication_year , 'NA' as issue,'NA' as volume,  'NA' as chapternumber, 'proceedings' as venue_type, Proceeding.title as publication_venue,OrgID.id as publisher, event
                        FROM ProceedingPaper
                        JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID
                        Left JOIN Proceeding ON ProceedingPaper.PublicationVenue == Proceeding.internalID
                        Left JOIN OrgID ON Proceeding.Publisher == OrgID.OrgID) a

                        ;
                        """
                publications = read_sql(query, con)

            else:
                publications = pd.DataFrame({'id': pd.Series(dtype='str'),
                                        'title': pd.Series(dtype='str'),
                                        'type': pd.Series(dtype='str'),
                                        'publication_year': pd.Series(dtype='int'),
                                        'issue': pd.Series(dtype='str'),
                                        'volume': pd.Series(dtype='str'),
                                        'chapter': pd.Series(dtype='str'),
                                        'publication_venue': pd.Series(dtype='str'),
                                        'venue_type': pd.Series(dtype='str'),
                                        'publisher': pd.Series(dtype='str'),
                                        'event': pd.Series(dtype='str')})





            with open(path, "r", encoding="utf-8") as f:
                json_doc = load(f)

                ## Making venue
                venues = json_doc["venues_id"]

                lst_doi = []
                lst_issn = []

                for key in venues:
                    for item in venues[key]:
                        lst_doi.append(key)
                        lst_issn.append(item)



                venues_df = DataFrame({
                    "doi": Series(lst_doi, dtype="string", name="doi"),
                    "issn": Series(lst_issn, dtype="string", name="issn")
                })



                venue_internal_id = []

                if os.path.exists(self.getDbPath()):
                    with connect(self.getDbPath()) as con:
                        query = """
                        
                        SELECT Journal.internalID as internalID, PubID.id as id
                        FROM JournalArticle 
                        JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                        JOIN Journal ON JournalArticle.PublicationVenue == Journal.internalID
                        
                        UNION
                        
                        SELECT Book.internalID as internalID, PubID.id as id
                        FROM BookChapter 
                        JOIN PubID ON BookChapter.internalID == PubID.PublicationID
                        JOIN Book ON BookChapter.PublicationVenue == Book.internalID
                        
                        UNION
                        
                        SELECT Proceeding.internalID as internalID, PubID.id as id
                        FROM ProceedingPaper 
                        JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID
                        JOIN Proceeding ON ProceedingPaper.PublicationVenue == Proceeding.internalID
        
                        """
                        df_sql = read_sql(query, con)

                        df_sql = df_sql.values.tolist()

                        ven_dict = {}
                        for elem in df_sql:
                            if elem[1] in ven_dict:
                                ven_dict[elem[1]].append(elem[0])
                            else:
                                ven_dict[elem[1]] = [elem[0]]


                        l1=[]
                        l2=[]
                        for idx, row in venues_df.iterrows():

                            if ven_dict.get(row["doi"],None) !=None:

                                l1.append(row["doi"])
                                l2.append(ven_dict.get(row["doi"])[0])

                                venue_internal_id.append(ven_dict.get(row["doi"])[0])

                        venueID_df = DataFrame({
                            "doiordinati": Series(l1, dtype="string", name="doiordinati"),
                            "VenueID": Series(l2, dtype="string", name="VenueID")
                        })

                        venueID_df= venueID_df.drop_duplicates()
                        doi_merge = merge(venueID_df, venues_df, left_on="doiordinati", right_on="doi")



                        VenueID = doi_merge[["VenueID", "issn"]]
                        VenueID = VenueID.rename(columns={"issn": "id"})


                else:
                    for idx, row in venues_df.iterrows():
                        venue_internal_id.append("venue-" + str(idx))




                    doi_single_venue = []
                    for doi in lst_doi:
                        if doi not in doi_single_venue:
                            doi_single_venue.append(doi)


                    venueID_df = DataFrame({
                        "doiordinati": Series(doi_single_venue, dtype="string", name="doiordinati"),
                        "VenueID": Series(venue_internal_id, dtype="string", name="VenueID")
                    })
                    doi_merge = merge(venueID_df, venues_df, left_on="doiordinati", right_on="doi")



                    VenueID = doi_merge[["VenueID", "issn"]]
                    VenueID = VenueID.rename(columns={"issn": "id"})
                    VenueID = pd.DataFrame({'VenueID': pd.Series(dtype='str'),
                                            'id': pd.Series(dtype='str')
                                            })






            #Getting all the authors
            authors = json_doc["authors"]

            lst_item = []
            lst_doi_authors = []

            for key in authors:
                for item in authors[key]:
                    lst_doi_authors.append(key)
                    lst_item.append(item)


            people = []

            for item in lst_item:
                if item not in people:
                    people.append(item)



            familylst = []
            for item in people:
                familylst.append(item.get("family"))



            givenlst = []
            for item in people:
                givenlst.append(item.get("given"))



            orcidlst = []
            for item in people:
                orcidlst.append(item.get("orcid"))



            people = DataFrame({
                "orcid": Series(orcidlst, dtype="string", name="orcid"),
                "given_name": Series(givenlst, dtype="string", name="given"),
                "family_name": Series(familylst, dtype="string", name="family"),
            })


            person_internal_id = []

            #getting count of existing person
            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = "SELECT count(*) FROM Person"
                    df_sql = read_sql(query, con)
                    person_count = df_sql.values.tolist()[0][0] + 1

            else:
                person_count =0

            #making new person
            for idx, row in people.iterrows():
                person_internal_id.append("person-" + str(idx + person_count))


            PersonID = DataFrame({
                "person_internal_id": Series(person_internal_id, dtype="string", name="person_internal_id"),
                "orcid": Series(orcidlst, dtype="string", name="orcid")
            })

            PersonID = PersonID.rename(columns={"orcid": "id"})


            PersonTable = DataFrame({
                "person_internal_id": Series(person_internal_id, dtype="string", name="person_internal_id"),
                "given_name": Series(givenlst, dtype="string", name="given"),
                "family_name": Series(familylst, dtype="string", name="family"),
            })

            PersonTable = PersonTable.rename(columns={"person_internal_id": "internalID"})


            ##AUTHORSTABLE
            lst_doi_single_authors = []
            for key in authors:
                lst_doi_single_authors.append(key)



            ciao = DataFrame({
                "doi_single_authors": Series(lst_doi_single_authors, dtype="string", name="doi_single_authors"),
            })

            authors_internal_id = []
            #getting count of existing authors
            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = "SELECT count(*) FROM Author"
                    df_sql = read_sql(query, con)
                    author_count = df_sql.values.tolist()[0][0] + 1

            else:
                author_count = 0

            # creating new authors
            for idx, row in ciao.iterrows():
                authors_internal_id.append("authors-" + str(idx + author_count))


            authorsID_df = DataFrame({
                "doi_single_authors": Series(lst_doi_single_authors, dtype="string", name="doi_single_authors"),
                "AuthorID": Series(authors_internal_id, dtype="string", name="AuthorID")
            })



            orcidlst_tutti = []
            for item in lst_item:
                orcidlst_tutti.append(item.get("orcid"))



            cane = DataFrame({
                "lst_doi_authors": Series(lst_doi_authors, dtype="string", name="lst_doi_authors"),
                "orcidlst_tutti": Series(orcidlst_tutti, dtype="string", name="orcidlst_tutti"),
            })



            unione = merge(authorsID_df, cane, left_on="doi_single_authors", right_on="lst_doi_authors")



            AuthorsID = unione[["AuthorID"]]



            PersonTable1 = DataFrame({
                "person_internal_id": Series(person_internal_id, dtype="string", name="person_internal_id"),
                "given_name": Series(givenlst, dtype="string", name="given"),
                "family_name": Series(familylst, dtype="string", name="family"),
                "orcid": Series(orcidlst, dtype="string", name="orcid")
            })



            unione1 = merge(cane, PersonTable1, left_on="orcidlst_tutti", right_on="orcid")


            gatto = unione1[["lst_doi_authors", "person_internal_id"]]



            unione2 = merge(gatto, authorsID_df, left_on="lst_doi_authors", right_on="doi_single_authors")

            #creating authors table with doi and personID. Will use them later when we link publications

            Authors = unione2[["lst_doi_authors", "person_internal_id"]]
            Authors = Authors.rename(columns={"lst_doi_authors": "AuthorID"})




            ##PUBBLICATIONSID
            publication_ids = publications[["id"]]


            # Create a new column with internal identifiers for each publication
            publication_internal_id = []

            if os.path.exists(self.getDbPath()):
                #getting existing ids
                with connect(self.getDbPath()) as con:
                    query = """
                    SELECT internalID,  PubID.id as id
                       FROM JournalArticle
                       JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                    UNION
                    SELECT internalID,  PubID.id as id
                       FROM BookChapter
                       JOIN PubID ON BookChapter.internalID == PubID.PublicationID
                    UNION
                    SELECT internalID,  PubID.id as id
                       FROM ProceedingPaper
                       JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID
                    """
                    df_sql = read_sql(query, con)
                    df_sql = df_sql.values.tolist()

                    pub_dict = {}
                    for elem in df_sql:
                        if elem[1] in pub_dict:
                            pub_dict[elem[1]].append(elem[0])
                        else:
                            pub_dict[elem[1]] = [elem[0]]

                    for idx, row in publications.iterrows():
                        publication_internal_id.append(pub_dict.get(row["id"])[0])

            else:
                for idx, row in publications.iterrows():
                    publication_internal_id.append("publication-" + str(idx))


            publication_ids.insert(0, "PublicationID", Series(publication_internal_id, dtype="string"))

            ##ORGANIZATIONTABLE
            organization = json_doc["publishers"]



            j = "name"
            name_list = [val[j] for key, val in organization.items()]


            OrganizationTable = DataFrame({
                "name": Series(name_list, dtype="string", name="name")
            })


            organization_internal_id = []

            if os.path.exists(self.getDbPath()):
                #getting count of existing orgs
                with connect(self.getDbPath()) as con:
                    query = "SELECT count(*) FROM Organization"
                    df_sql = read_sql(query, con)
                    org_count = df_sql.values.tolist()[0][0] + 1

            else:
                org_count = 0


            #creating new orgs
            for idx, row in OrganizationTable.iterrows():
                organization_internal_id.append("Org-" + str(idx + org_count))


            OrganizationTable.insert(0, "OrgID", Series(organization_internal_id, dtype="string"))
            OrganizationTable = OrganizationTable.rename(columns={"OrgID": "internalID"})


            ##ORGID
            crossref_list = []

            for k in organization:
                crossref_list.append(k)


            orgId_df = DataFrame({
                "id": Series(crossref_list, dtype="string", name="id"),
                "name": Series(name_list, dtype="string", name="name")
            })



            orgId_joined = merge(OrganizationTable, orgId_df, left_on="name", right_on="name")


            OrgId_table = orgId_joined[
                ["internalID", "id"]]
            OrgId_table = OrgId_table.rename(columns={"internalID": "OrgID"})


            citations = json_doc["references"]


            lst_doi = []
            lst_cit = []
            cit_id = []
            count = 0
            #getting cites information. We will use this later to link Publications
            for key in citations:
                if len(citations[key]) == 0:
                    cit_id.append("cite-" + str(count))
                    lst_doi.append(key)
                    lst_cit.append("NA")
                else:
                    for item in citations[key]:
                        if item == None:
                            lst_cit.append(None)
                        else:
                            cit_id.append("cite-" + str(count))
                            lst_doi.append(key)
                            lst_cit.append(item)
                    count = count + 1



            cite1_df = DataFrame({
                "cit": Series(lst_cit, dtype="string", name="citeid"),
                "doi": Series(lst_doi, dtype="string", name="doi"),
                "internalID": Series(cit_id, dtype="string", name="internalID")

            })


            cite_df = merge(publication_ids, cite1_df, left_on="id", right_on="cit")


            CitesTable = cite_df[["internalID", "PublicationID"]]

            # Merging with organization and venueID table and creating venues if they exist

            ##JOURNALTABLE
            journals = publications.query("venue_type == 'journal'")

            journals_df = journals[["publication_venue", "id", "publisher"]]

            VenueID_journal = doi_merge[["VenueID", "doi"]]
            VenueID_journal = VenueID_journal.drop_duplicates()


            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """SELECT PubID.id, Journal.publisher as Publisher FROM Journal join JournalArticle on JournalArticle.PublicationVenue  = Journal.internalID
                            join PubID on JournalArticle.internalID = PubID.PublicationID
                            """
                    publisher = read_sql(query, con)

            else:
                publisher = pd.DataFrame({'id': pd.Series(dtype='str'),
                                          'Publisher': pd.Series(dtype='str'),
                                            })


            OrgId_table_j = merge(publisher,OrgId_table, left_on="Publisher", right_on="id")
            journal1 = merge(journals_df, VenueID_journal, left_on="id", right_on="doi")
            journalfinal = merge(journal1, OrgId_table_j, left_on="id", right_on="id_x")
            JournalTable = journalfinal[["VenueID", "publication_venue", "OrgID"]]
            JournalTable = JournalTable.rename(columns={"VenueID": "internalID"})
            JournalTable = JournalTable.rename(columns={"OrgID": "Publisher"})
            JournalTable = JournalTable.rename(columns={"publication_venue": "title"})


            ##BOOKTABLE
            books = publications.query("venue_type == 'book'")


            books_df = books[["publication_venue", "id", "publisher"]]


            VenueID_book = doi_merge[["VenueID", "doi"]]
            VenueID_book = VenueID_book.drop_duplicates()

            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """SELECT PubID.id, Book.publisher as Publisher FROM Book join BookChapter 
                    on BookChapter.PublicationVenue  = Book.internalID
                            join PubID on BookChapter.internalID = PubID.PublicationID
                            """
                    publisher = read_sql(query, con)
            else:
                publisher = pd.DataFrame({'id': pd.Series(dtype='str'),
                                          'Publisher': pd.Series(dtype='str'),
                                          })

            OrgId_table_b = merge(publisher, OrgId_table, left_on="Publisher", right_on="id")
            book1 = merge(books_df, VenueID_book, left_on="id", right_on="doi")
            journal1 = merge(journals_df, VenueID_journal, left_on="id", right_on="doi")


            journalfinal = merge(journal1, OrgId_table_j, left_on="id", right_on="id_x")
            bookfinal = merge(book1, OrgId_table_b, left_on="id", right_on="id_x")


            BookTable = bookfinal[["VenueID", "publication_venue", "OrgID"]]
            BookTable = BookTable.rename(columns={"VenueID": "internalID"})
            BookTable = BookTable.rename(columns={"OrgID": "Publisher"})
            BookTable = BookTable.rename(columns={"publication_venue": "title"})



            ##PROCEEDINGTABLE
            proceedings = publications.query("venue_type == 'proceedings'")


            proceedings_df = proceedings[["publication_venue", "id", "publisher", "event"]]


            VenueID_proceedings = doi_merge[["VenueID", "doi"]]
            VenueID_proceedings = VenueID_proceedings.drop_duplicates()


            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """SELECT PubID.id, Proceeding.publisher as Publisher FROM Proceeding join ProceedingPaper 
                    on ProceedingPaper.PublicationVenue  = Proceeding.internalID
                            join PubID on ProceedingPaper.internalID = PubID.PublicationID
                            """
                    publisher = read_sql(query, con)

            else:
                publisher = pd.DataFrame({'id': pd.Series(dtype='str'),
                                          'Publisher': pd.Series(dtype='str'),
                                          })

            OrgId_table_p = merge(publisher, OrgId_table, left_on="Publisher", right_on="id")

            proceeding1 = merge(proceedings_df, VenueID_proceedings, left_on="id", right_on="doi")


            proceedingfinal = merge(proceeding1, OrgId_table_p, left_on="id", right_on="id_x")


            ProceedingTable = proceedingfinal[["VenueID", "publication_venue", "OrgID", "event"]]
            ProceedingTable = ProceedingTable.rename(columns={"VenueID": "internalID"})
            ProceedingTable = ProceedingTable.rename(columns={"OrgID": "Publisher"})
            ProceedingTable = ProceedingTable.rename(columns={"publication_venue": "title"})


            #AuthorID joining with all the  publications if they exist.
            #For all the person that joins with publication we create authorID. For the ones that are not able to join
            #we leave them with just dois so they are able to join with other publications
            joined_auth = merge(publications, unione, left_on="id", right_on="doi_single_authors", how="left")
            joined_auth = joined_auth[["doi_single_authors", "AuthorID"]]
            joined_auth = joined_auth.drop_duplicates()
            joined_auth = joined_auth[joined_auth['doi_single_authors'].notna()]

            dics = joined_auth.set_index('doi_single_authors').T.to_dict('list')

            lst1 = []
            lst2 = []
            for idx, row in Authors.iterrows():
                AID = row["AuthorID"]
                p_internal_id = row["person_internal_id"]
                if dics.get(AID, None) == None:
                    lst1.append(AID)
                else:
                    lst1.append(dics[AID][0])
                lst2.append(p_internal_id)

            Authors = DataFrame({
                "AuthorID": Series(lst1, dtype="string", name="AuthorID"),
                "person_internal_id": Series(lst2, dtype="string", name="person_internal_id")

            })

            #For each of the publications, we merge them with citations, authors and venues to get the final tables

            ##JOURNALARCTICLETABLE
            journal_articles = publications.query("type == 'journal-article'")


            pub_joined = merge(publication_ids, journal_articles, left_on="id", right_on="id")

            cit_joined = merge(pub_joined, cite1_df, left_on="id", right_on="doi")
            cit_joined = cit_joined[
                ["PublicationID", "publication_year", "title", "internalID", "issue", "volume", "doi"]]

            cit_joined = cit_joined.drop_duplicates()


            authors_joined = merge(cit_joined, unione, left_on="doi", right_on="doi_single_authors")
            authors_joined = authors_joined.drop_duplicates()


            authors_joined = authors_joined[
                ["PublicationID", "publication_year", "title", "internalID", "issue", "volume", "doi", "AuthorID"]]

            venue_joined = merge(authors_joined, venueID_df, left_on="doi", right_on="doiordinati")
            venue_joined = venue_joined.drop_duplicates()
            venue_joined = venue_joined[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorID", "VenueID", "issue", "volume"]]


            JournalArticleTable = venue_joined.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorID": "author",
                "VenueID": "PublicationVenue"})


            # BOOKCHAPTERTABLE
            book_chapters = publications.query("type == 'book-chapter'")

            pub_joined_bc = merge(publication_ids, book_chapters, left_on="id", right_on="id")

            cit_joined_bc = merge(pub_joined_bc, cite1_df, left_on="id", right_on="doi")
            cit_joined_bc = cit_joined_bc[
                ["PublicationID", "publication_year", "title", "internalID", "doi", "chapter"]]

            cit_joined_bc = cit_joined_bc.drop_duplicates()

            authors_joined_bc = merge(cit_joined_bc, unione, left_on="doi", right_on="doi_single_authors")
            authors_joined_bc = authors_joined_bc.drop_duplicates()
            authors_joined_bc = authors_joined_bc[
                ["PublicationID", "publication_year", "title", "internalID", "doi", "AuthorID", "chapter"]]


            venue_joined_bc = merge(authors_joined_bc, venueID_df, left_on="doi", right_on="doiordinati")
            venue_joined_bc = venue_joined_bc.drop_duplicates()
            venue_joined_bc = venue_joined_bc[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorID", "VenueID", "chapter"]]


            BookChapterTable = venue_joined_bc.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorID": "author",
                "VenueID": "PublicationVenue",
                "chapter": "chapternumber"})



            ##PROCEEDINGSPAPERTABLE

            proceeding_papers = publications.query("type == 'proceedings-paper'")


            pub_joined_pp = merge(publication_ids, proceeding_papers, left_on="id", right_on="id")

            cit_joined_pp = merge(pub_joined_pp, cite1_df, left_on="id", right_on="doi")
            cit_joined_pp = cit_joined_pp[["PublicationID", "publication_year", "title", "internalID", "doi"]]

            cit_joined_pp = cit_joined_pp.drop_duplicates()

            authors_joined_pp = merge(cit_joined_pp, unione, left_on="doi", right_on="doi_single_authors")
            authors_joined_pp = authors_joined_pp.drop_duplicates()
            authors_joined_pp = authors_joined_pp[
                ["PublicationID", "publication_year", "title", "internalID", "doi", "AuthorID"]]


            venue_joined_pp = merge(authors_joined_pp, venueID_df, left_on="doi", right_on="doiordinati")
            venue_joined_pp = venue_joined_pp.drop_duplicates()
            venue_joined_pp = venue_joined_pp[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorID", "VenueID"]]


            ProceedingsPapersTable = venue_joined_pp.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorID": "author",
                "VenueID": "PublicationVenue"})



            ##Check if db exists and insert data using queries
            ##ADDING TO DATABASE

            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    con.commit()
                with connect(self.getDbPath()) as con:
                    #If it already exists then we append rest of the data but we replace
                    #for publicationsid, all publications and venue tables because we load their data by
                    # queries and then add on to that data again.
                    OrganizationTable.to_sql("Organization", con, if_exists="append", index=False)
                    OrgId_table.to_sql("OrgID", con, if_exists="append", index=False)
                    PersonTable.to_sql("Person", con, if_exists="append", index=False)
                    PersonID.to_sql("PersonID", con, if_exists="append", index=False)
                    Authors.to_sql("Author", con, if_exists="append", index=False)
                    CitesTable.to_sql("Cites", con, if_exists="append", index=False)
                    publication_ids.to_sql("PubID", con, if_exists="replace", index=False)
                    VenueID.to_sql("VenueID", con, if_exists="append", index=False)
                    BookTable.to_sql("Book", con, if_exists="replace", index=False)
                    BookChapterTable.to_sql("BookChapter", con, if_exists="replace", index=False)
                    JournalTable.to_sql("Journal", con, if_exists="replace", index=False)
                    JournalArticleTable.to_sql("JournalArticle", con, if_exists="replace", index=False)
                    ProceedingTable.to_sql("Proceeding", con, if_exists="replace", index=False)
                    ProceedingsPapersTable.to_sql("ProceedingPaper", con, if_exists="replace", index=False)

            else:
                with connect(self.getDbPath()) as con:
                    con.commit()
                with connect(self.getDbPath()) as con:
                    OrganizationTable.to_sql("Organization", con, if_exists="replace", index=False)
                    OrgId_table.to_sql("OrgID", con, if_exists="replace", index=False)
                    PersonTable.to_sql("Person", con, if_exists="replace", index=False)
                    PersonID.to_sql("PersonID", con, if_exists="replace", index=False)
                    Authors.to_sql("Author", con, if_exists="replace", index=False)
                    CitesTable.to_sql("Cites", con, if_exists="replace", index=False)
                    publication_ids.to_sql("PubID", con, if_exists="replace", index=False)
                    VenueID.to_sql("VenueID", con, if_exists="replace", index=False)
                    BookTable.to_sql("Book", con, if_exists="replace", index=False)
                    BookChapterTable.to_sql("BookChapter", con, if_exists="replace", index=False)
                    JournalTable.to_sql("Journal", con, if_exists="replace", index=False)
                    JournalArticleTable.to_sql("JournalArticle", con, if_exists="replace", index=False)
                    ProceedingTable.to_sql("Proceeding", con, if_exists="replace", index=False)
                    ProceedingsPapersTable.to_sql("ProceedingPaper", con, if_exists="replace", index=False)


        if path.split(".")[1] == 'csv':

            #reading csv
            publications = read_csv(path,
                        keep_default_na=False,
                        dtype={
                            "id": "string",
                            "title": "string",
                            "type": "string",
                            "publication_year": "int",
                            "issue":"string",
                            "volume":"string",
                            "chapter":"string",
                            "publication_venue": "string",
                            "venue_type":"string",
                            "publisher":"string",
                            "event": "string"
                        })

            ##getting existing tables one by one and if they dont exist then we create empty ones
            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:

                    query = """
                    SELECT VenueID, id
                       FROM VenueID
                    """
                    VenueID = read_sql(query, con)

            else:
                VenueID= pd.DataFrame({'VenueID': pd.Series(dtype='str'),
                                        'id': pd.Series(dtype='str')
                                        })

            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """
                    SELECT person_internal_id, id
                       FROM PersonID
                    """
                    PersonID = read_sql(query, con)

            else:
                PersonID= pd.DataFrame({'person_internal_id': pd.Series(dtype='str'),
                                        'id': pd.Series(dtype='str')
                                        })

            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """
                    SELECT internalID, given_name, family_name
                       FROM Person
                    """
                    PersonTable = read_sql(query, con)
                    #
            else:
                PersonTable= pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                        'given_name': pd.Series(dtype='str'), 
                                        'family_name': pd.Series(dtype='str')
                                        })

            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """
                    SELECT AuthorID, person_internal_id
                       FROM Author
                    """
                    Authors = read_sql(query, con)

            else:
                Authors= pd.DataFrame({'AuthorID': pd.Series(dtype='str'),
                                        'person_internal_id': pd.Series(dtype='str')
                                        })
            
            ##PUBBLICATIONSID
            publication_ids = publications[["id"]]


            # Create a new column with internal identifiers for each publication
            publication_internal_id = []

            if os.path.exists(self.getDbPath()):
                #Getting count of publications if they exist
                with connect(self.getDbPath()) as con:

                    query = """
                            SELECT count(*) From PubId;                                         
                            """
                    pub_count = read_sql(query, con)
                    pub_count = pub_count.values.tolist()[0][0] + 1

            else:
                pub_count=0

            for idx, row in publications.iterrows():
                publication_internal_id.append("publication-" + str(idx+pub_count))



            publication_ids.insert(0, "PublicationID", Series(publication_internal_id, dtype="string"))


            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """
                    SELECT internalID, name
                       FROM Organization
                    """
                    OrganizationTable = read_sql(query, con)

            else:
                OrganizationTable = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                        'name': pd.Series(dtype='str')
                                        })
            
            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """
                    SELECT OrgID, id
                       FROM OrgID
                    """
                    OrgId_table = read_sql(query, con)

            else:
                OrgId_table = pd.DataFrame({'OrgID': pd.Series(dtype='str'),
                                        'id': pd.Series(dtype='str')
                                        })
            
            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """
                    SELECT internalID, PublicationID
                       FROM Cites
                    """
                    CitesTable = read_sql(query, con)

            else:
                CitesTable = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                        'PublicationID': pd.Series(dtype='str')
                                        })

            #For each of the venues, we get count of them if they already exist and then add on to them. We also link them to
            # organizations if they exist. If they organization does not exist we leave the publisherid so that it can be linked
            #with organizations the next time they want to be joined.
            ##JOURNALTABLE

            journals = publications.query("venue_type == 'journal'")
            journals=journals.reset_index()

            journals_df = journals[["publication_venue", "id", "publisher"]]

            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query= """
                    Select count(internalID) from Journal;
                    
                    """
                    ven_ids_count = read_sql(query, con)

                    ven_ids_count = ven_ids_count.values.tolist()[0][0] + 1
            else:
                ven_ids_count =0

            lst1=[]
            lst2=[]

            for idx,row in journals.iterrows():
                lst1.append("venue-" + str(idx+ven_ids_count))
                lst2.append(row["id"])
                last_venue=idx+ven_ids_count


            ven_ids_journal = DataFrame({
                "VenueID": Series(lst1, dtype="string", name="VenueID"),
                "doi": Series(lst2, dtype="string", name="doi")
            })

            journal1 = merge(journals_df, ven_ids_journal, left_on="id", right_on="doi",how="left")
            journalfinal = merge(journal1, OrgId_table, left_on="publisher", right_on="id", how="left")

            if len(OrgId_table)==0:
                JournalTable = journalfinal[["VenueID", "publication_venue", "publisher"]]
                JournalTable = JournalTable.rename(columns={"VenueID": "internalID"})
                JournalTable = JournalTable.rename(columns={"publisher": "Publisher"})
                JournalTable = JournalTable.rename(columns={"publication_venue": "title"})
            else:
                JournalTable = journalfinal[["VenueID", "publication_venue", "OrgID"]]
                JournalTable = JournalTable.rename(columns={"VenueID": "internalID"})
                JournalTable = JournalTable.rename(columns={"OrgID": "Publisher"})
                JournalTable = JournalTable.rename(columns={"publication_venue": "title"})



            ##Book Table

            books = publications.query("venue_type == 'book'")
            books = books.reset_index()

            books_df =  books[["publication_venue", "id", "publisher" ]]

            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """
                                Select count(internalID) from Book;

                                """
                    ven_ids_count2 = read_sql(query, con)

                    ven_ids_count2 = ven_ids_count2.values.tolist()[0][0] + 1
            else:
                ven_ids_count2 = 0

            lst1 = []
            lst2 = []


            for idx, row in books.iterrows():
                lst1.append("venue-" + str(idx+ ven_ids_count2+last_venue))
                lst2.append(row["id"])
                last_last_venue=idx+ ven_ids_count2+last_venue+1


            ven_ids_book = DataFrame({
                "VenueID": Series(lst1, dtype="string", name="VenueID"),
                "doi": Series(lst2, dtype="string", name="doi")
            })
            book1 = merge(books_df, ven_ids_book, left_on="id", right_on="doi", how="left")
            bookfinal = merge(book1, OrgId_table, left_on="publisher", right_on="id", how="left")
            if len(OrgId_table) == 0:

                BookTable = bookfinal[["VenueID", "publication_venue", "publisher"]]
                BookTable = BookTable.rename(columns={"VenueID": "internalID"})
                BookTable = BookTable.rename(columns={"OrgID": "Publisher"})
                BookTable = BookTable.rename(columns={"publication_venue": "title"})
            else:
                BookTable = bookfinal[["VenueID", "publication_venue", "OrgID"]]
                BookTable = BookTable.rename(columns={"VenueID": "internalID"})
                BookTable = BookTable.rename(columns={"OrgID": "Publisher"})
                BookTable = BookTable.rename(columns={"publication_venue": "title"})


            ##Proceeding Table

            proceedings = publications.query("venue_type == 'proceedings'")

            proceedings_df =  proceedings[["publication_venue", "id", "publisher", "event" ]]

            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """
                              Select count(internalID) from Proceeding;
                             """
                    ven_ids_count3 = read_sql(query, con)
                    # print(ven_ids)
                    ven_ids_count3 = ven_ids_count3.values.tolist()[0][0] + 1
            else:
                ven_ids_count3 = 0

            lst1 = []
            lst2 = []


            for idx, row in proceedings.iterrows():
                lst1.append("venue-" + str(idx + ven_ids_count2 + last_last_venue ))
                lst2.append(row["id"])

            ven_ids_pp = DataFrame({
                "VenueID": Series(lst1, dtype="string", name="VenueID"),
                "doi": Series(lst2, dtype="string", name="doi")
            })


            proceeding1 = merge(proceedings_df, ven_ids_pp, left_on="id", right_on="doi",how="left")
            proceedingfinal = merge(proceeding1, OrgId_table, left_on="publisher", right_on="id", how="left")


            if len(OrgId_table) == 0:
                ProceedingTable = proceedingfinal[["VenueID", "publication_venue", "publisher", "event"]]
                ProceedingTable = ProceedingTable.rename(columns={"VenueID": "internalID"})
                ProceedingTable = ProceedingTable.rename(columns={"OrgID": "Publisher"})
                ProceedingTable = ProceedingTable.rename(columns={"publication_venue": "title"})
            else:
                ProceedingTable = proceedingfinal[["VenueID", "publication_venue", "OrgID", "event"]]
                ProceedingTable = ProceedingTable.rename(columns={"VenueID": "internalID"})
                ProceedingTable = ProceedingTable.rename(columns={"OrgID": "Publisher"})
                ProceedingTable = ProceedingTable.rename(columns={"publication_venue": "title"})




            #making Authors Table. We check all the Authors if they exist already. If they are not linked with a publication then
            #Author id will be a doi. Otherwise it will be a author internal id (which we create in this step when we merge with
            #publication data). We combine both merged and unmerged authorsid df data and save it to database
            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """select * from Author """
                    a = read_sql(query, con)
            else:
                a = pd.DataFrame({'AuthorID': pd.Series(dtype='str'),
                                  'person_internal_id': pd.Series(dtype='str')
                                  })


            test_df = merge(publications, a, left_on="id", right_on="AuthorID", how="left")
            nan_Authors = test_df[test_df['AuthorID'].isna()]
            nan_Authors = nan_Authors[['AuthorID', 'person_internal_id']]
            notnan_Authors = test_df[test_df['AuthorID'].notna()]
            notnan_Authors = notnan_Authors[['AuthorID', 'person_internal_id']]


            pubs_dict = (publication_ids.set_index('id').T.to_dict('list'))
            lst1 = []
            lst2 = []
            lst3 = []
            for idx, row in notnan_Authors.iterrows():
                if pubs_dict.get(row["AuthorID"], None) != None:
                    lst1.append(row["AuthorID"])
                    lst2.append(row["person_internal_id"])
                    pub = pubs_dict[row["AuthorID"]][0]
                    id = pub.split("-")[1]
                    auth = "authors-" + id
                    lst3.append(auth)

            newAuth = DataFrame({
                "doi": Series(lst1, dtype="string", name="doi"),
                "person_internal_id": Series(lst2, dtype="string", name="person_internal_id"),
                "AuthorsID": Series(lst3, dtype="string", name="AuthorsID")
            })

            Auth_f = newAuth[["AuthorsID","person_internal_id"]]
            Auth_f=Auth_f.rename(columns={"AuthorsID":"AuthorID"})
            #combining both merged and unmerged authors
            Authors = pd.concat([Auth_f, nan_Authors])
            Authors = Authors[Authors['AuthorID'].notna()]

            #For each of the publications, it joins with authors information, if they exist and also joins with the venues
            #that we just created above
            ##JOURNALARCTICLETABLE
            journal_articles = publications.query("type == 'journal-article'")

            pub_joined = merge(publication_ids, journal_articles, left_on="id", right_on="id")
            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query="""
                    select JournalArticle.cites as internalID, PubID.id as doi from JournalArticle join PubID on 
                    JournalArticle.internalID = PubID.PublicationID
                    """
                    j = read_sql(query, con)
            else:
                j = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                           'doi': pd.Series(dtype='str')
                                           })

            cit_joined = merge(pub_joined, j, left_on="id", right_on="doi",how="left")
            cit_joined = cit_joined[
                 ["PublicationID", "publication_year", "title", "internalID", "issue", "volume", "id"]]
            cit_joined = cit_joined.drop_duplicates()


            new_join =  merge(cit_joined, newAuth, left_on="id", right_on="doi",how="left")
            new_join = new_join[["PublicationID", "publication_year", "title", "internalID", "issue", "volume", "id", "AuthorsID"]]
            authors_joined = new_join.drop_duplicates()

            v=ven_ids_journal.drop_duplicates()
            v = v.rename(columns={
                "doi": "doi_v"})
            venue_joined = merge(authors_joined, v, left_on="id", right_on="doi_v",how="left")
            venue_joined = venue_joined.drop_duplicates()
            venue_joined = venue_joined[
                 ["PublicationID", "publication_year", "title", "internalID", "AuthorsID", "VenueID", "issue", "volume"]]

            JournalArticleTable = venue_joined.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorsID": "author",
                "VenueID": "PublicationVenue"})



            ###BOOKCHAPTERTABLE

            book_chapters = publications.query("type == 'book-chapter'")
            pub_joined_bc = merge(publication_ids, book_chapters, left_on="id", right_on="id")

            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """
                                select BookChapter.cites as internalID, PubID.id as doi from BookChapter join PubID on 
                                BookChapter.internalID = PubID.PublicationID
                                """
                    j_1 = read_sql(query, con)
            else:
                j_1 = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                  'doi': pd.Series(dtype='str')
                                  })


            cit_joined_bc= merge(pub_joined_bc, j_1, left_on="id", right_on="doi", how="left")

            cit_joined_bc = cit_joined_bc[
                ["PublicationID", "publication_year", "title", "internalID", "chapter", "id"]]

            cit_joined_bc = cit_joined_bc.drop_duplicates()


            new_join_bc = merge(cit_joined_bc, newAuth, left_on="id", right_on="doi", how="left")
            new_join_bc = new_join_bc[
                ["PublicationID", "publication_year", "title", "internalID", "chapter", "id", "AuthorsID"]]
            authors_joined_bc = new_join_bc.drop_duplicates()

            v_1 = ven_ids_book.drop_duplicates()
            v_1 = v_1.rename(columns={
                "doi": "doi_v"})

            venue_joined_bc = merge(authors_joined_bc, v_1, left_on="id", right_on="doi_v", how="left")
            venue_joined_bc = venue_joined_bc.drop_duplicates()
            venue_joined_bc = venue_joined_bc[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorsID", "VenueID","chapter"]]


            BookChapterTable = venue_joined_bc.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorsID": "author",
                "VenueID": "PublicationVenue",
                "chapter": "chapternumber"
            })


            ###PROCEEDINGSPAPERTABLE

            proceeding_papers = publications.query("type == 'proceedings-paper'")
            pub_joined_pp = merge(publication_ids, proceeding_papers, left_on="id", right_on="id")

            if os.path.exists(self.getDbPath()):
                with connect(self.getDbPath()) as con:
                    query = """
                                            select ProceedingPaper.cites as internalID, PubID.id as doi 
                                            from ProceedingPaper join PubID on 
                                            ProceedingPaper.internalID = PubID.PublicationID
                                            """
                    j_2 = read_sql(query, con)
            else:
                j_2 = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                    'doi': pd.Series(dtype='str')
                                    })

            cit_joined_pp = merge(pub_joined_pp, j_2, left_on="id", right_on="doi", how="left")

            cit_joined_pp = cit_joined_pp[
                ["PublicationID", "publication_year", "title", "internalID", "id"]]

            cit_joined_pp = cit_joined_pp.drop_duplicates()

            new_join_pp = merge(cit_joined_pp, newAuth, left_on="id", right_on="doi", how="left")
            new_join_pp = new_join_pp[
                ["PublicationID", "publication_year", "title", "internalID",  "id", "AuthorsID"]]
            authors_joined_pp = new_join_pp.drop_duplicates()
            v_2 = ven_ids_pp.drop_duplicates()
            v_2 = v_2.rename(columns={
                "doi": "doi_v"})

            venue_joined_pp = merge(authors_joined_pp, v_2, left_on="id", right_on="doi_v", how="left")
            venue_joined_pp = venue_joined_pp.drop_duplicates()
            venue_joined_pp = venue_joined_pp[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorsID", "VenueID"]]

            ProceedingsPapersTable = venue_joined_pp.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorsID": "author",
                "VenueID": "PublicationVenue"
            })


            ##Check if db exists and insert data using queries
            ##ADDING TO DATABASE

            if os.path.exists(self.getDbPath()):

                with connect(self.getDbPath()) as con:
                    con.commit()
                with connect(self.getDbPath()) as con:
                    #It repalces the tables related to organization, person,  cites and venues because we query them
                    # to get already existing data. For the ones that
                    # we got information about in csv we append them (publication and venue data)
                    OrganizationTable.to_sql("Organization", con, if_exists="replace", index=False)
                    OrgId_table.to_sql("OrgID", con, if_exists="replace", index=False)
                    PersonTable.to_sql("Person", con, if_exists="replace", index=False)
                    PersonID.to_sql("PersonID", con, if_exists="replace", index=False)
                    Authors.to_sql("Author", con, if_exists="replace", index=False)
                    CitesTable.to_sql("Cites", con, if_exists="replace", index=False)
                    publication_ids.to_sql("PubID", con, if_exists="append", index=False)
                    VenueID.to_sql("VenueID", con, if_exists="replace", index=False)
                    BookTable.to_sql("Book", con, if_exists="append", index=False)
                    BookChapterTable.to_sql("BookChapter", con, if_exists="append", index=False)
                    JournalTable.to_sql("Journal", con, if_exists="append", index=False)
                    JournalArticleTable.to_sql("JournalArticle", con, if_exists="append", index=False)
                    ProceedingTable.to_sql("Proceeding", con, if_exists="append", index=False)
                    ProceedingsPapersTable.to_sql("ProceedingPaper", con, if_exists="append", index=False)

            else:
                with connect(self.getDbPath()) as con:
                    con.commit()
                with connect(self.getDbPath()) as con:
                    OrganizationTable.to_sql("Organization", con, if_exists="replace", index=False)
                    OrgId_table.to_sql("OrgID", con, if_exists="replace", index=False)
                    PersonTable.to_sql("Person", con, if_exists="replace", index=False)
                    PersonID.to_sql("PersonID", con, if_exists="replace", index=False)
                    Authors.to_sql("Author", con, if_exists="replace", index=False)
                    CitesTable.to_sql("Cites", con, if_exists="replace", index=False)
                    publication_ids.to_sql("PubID", con, if_exists="replace", index=False)
                    VenueID.to_sql("VenueID", con, if_exists="replace", index=False)
                    BookTable.to_sql("Book", con, if_exists="replace", index=False)
                    BookChapterTable.to_sql("BookChapter", con, if_exists="replace", index=False)
                    JournalTable.to_sql("Journal", con, if_exists="replace", index=False)
                    JournalArticleTable.to_sql("JournalArticle", con, if_exists="replace", index=False)
                    ProceedingTable.to_sql("Proceeding", con, if_exists="replace", index=False)
                    ProceedingsPapersTable.to_sql("ProceedingPaper", con, if_exists="replace", index=False)

        return True
#There is a recursive and dynamic function in both reletional and triplestore query processor
# that takes in doi, list s, and dictionary dic as input.
#It makes a publication object on the the doi and then recursively calls itself
# for all the citations of that publication until it reaches the base case.
class RelationalQueryProcessor(RelationalProcessor):
    def __init__(self):
        super().__init__()

    def getPublicationsPublishedInYear(self, year):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume, 'NA' as chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Journal.title, 'NA' as event, Journal.Publisher, OrgID.id, Organization.name, Pub2.id
                                FROM JournalArticle 
                                LEFT JOIN Cites ON JournalArticle.cites == Cites.internalID
                                LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                                Left JOIN PubID ON JournalArticle.internalID == PubID.PublicationID

                                LEFT JOIN VenueID ON JournalArticle.PublicationVenue == VenueID.VenueID
                                LEFT JOIN Author ON JournalArticle.author == Author.AuthorID
                                LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                                LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                                LEFT JOIN Journal  ON JournalArticle.PublicationVenue == Journal.internalID
                                LEFT JOIN Organization ON Journal.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                WHERE publication_year= """ + str(year) + """
                                UNION
                               SELECT BookChapter.internalID, PubID.id, BookChapter.title, publication_year,'NA' as issue,'NA' as volume, chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Book.title, 'NA' as event, Book.Publisher, OrgID.id, Organization.name, Pub2.id
                                FROM BookChapter 
                                LEFT JOIN Cites ON BookChapter.cites == Cites.internalID
                                LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                                Left JOIN PubID ON BookChapter.internalID == PubID.PublicationID

                                LEFT JOIN VenueID ON BookChapter.PublicationVenue == VenueID.VenueID
                                LEFT JOIN Author ON BookChapter.author == Author.AuthorID
                                LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                                LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                                LEFT JOIN Book  ON BookChapter.PublicationVenue == Book.internalID
                                LEFT JOIN Organization ON Book.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                WHERE publication_year= """ + str(year) + """ 
                               UNION
                               SELECT ProceedingPaper.internalID, PubID.id, ProceedingPaper.title, publication_year,'NA' as issue,'NA' as volume,'NA' as chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Proceeding.title, event, Proceeding.Publisher, OrgID.id, Organization.name, Pub2.id
                                FROM ProceedingPaper 
                                LEFT JOIN Cites ON ProceedingPaper.cites == Cites.internalID
                                LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                                Left JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID

                                LEFT JOIN VenueID ON ProceedingPaper.PublicationVenue == VenueID.VenueID
                                LEFT JOIN Author ON ProceedingPaper.author == Author.AuthorID
                                LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                                LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                                LEFT JOIN Proceeding  ON ProceedingPaper.PublicationVenue == Proceeding.internalID
                                LEFT JOIN Organization ON Proceeding.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                WHERE publication_year= """ + str(year) + """ 

                                """
            df_sql = read_sql(query, con)
            return df_sql

    def getPublicationsByAuthorId(self, id):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume, 'NA' as chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Journal.title, 'NA' as event, Journal.Publisher, OrgID.id, Organization.name, Pub2.id
                            FROM JournalArticle LEFT JOIN Author ON JournalArticle.author ==Author.AuthorID
                            LEFT JOIN Cites ON JournalArticle.cites == Cites.internalID
                            LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                            Left JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                            LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                            LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                            LEFT JOIN VenueID ON JournalArticle.PublicationVenue == VenueID.VenueID
                            LEFT JOIN Journal  ON JournalArticle.PublicationVenue == Journal.internalID
                                LEFT JOIN Organization ON Journal.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                            WHERE PersonID.id='""" + id + """'
                            UNION
                            SELECT BookChapter.internalID, PubID.id, BookChapter.title, publication_year,'NA' as issue,'NA' as volume, chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Book.title, 'NA' as event, Book.Publisher, OrgID.id, Organization.name, Pub2.id
                            FROM BookChapter LEFT JOIN Author ON BookChapter.author ==Author.AuthorID
                            LEFT JOIN Cites ON BookChapter.cites == Cites.internalID
                            LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                            Left JOIN PubID ON BookChapter.internalID == PubID.PublicationID

                            LEFT JOIN PersonID ON  Author.person_internal_id ==PersonID.person_internal_id
                            LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                            LEFT JOIN VenueID ON BookChapter.PublicationVenue == VenueID.VenueID
                            LEFT JOIN Book  ON BookChapter.PublicationVenue == Book.internalID
                                LEFT JOIN Organization ON Book.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                            WHERE PersonID.id='""" + id + """'
                            UNION
                            SELECT ProceedingPaper.internalID, PubID.id, ProceedingPaper.title, publication_year,'NA' as issue,'NA' as volume,'NA' as chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Proceeding.title, event, Proceeding.Publisher, OrgID.id, Organization.name, Pub2.id
                            FROM ProceedingPaper LEFT JOIN Author ON ProceedingPaper.author ==Author.AuthorID

                             LEFT JOIN Cites ON ProceedingPaper.cites == Cites.internalID
                            LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                            Left JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID
                            LEFT JOIN PersonID ON  Author.person_internal_id ==PersonID.person_internal_id
                            LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                            LEFT JOIN VenueID ON ProceedingPaper.PublicationVenue == VenueID.VenueID
                            LEFT JOIN Proceeding  ON ProceedingPaper.PublicationVenue == Proceeding.internalID
                                LEFT JOIN Organization ON Proceeding.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                            WHERE PersonID.id='""" + id + """' ;  
                         """
            df_sql = read_sql(query, con)
            return df_sql

    def getMostCitedPublication(self):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume, 'NA' as chapternumber, author, PersonID.id, given_name, family_name, PublicationVenue, VenueID.id, Journal.title, 'NA' as event, Journal.Publisher, OrgID.id, Organization.name, Pub2.id
                               FROM JournalArticle JOIN (SELECT PublicationID  FROM Cites 
                                                            GROUP BY PublicationID
                                                            ORDER BY COUNT (*) DESC
                                                            LIMIT 1) pub 
                               ON JournalArticle.internalID == pub.PublicationID
                               LEFT JOIN PubID ON pub.PublicationID == PubID.PublicationID
                               LEFT JOIN Author ON JournalArticle.author == Author.AuthorID
                               LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                               LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                               LEFT JOIN Journal  ON JournalArticle.PublicationVenue == Journal.internalID
                               LEFT JOIN VenueID ON Journal.internalID == VenueID.VenueID
                               LEFT JOIN Organization ON Journal.Publisher == Organization.internalID 
                               LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                               LEFT JOIN Cites ON JournalArticle.cites == Cites.internalID
                               LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                               UNION 
                               SELECT BookChapter.internalID, PubID.id, BookChapter.title, publication_year,'NA' as issue,'NA' as volume, chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Book.title, 'NA' as event, Book.Publisher, OrgID.id, Organization.name, Pub2.id
                               FROM BookChapter JOIN (SELECT PublicationID  FROM Cites 
                                                            GROUP BY PublicationID
                                                            ORDER BY COUNT (*) DESC
                                                            LIMIT 1) pub 
                               ON BookChapter.internalID == pub.PublicationID
                               LEFT JOIN PubID ON pub.PublicationID == PubID.PublicationID
                               LEFT JOIN Author ON BookChapter.author == Author.AuthorID
                               LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                               LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                               LEFT JOIN Book  ON BookChapter.PublicationVenue == Book.internalID
                               LEFT JOIN VenueID ON Book.internalID == VenueID.VenueID
                               LEFT JOIN Organization ON Book.Publisher == Organization.internalID 
                               LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                               LEFT JOIN Cites ON BookChapter.cites == Cites.internalID
                               LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                               UNION
                               SELECT ProceedingPaper.internalID, PubID.id, ProceedingPaper.title, publication_year,'NA' as issue,'NA' as volume,'NA' as chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Proceeding.title, event, Proceeding.Publisher, OrgID.id, Organization.name, Pub2.id 
                               FROM ProceedingPaper JOIN (SELECT PublicationID  FROM Cites 
                                                            GROUP BY PublicationID
                                                            ORDER BY COUNT (*) DESC
                                                            LIMIT 1) pub 
                               ON ProceedingPaper.internalID == pub.PublicationID 
                               LEFT JOIN PubID ON pub.PublicationID == PubID.PublicationID
                               LEFT JOIN Author ON ProceedingPaper.author == Author.AuthorID
                               LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                               LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                               LEFT JOIN Proceeding  ON ProceedingPaper.PublicationVenue == Proceeding.internalID
                               LEFT JOIN VenueID ON Proceeding.internalID == VenueID.VenueID
                               LEFT JOIN Organization ON Proceeding.Publisher == Organization.internalID 
                               LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                               LEFT JOIN Cites ON ProceedingPaper.cites == Cites.internalID
                               LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID ; 
                            """
            df_sql = read_sql(query, con)
            return df_sql

    def getMostCitedVenue(self):
        with connect(self.getDbPath()) as con:
            query = """SELECT Journal.internalID, VenueID.id, Journal.title, Publisher, OrgID.id, Organization.name ,'NA' as event
                               FROM JournalArticle JOIN (SELECT PublicationID  FROM Cites 
                                                            GROUP BY PublicationID
                                                            ORDER BY COUNT (*) DESC
                                                            LIMIT 1) pub 
                               ON JournalArticle.internalID == pub.PublicationID 
                               LEFT JOIN Journal ON JournalArticle.PublicationVenue == Journal.internalID
                               LEFT JOIN VenueID ON Journal.internalID == VenueID.VenueID 
                               LEFT JOIN Organization ON Journal.Publisher == Organization.InternalID
                               LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID 
                               UNION
                               SELECT Book.internalID, VenueID.id, Book.title, Publisher, OrgID.id, Organization.name, 'NA' as event
                               FROM BookChapter JOIN (SELECT PublicationID  FROM Cites 
                                                            GROUP BY PublicationID
                                                            ORDER BY COUNT (*) DESC
                                                            LIMIT 1) pub 
                               ON BookChapter.internalID == pub.PublicationID 
                               LEFT JOIN Book ON BookChapter.PublicationVenue == Book.internalID
                               LEFT JOIN VenueID ON Book.internalID == VenueID.VenueID 
                               LEFT JOIN Organization ON Book.Publisher == Organization.InternalID
                               LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID 
                               UNION
                               SELECT Proceeding.internalID, VenueID.id, Proceeding.title, Publisher, OrgID.id, Organization.name, Proceeding.event
                               FROM ProceedingPaper JOIN (SELECT PublicationID  FROM Cites 
                                                            GROUP BY PublicationID
                                                            ORDER BY COUNT (*) DESC
                                                            LIMIT 1) pub 
                               ON ProceedingPaper.internalID == pub.PublicationID 
                               LEFT JOIN Proceeding ON ProceedingPaper.PublicationVenue == Proceeding.internalID
                               LEFT JOIN VenueID ON Proceeding.internalID == VenueID.VenueID 
                               LEFT JOIN Organization ON Proceeding.Publisher == Organization.InternalID
                               LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID    
                            """
            df_sql = read_sql(query, con)
            return df_sql

    def getVenuesByPublisherId(self, id):
        with connect(self.getDbPath()) as con:
            query = """SELECT Journal.internalID, VenueID.id, title, Publisher, OrgID.id, Organization.name, 'NA' as event
                                FROM Journal LEFT JOIN Organization ON Journal.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                LEFT JOIN VenueID ON Journal.internalID == VenueID.VenueID
                                WHERE OrgID.id= '""" + id + """'
                                UNION
                                SELECT Book.internalID, VenueID.id, title, Publisher, OrgID.id, Organization.name , 'NA' as event
                                FROM Book LEFT JOIN Organization ON Book.Publisher == Organization.internalID
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                LEFT JOIN VenueID ON Book.internalID == VenueID.VenueID
                                WHERE OrgID.id='""" + id + """'
                                UNION
                                SELECT Proceeding.internalID, VenueID.id, title, Publisher, OrgID.id, Organization.name, Proceeding.event
                                FROM Proceeding LEFT JOIN Organization ON Proceeding.Publisher == Organization.internalID
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                LEFT JOIN VenueID ON Proceeding.internalID == VenueID.VenueID
                                WHERE OrgID.id='""" + id + """' 
                             """
            df_sql = read_sql(query, con)
            return df_sql

    def getPublicationInVenue(self, id):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume, 'NA' as chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Journal.title, 'NA' as event, Journal.Publisher, OrgID.id, Organization.name, Pub2.id
                                FROM JournalArticle 
                                LEFT JOIN Cites ON JournalArticle.cites == Cites.internalID
                                LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                                Left JOIN PubID ON JournalArticle.internalID == PubID.PublicationID

                                LEFT JOIN VenueID ON JournalArticle.PublicationVenue == VenueID.VenueID
                                LEFT JOIN Author ON JournalArticle.author == Author.AuthorID
                                LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                                LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                                LEFT JOIN Journal  ON JournalArticle.PublicationVenue == Journal.internalID
                                LEFT JOIN Organization ON Journal.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                            WHERE VenueID.id='""" + id + """'
                            UNION
                            SELECT BookChapter.internalID, PubID.id, BookChapter.title, publication_year,'NA' as issue,'NA' as volume, chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Book.title, 'NA' as event, Book.Publisher, OrgID.id, Organization.name, Pub2.id
                                FROM BookChapter 
                                LEFT JOIN Cites ON BookChapter.cites == Cites.internalID
                                LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                                Left JOIN PubID ON BookChapter.internalID == PubID.PublicationID

                                LEFT JOIN VenueID ON BookChapter.PublicationVenue == VenueID.VenueID
                                LEFT JOIN Author ON BookChapter.author == Author.AuthorID
                                LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                                LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                                LEFT JOIN Book  ON BookChapter.PublicationVenue == Book.internalID
                                LEFT JOIN Organization ON Book.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                WHERE VenueID.id='""" + id + """'
                            UNION
                            SELECT ProceedingPaper.internalID, PubID.id, ProceedingPaper.title, publication_year,'NA' as issue,'NA' as volume,'NA' as chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Proceeding.title, event, Proceeding.Publisher, OrgID.id, Organization.name, Pub2.id
                                FROM ProceedingPaper 
                                LEFT JOIN Cites ON ProceedingPaper.cites == Cites.internalID
                                LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                                Left JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID

                                LEFT JOIN VenueID ON ProceedingPaper.PublicationVenue == VenueID.VenueID
                                LEFT JOIN Author ON ProceedingPaper.author == Author.AuthorID
                                LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                                LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                                LEFT JOIN Proceeding  ON ProceedingPaper.PublicationVenue == Proceeding.internalID
                                LEFT JOIN Organization ON Proceeding.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                               WHERE VenueID.id='""" + id + """'
                         """
            df_sql = read_sql(query, con)
            return df_sql

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Journal.title, Journal.Publisher, OrgID.id, Organization.name, Pub2.id
                            FROM JournalArticle LEFT JOIN Author ON JournalArticle.author ==Author.AuthorID
                            LEFT JOIN Cites ON JournalArticle.cites == Cites.internalID
                            LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                            Left JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                            LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                            LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                            LEFT JOIN VenueID ON JournalArticle.PublicationVenue == VenueID.VenueID
                            LEFT JOIN Journal  ON JournalArticle.PublicationVenue == Journal.internalID
                                LEFT JOIN Organization ON Journal.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                            WHERE VenueID.id= '""" + journalId + """'and JournalArticle.volume= """ + str(
                volume) + """ and JournalArticle.issue= """ + str(issue) + """                 
                        """
            df_sql = read_sql(query, con)
            return df_sql


    def getJournalArticlesInVolume(self, volume, journalId):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Journal.title, Journal.Publisher, OrgID.id, Organization.name, Pub2.id
                            FROM JournalArticle LEFT JOIN Author ON JournalArticle.author ==Author.AuthorID
                            LEFT JOIN Cites ON JournalArticle.cites == Cites.internalID
                            LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                            Left JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                            LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                            LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                            LEFT JOIN VenueID ON JournalArticle.PublicationVenue == VenueID.VenueID
                            LEFT JOIN Journal  ON JournalArticle.PublicationVenue == Journal.internalID
                                LEFT JOIN Organization ON Journal.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                           WHERE VenueID.id= '""" + journalId + """'and JournalArticle.volume= """ + str(volume) + """ 
                        """
            df_sql = read_sql(query, con)
            return df_sql


    def getJournalArticlesInJournal(self, journalId):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Journal.title, Journal.Publisher, OrgID.id, Organization.name, Pub2.id
                            FROM JournalArticle LEFT JOIN Author ON JournalArticle.author ==Author.AuthorID
                            LEFT JOIN Cites ON JournalArticle.cites == Cites.internalID
                            LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                            Left JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                            LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                            LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                            LEFT JOIN VenueID ON JournalArticle.PublicationVenue == VenueID.VenueID
                            LEFT JOIN Journal  ON JournalArticle.PublicationVenue == Journal.internalID
                                LEFT JOIN Organization ON Journal.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                           WHERE VenueID.id= '""" + journalId + """'                
                        """
            df_sql = read_sql(query, con)
            return df_sql


    def getProceedingsByEvent(self, eventPartialName):
        with connect(self.getDbPath()) as con:
            query = """SELECT Proceeding.internalID, VenueID.id, title, Publisher, OrgID.id, name, Proceeding.event
                               FROM Proceeding 
                               LEFT JOIN VenueID ON Proceeding.internalID == VenueID.VenueID 
                               LEFT JOIN Organization ON Proceeding.Publisher == Organization.InternalID
                               LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID  
                               WHERE lower(event) LIKE '%""" + eventPartialName + """%' ;
                            """
            df_sql = read_sql(query, con)
            return df_sql


    def getPublicationAuthors(self, publicationId):
        with connect(self.getDbPath()) as con:
            query = """SELECT  Person.internalID, PersonID.id,  given_name, family_name
                               FROM Person 
                               LEFT JOIN Author ON Person.internalID ==Author.person_internal_id
                               LEFT JOIN JournalArticle ON  Author.AuthorID ==JournalArticle.author
                               LEFT JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                               LEFT JOIN PersonID ON Person.internalID == PersonID.person_internal_id
                               WHERE PubID.id='""" + publicationId + """'
                               UNION
                               SELECT Person.internalID, PersonID.id, given_name, family_name 
                               FROM Person 
                               LEFT JOIN Author ON Person.internalID ==Author.person_internal_id
                               LEFT JOIN BookChapter ON  Author.AuthorID ==BookChapter.author
                               LEFT JOIN PubID ON BookChapter.internalID == PubID.PublicationID
                               LEFT JOIN PersonID ON Person.internalID == PersonID.person_internal_id
                               WHERE PubID.id='""" + publicationId + """'
                               UNION
                               SELECT Person.internalID, PersonID.id,  given_name, family_name
                               FROM Person 
                               LEFT JOIN Author ON Person.internalID ==Author.person_internal_id
                               LEFT JOIN ProceedingPaper ON  Author.AuthorID ==ProceedingPaper.author
                               LEFT JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID
                               LEFT JOIN PersonID ON Person.internalID == PersonID.person_internal_id
                               WHERE PubID.id='""" + publicationId + """'

                            """
            df_sql = read_sql(query, con)
            return df_sql


    def getPublicationsByAuthorsName(self, authorPartialName):
        with connect(self.getDbPath()) as con:
            query = """ SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume, 'NA' as chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Journal.title, 'NA' as event, Journal.Publisher, OrgID.id, Organization.name, Pub2.id
                                FROM JournalArticle 
                                LEFT JOIN Cites ON JournalArticle.cites == Cites.internalID
                                LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                                Left JOIN PubID ON JournalArticle.internalID == PubID.PublicationID

                                LEFT JOIN VenueID ON JournalArticle.PublicationVenue == VenueID.VenueID
                                LEFT JOIN Author ON JournalArticle.author == Author.AuthorID
                                LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                                LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                                LEFT JOIN Journal  ON JournalArticle.PublicationVenue == Journal.internalID
                                LEFT JOIN Organization ON Journal.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                           WHERE lower(Person.family_name) LIKE '%""" + authorPartialName + """%'  OR lower(Person.given_name) LIKE '%""" + authorPartialName + """%'
                           UNION
                           SELECT BookChapter.internalID, PubID.id, BookChapter.title, publication_year,'NA' as issue,'NA' as volume, chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Book.title, 'NA' as event, Book.Publisher, OrgID.id, Organization.name, Pub2.id
                                FROM BookChapter 
                                LEFT JOIN Cites ON BookChapter.cites == Cites.internalID
                                LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                                Left JOIN PubID ON BookChapter.internalID == PubID.PublicationID

                                LEFT JOIN VenueID ON BookChapter.PublicationVenue == VenueID.VenueID
                                LEFT JOIN Author ON BookChapter.author == Author.AuthorID
                                LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                                LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                                LEFT JOIN Book  ON BookChapter.PublicationVenue == Book.internalID
                                LEFT JOIN Organization ON Book.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                           WHERE lower(Person.family_name) LIKE '%""" + authorPartialName + """%'  OR lower(Person.given_name) LIKE '%""" + authorPartialName + """%'
                           UNION
                           SELECT ProceedingPaper.internalID, PubID.id, ProceedingPaper.title, publication_year,'NA' as issue,'NA' as volume,'NA' as chapternumber, 
                                   author, PersonID.id , given_name, family_name,  
                                   PublicationVenue, VenueID.id, Proceeding.title,Proceeding.event, Proceeding.Publisher, OrgID.id, Organization.name, Pub2.id
                                FROM ProceedingPaper 
                                LEFT JOIN Cites ON ProceedingPaper.cites == Cites.internalID
                                LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                                Left JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID

                                LEFT JOIN VenueID ON ProceedingPaper.PublicationVenue == VenueID.VenueID
                                LEFT JOIN Author ON ProceedingPaper.author == Author.AuthorID
                                LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                                LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                                LEFT JOIN Proceeding  ON ProceedingPaper.PublicationVenue == Proceeding.internalID
                                LEFT JOIN Organization ON Proceeding.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                           WHERE lower(Person.family_name) LIKE '%""" + authorPartialName + """%'  OR lower(Person.given_name) LIKE '%""" + authorPartialName + """%' ;
                        """
            df_sql = read_sql(query, con)
            return df_sql

    def getDistinctPublishersOfPublications(self, pubIdList):
        lst = []
        for item in pubIdList:
            with connect(self.getDbPath()) as con:
                query = """SELECT  Organization.internalID ,OrgID.id, Organization.name
                                FROM JournalArticle 
                                LEFT  JOIN PubID ON JournalArticle.internalID == PubID.PublicationID 
                                LEFT JOIN Journal  ON JournalArticle.PublicationVenue == Journal.internalID
                                LEFT JOIN Organization ON Journal.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                   WHERE PubID.id ='""" + item + """'
                                   UNION 
                                   SELECT Organization.internalID ,OrgID.id, Organization.name
                                FROM BookChapter 
                                LEFT  JOIN PubID ON BookChapter.internalID == PubID.PublicationID
                                LEFT JOIN Book  ON BookChapter.PublicationVenue == Book.internalID
                                LEFT JOIN Organization ON Book.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                   WHERE PubID.id ='""" + item + """'
                                   UNION 
                                   SELECT Organization.internalID ,OrgID.id, Organization.name
                                FROM ProceedingPaper 
                                LEFT  JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID
                                LEFT JOIN Proceeding  ON ProceedingPaper.PublicationVenue == Proceeding.internalID
                                LEFT JOIN Organization ON Proceeding.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                   WHERE PubID.id ='""" + item + """' ;
                                """
                df_sql = read_sql(query, con)
                lst.append(df_sql)
        final = pd.concat(lst)
        return final

    def getCitesDoi(self, doi, s, dic):
        with connect(self.getDbPath()) as con:
            if doi not in dic:
                query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume, 'NA' as chapternumber, author, PersonID.id, given_name, family_name, PublicationVenue, VenueID.id, Journal.title, 'NA' as event, Journal.Publisher, OrgID.id, Organization.name, Pub2.id
                                FROM JournalArticle 
                                LEFT JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                                LEFT JOIN Author ON JournalArticle.author == Author.AuthorID
                                LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                                LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                                LEFT JOIN Journal  ON JournalArticle.PublicationVenue == Journal.internalID
                                LEFT JOIN VenueID ON Journal.internalID == VenueID.VenueID
                                LEFT JOIN Organization ON Journal.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                LEFT JOIN Cites ON JournalArticle.cites == Cites.internalID
                                LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID 
                                WHERE PubID.id='""" + doi + """'
                                UNION
                                SELECT BookChapter.internalID, PubID.id, BookChapter.title, publication_year,'NA' as issue,'NA' as volume, chapternumber, 
                                    author, PersonID.id , given_name, family_name,  
                                    PublicationVenue, VenueID.id, Book.title, 'NA' as event, Book.Publisher, OrgID.id, Organization.name, Pub2.id
                                FROM BookChapter 
                                LEFT JOIN PubID ON BookChapter.internalID == PubID.PublicationID
                                LEFT JOIN Author ON BookChapter.author == Author.AuthorID
                                LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                                LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                                LEFT JOIN Book  ON BookChapter.PublicationVenue == Book.internalID
                                LEFT JOIN VenueID ON Book.internalID == VenueID.VenueID
                                LEFT JOIN Organization ON Book.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                LEFT JOIN Cites ON BookChapter.cites == Cites.internalID
                                LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID 
                                WHERE PubID.id='""" + doi + """'  
                                UNION
                                SELECT ProceedingPaper.internalID, PubID.id, ProceedingPaper.title, publication_year,'NA' as issue,'NA' as volume, 'NA' as chapternumber, 
                                    author, PersonID.id , given_name, family_name,  
                                    PublicationVenue, VenueID.id, Proceeding.title, event, Proceeding.Publisher, OrgID.id, Organization.name, Pub2.id
                                FROM ProceedingPaper 
                                LEFT JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID
                                LEFT JOIN Author ON ProceedingPaper.author == Author.AuthorID
                                LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                                LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                                LEFT JOIN Proceeding  ON ProceedingPaper.PublicationVenue == Proceeding.internalID
                                LEFT JOIN VenueID ON Proceeding.internalID == VenueID.VenueID
                                LEFT JOIN Organization ON Proceeding.Publisher == Organization.internalID 
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                LEFT JOIN Cites ON ProceedingPaper.cites == Cites.internalID
                                LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID 
                                WHERE PubID.id='""" + doi + """'  
                                """
                df_sql = read_sql(query, con)
                df_publications_sql = df_sql.fillna('NA')
                columns_names = ["internalID", "id", "title", "publication_year", "issue", "volume", "chapternumber",
                                 "author", "id", "given_name", "family_name", "PublicationVenue", "id", "title",
                                 "event", "Publisher", "id", "name", "citesdoi"]
                df_publications_sql.columns = columns_names

                if len(df_publications_sql) == 0:
                    return None


                s = s + list(set(df_publications_sql.citesdoi.tolist()))

                if len(s) == 0:
                    cite = 'NA'
                else:
                    cite = 'NA'
                    for item in s:
                        if item != 'NA':
                            # cite = s.pop(s.index(item))
                            cite = s[s.index(item)]

                if cite == 'NA' or doi in s:
                    df_final = df_publications_sql.fillna('NA')
                    df_final = df_final.values.tolist()
                    authors = {}
                    for i in df_final:
                        if authors.get(i[0], None) == None:
                            authors[i[0]] = [[i[7], i[8], i[9], i[10]]]
                        else:
                            if [i[7], i[8], i[9], i[10]] not in authors[i[0]]:
                                authors[i[0]].append([i[7], i[8], i[9], i[10]])

                    venue = {}
                    for i in df_final:
                        if venue.get(i[0], None) == None:
                            venue[i[0]] = [[i[11], i[12], i[13], i[14]]]
                        else:
                            if [i[11], i[12], i[13], i[14]] not in venue[i[0]]:
                                venue[i[0]].append([i[11], i[12], i[13], i[14]])

                    publisher = {}
                    for i in df_final:
                        if publisher.get(i[0], None) == None:
                            publisher[i[0]] = [[i[15], i[16], i[17]]]
                        else:
                            if [i[15], i[16], i[17]] not in publisher[i[0]]:
                                publisher[i[0]].append([i[15], i[16], i[17]])

                    pubs = {}
                    for i in df_final:
                        if pubs.get(i[0], None) == None:
                            pubs[i[0]] = [[i[1], i[2], i[3], i[4], i[5], i[6]]]
                        else:
                            if [i[1], i[2], i[3], i[4], i[5], i[6]] not in pubs[i[0]]:
                                pubs[i[0]].append([i[1], i[2], i[3], i[4], i[5], i[6]])

                    for item in pubs:
                        lst_author = []
                        lst_ven_id = []
                        for a in authors[item]:
                            lst_author.append(Person([a[1]], a[2], a[3]))

                        for b in venue[item]:
                            lst_ven_id.append(b[1])


                        publish = Organization([publisher[item][0][1]], publisher[item][0][2])
                        ven = Venue(lst_ven_id, venue[item][0][2], publish)


                        f_pub = pubs[item][0]

                        if (f_pub[3] != 'NA' or f_pub[4] != 'NA' and f_pub[5] == 'NA'):
                            dic[doi] = JournalArticle([f_pub[0]], f_pub[2], f_pub[1], ven, [], lst_author, f_pub[3],
                                                      f_pub[4])
                        elif f_pub[5] != 'NA':
                            dic[doi] = BookChapter([f_pub[0]], f_pub[2], f_pub[1], ven, [], lst_author, f_pub[5])
                        else:
                            dic[doi] = ProceedingsPaper([f_pub[0]], f_pub[2], f_pub[1], ven, [], lst_author)


                else:
                    df_final = df_publications_sql.fillna('NA')
                    df_final = df_final.values.tolist()
                    authors = {}
                    for i in df_final:
                        if authors.get(i[0], None) == None:
                            authors[i[0]] = [[i[7], i[8], i[9], i[10]]]
                        else:
                            if [i[7], i[8], i[9], i[10]] not in authors[i[0]]:
                                authors[i[0]].append([i[7], i[8], i[9], i[10]])

                    venue = {}
                    for i in df_final:
                        if venue.get(i[0], None) == None:
                            venue[i[0]] = [[i[11], i[12], i[13], i[14]]]
                        else:
                            if [i[11], i[12], i[13], i[14]] not in venue[i[0]]:
                                venue[i[0]].append([i[11], i[12], i[13], i[14]])

                    publisher = {}
                    for i in df_final:
                        if publisher.get(i[0], None) == None:
                            publisher[i[0]] = [[i[15], i[16], i[17]]]
                        else:
                            if [i[15], i[16], i[17]] not in publisher[i[0]]:
                                publisher[i[0]].append([i[15], i[16], i[17]])

                    pubs = {}
                    for i in df_final:
                        if pubs.get(i[0], None) == None:
                            pubs[i[0]] = [[i[1], i[2], i[3], i[4], i[5], i[6]]]
                        else:
                            if [i[1], i[2], i[3], i[4], i[5], i[6]] not in pubs[i[0]]:
                                pubs[i[0]].append([i[1], i[2], i[3], i[4], i[5], i[6]])

                    for item in pubs:
                        lst_author = []
                        lst_ven_id = []
                        for a in authors[item]:
                            lst_author.append(Person([a[1]], a[2], a[3]))

                        for b in venue[item]:
                            lst_ven_id.append(b[1])


                        publish = Organization([publisher[item][0][1]], publisher[item][0][2])
                        ven = Venue(lst_ven_id, venue[item][0][2], publish)


                        f_pub = pubs[item][0]

                        if (f_pub[3] != 'NA' or f_pub[4] != 'NA' and f_pub[5] == 'NA'):
                            dic[doi] = JournalArticle([f_pub[0]], f_pub[2], f_pub[1], ven,
                                                      [self.getCitesDoi(cite, s, dic)], lst_author, f_pub[3], f_pub[4])
                        elif f_pub[5] != 'NA':
                            dic[doi] = BookChapter([f_pub[0]], f_pub[2], f_pub[1], ven,
                                                   [self.getCitesDoi(cite, s, dic)], lst_author, f_pub[5])
                        else:
                            dic[doi] = ProceedingsPaper([f_pub[0]], f_pub[2], f_pub[1], ven,
                                                        [self.getCitesDoi(cite, s, dic)], lst_author)

            return dic[doi]

# TripleStore part.
#We use this to set endpoint url
class TriplestoreProcessor(object):
    def __init__(self):
        self.endpointUrl = ''

    def getEndpointUrl(self):
        return self.endpointUrl

    def setEndpointUrl(self, url):
        self.endpointUrl = url

#Here we have upload data function for triplestore.
class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self):
        super().__init__()


    #The upload data function is divided into 2 parts.
    # One is for json files and one is for csv file
    #For json file we create Organizations and Authors. For references and venuesids, it will link references to
    #publications if they already exist in the database. It will link venueids to venues if they alreaady exist in the
    # database. It also links authors and organizations to publications and venues if they exist

    #For csv files we create Publications and Venues. If authors exist, we link them to the created publications. If organizations
    # exist we link them to the created venues.


    def uploadData(self, path):
        my_graph = Graph()

        #For the resources we have just made our own resource links since this was out of the scope of our project
        # classes of resources
        JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
        BookChapter = URIRef("https://schema.org/Chapter")
        Journal = URIRef("https://schema.org/Periodical")
        Book = URIRef("https://schema.org/Book")
        ProceedingPaper = URIRef("https://schema.org/ProceedingPaper")
        Proceedings = URIRef("https://schema.org/Proceedings")
        Person = URIRef("https://schema.org/Person")
        Organization = URIRef("https://schema.org/Organization")
        VenueClass= URIRef("https://schema.org/VenueClass")

        # attributes related to classes
        identifier = URIRef("https://schema.org/identifier")
        doiId = URIRef("https://schema.org/doiId")
        givenName = URIRef("https://schema.org/givenName")
        familyName = URIRef("https://schema.org/familyName")

        publicationYear = URIRef("https://schema.org/datePublished")
        title = URIRef("https://schema.org/name")
        issue = URIRef("https://schema.org/issueNumber")
        volume = URIRef("https://schema.org/volumeNumber")
        chapterNumber = URIRef("https://schema.org/chapterNumber")
        doiPublisher =  URIRef("https://schema.org/doiPublisher")

        name = URIRef("https://schema.org/name")
        event = URIRef("https://schema.org/event")
        subclass =  URIRef("https://schema.org/subclass")


        # relations among classes
        publicationVenue = URIRef("https://schema.org/isPartOf")
        publisher = URIRef("https://schema.org/publisher")
        cites = URIRef("https://schema.org/cites")
        author = URIRef("https://schema.org/authorf")

        # This is the string defining the base URL used to defined
        # the URLs of all the resources created from the data
        base_url = "https://comp-data.github.io/res/"

        if path.split(".")[1] == 'json':

            ## PUBLISHER

            ##getting count of existing publishers
            publish_count_query = """
            SELECT (COUNT(?subj) as ?pCount)
            WHERE {
                ?subj a <https://schema.org/Organization> .
                }
            """
            publish_count = get(self.getEndpointUrl(), publish_count_query, True)
            publish_count=publish_count['pCount'].iloc[0]+1
            with open(path, "r", encoding="utf-8") as f:
                json_doc = load(f)
                pub = json_doc["publishers"]

            lst3 = []
            lst4 = []
            lst5 = []

            for key in pub:
                lst3.append(key)
                lst4.append(pub[key]["id"])
                lst5.append(pub[key]["name"])

            publisher_df = DataFrame({
                "id": Series(lst3, dtype="string", name="id"),
                "publisherID": Series(lst4, dtype="string", name="publisher"),
                "name": Series(lst5, dtype="string", name="publisherName"),

            })

            org_internal_id={}
            #creating new
            for idx, row in publisher_df.iterrows():
                local_id = "organization-" + str(idx + publish_count)
                subj = URIRef(base_url + local_id)

                my_graph.add((subj, RDF.type, Organization))
                my_graph.add((subj, name, Literal(row["name"])))
                my_graph.add((subj, identifier, Literal(row["publisherID"])))

                if row["publisherID"] in org_internal_id:
                    org_internal_id[row["publisherID"]].append(subj)
                else:
                    org_internal_id[row["publisherID"]] = [subj]

            ## AUTHORS
            #getting count of existing authors
            author_count_query = """
            SELECT (COUNT(?subj) as ?pCount)
            WHERE {
                ?subj a <https://schema.org/Person> .
                }
            """
            author_count = get(self.getEndpointUrl(), author_count_query, True)
            author_count = author_count['pCount'].iloc[0]+1
            authors_internal_id = {}
            with open(path, "r", encoding="utf-8") as f:
                json_doc = load(f)
                auth = json_doc["authors"]

            lst7 = []
            lst8 = []
            lst9 = []
            lst10 = []

            for key in auth:
                for item in auth[key]:
                    lst7.append(key)
                    lst8.append(item["family"])
                    lst9.append(item["given"])
                    lst10.append(item["orcid"])

            autho_df = DataFrame({
                "id": Series(lst7, dtype="string", name="id"),
                "familyName": Series(lst8, dtype="string", name="familyName"),
                "givenName": Series(lst9, dtype="string", name="givenName"),
                "personID": Series(lst10, dtype="string", name="personID")
            })

            unique_auth = {}
            #creating new authors
            for idx, row in autho_df.iterrows():
                if unique_auth.get(row["personID"], None) == None:

                    local_id = "person-" + str(idx + author_count)
                    subj = URIRef(base_url + local_id)
                    unique_auth[row["personID"]] = subj

                else:

                    subj = unique_auth[row["personID"]]

                my_graph.add((subj, RDF.type, Person))
                my_graph.add((subj, givenName, Literal(row["givenName"])))
                my_graph.add((subj, familyName, Literal(row["familyName"])))
                my_graph.add((subj, identifier, Literal(row["personID"])))
                my_graph.add((subj, doiId, Literal(row["id"])))

                if row["id"] in authors_internal_id:
                    authors_internal_id[row["id"]].append(subj)
                else:
                    authors_internal_id[row["id"]] = [subj]

            ## VENUES

            venue_count_query = """
            SELECT (COUNT(?subj) as ?pCount)
            WHERE {
                ?subj <https://schema.org/subclass> <https://schema.org/VenueClass> .
                }
            """
            venue_count = get(self.getEndpointUrl(), venue_count_query, True)
            venue_count =  venue_count['pCount'].iloc[0]+1
            with open(path, "r", encoding="utf-8") as f:
                json_doc = load(f)
                venue = json_doc["venues_id"]

            lst1 = []
            lst2 = []

            for key in venue:
                for item in venue[key]:
                    lst1.append(key)
                    lst2.append(item)

            venues_df = DataFrame({
                "id": Series(lst1, dtype="string", name="id"),
                "venueID": Series(lst2, dtype="string", name="Venue")
            })
            venues_internal_id = {}
            ven_items={}
            venue_list = venues_df.values.tolist()
            for item in venue_list:
                if ven_items.get(item[0],None)!=None:
                    ven_items[item[0]].append(item[1])
                else:
                    ven_items[item[0]] = [item[1]]

            #Getting existing Venues
            ven_query = """
                                    SELECT ?subj ?doi ?doiPublisher
                                    WHERE {
                                            VALUES ?type {
                                              <https://schema.org/Periodical>
                                            <https://schema.org/Book>
                                            <https://schema.org/Proceedings>
                                        }
                                        ?subj a ?type .
                                        ?subj <https://schema.org/doiId> ?doi.
                                        ?subj <https://schema.org/doiPublisher> ?doiPublisher
                                        }
                                    """
            ven_var = get(self.getEndpointUrl(), ven_query, True)
            ven_var = ven_var.values.tolist()

            ## Getting existing PUBLICATION

            publi_query = """
                        SELECT distinct ?subj ?doi
                        WHERE {
                                 VALUES ?type {
                                 <https://schema.org/ScholarlyArticle>
                                <https://schema.org/Chapter>
                                <https://schema.org/ProceedingsPaper>
                            }
                            ?subj a ?type .
                            ?subj <https://schema.org/identifier> ?doi.
                            }
                        """
            public = get(self.getEndpointUrl(), publi_query, True)
            public= public.values.tolist()

            public_dict = {}
            for elem in public:
                if elem[1] in public_dict:
                    public_dict[elem[1]].append(elem[0])
                else:
                    public_dict[elem[1]] = [elem[0]]

            with open(path, "r", encoding="utf-8") as f:
                json_doc = load(f)
                publi = json_doc["references"]

            ##adding data to existing venues
            for item in ven_var:
                subject= item[0]
                doi = item[1]
                doiPub = item[2]

                #adding venue ids
                if ven_items.get(doi,None)!=None:
                    ids = ven_items[doi]
                    for i in ids:
                        my_graph.add(((URIRef(subject), identifier, Literal(i))))

                if org_internal_id.get(doiPub,None)!=None:
                    my_graph.add((URIRef(subject), publisher, org_internal_id[doiPub][0]))


            #adding data to existing publications
            for item in public:
                subject = item[0]
                doi=item[1]


                #adding authors to publications
                for item in (authors_internal_id.get(doi, [])):
                    my_graph.add((URIRef(subject), author, item))


                #adding cites to publications
                if publi.get(doi, None) != None:
                    #print(subject,doi,publi[doi])
                    cited_pub = publi[doi]
                    if len(cited_pub)==0:
                        my_graph.add((URIRef(subject), cites, Literal("NA")))
                    else:
                        for i in cited_pub:
                            #print(public_dict.get(i))
                            if public_dict.get(i,None)!=None:
                                cited_item=URIRef(public_dict.get(i)[0])
                                my_graph.add((URIRef(subject), cites, cited_item))



        if path.split(".")[1]=='csv':

            publications = read_csv(path,
                                    keep_default_na=False,
                                    dtype={
                                        "id": "string",
                                        "title": "string",
                                        "type": "string",
                                        "publication_year": "int",
                                        "issue": "string",
                                        "volume": "string",
                                        "chapter": "string",
                                        "publication_venue": "string",
                                        "venue_type": "string",
                                        "publisher": "string",
                                        "event": "string"
                                    })

            #getting existing organizations
            org_query = """
                                        SELECT ?subj ?doi
                                        WHERE {
                                            ?subj a <https://schema.org/Organization> .
                                            ?subj <https://schema.org/identifier> ?doi 
                                            }


                                        """
            org_query = get(self.getEndpointUrl(), org_query, True)
            org_query = org_query.values.tolist()
            org_dict = {}
            for elem in org_query:
                if elem[1] in org_query:
                    org_dict[elem[1]].append(elem[0])
                else:
                    org_dict[elem[1]] = [elem[0]]


            #getting existing authors
            author_query = """
                                        SELECT ?subj ?doi
                                        WHERE {
                                            ?subj a <https://schema.org/Person> .
                                            ?subj <https://schema.org/doiId> ?doi
                                            }


                                        """
            author_query = get(self.getEndpointUrl(), author_query, True)
            author_query = author_query.values.tolist()


            auth_dict = {}
            for elem in author_query:
                if elem[1] in author_query:
                    auth_dict[elem[1]].append(elem[0])
                else:
                    auth_dict[elem[1]] = [elem[0]]

            # getting count of existing publications
            publi_query = """
                                    SELECT  (COUNT(?subj) as ?pCount)
                                    WHERE {
                                             VALUES ?type {
                                             <https://schema.org/ScholarlyArticle>
                                            <https://schema.org/Chapter>
                                            <https://schema.org/ProceedingsPaper>
                                        }
                                        ?subj a ?type .
                                        }
                                    """
            public_count = get(self.getEndpointUrl(), publi_query, True)
            public_count = public_count['pCount'].iloc[0]+1

            #Getting count of existing venues
            exist_ven_query = """
                                SELECT  (COUNT(?subj) as ?pCount)
                                WHERE {
                                         VALUES ?type {
                                         <https://schema.org/Periodical>
                                            <https://schema.org/Book>
                                            <https://schema.org/Proceedings>
                                    }
                                    ?subj a ?type .
                                    }
                                                """
            exist_ven_count = get(self.getEndpointUrl(), exist_ven_query, True)
            exist_ven_count = exist_ven_count['pCount'].iloc[0] + 1


            #creating new publications and venues. We insert if strings are not empty
            for idx, row in publications.iterrows():
                local_id = "publication-" + str(idx+public_count)
                subj = URIRef(base_url + local_id)

                ven_local_id = "venue-" + str(idx+exist_ven_count)
                ven_subj = URIRef(base_url + ven_local_id)

                if row["type"] == "journal-article":
                    my_graph.add((subj, RDF.type, JournalArticle))

                    if row["issue"] != "":
                        my_graph.add((subj, issue, Literal(row["issue"])))

                    if row["volume"] != "":
                        my_graph.add((subj, volume, Literal(row["volume"])))

                elif row["type"] == "book-chapter":
                    my_graph.add((subj, RDF.type, BookChapter))

                    if row["chapter"] != "":
                        my_graph.add((subj, chapterNumber, Literal(row["chapter"])))

                elif row["type"]== "proceedings-paper":

                    my_graph.add((subj, RDF.type, ProceedingPaper))

                if row["title"] != "":
                    my_graph.add((subj, title, Literal(row["title"])))

                if row["publication_year"] != "":
                    my_graph.add((subj, publicationYear, Literal(row["publication_year"])))

                if row["id"] != "":

                    my_graph.add((subj, identifier, Literal(row["id"])))

                my_graph.add((subj, publicationVenue, ven_subj))

                #Venues
                if row["venue_type"] == "journal":

                    my_graph.add((ven_subj, RDF.type, Journal))

                elif row["venue_type"] == "book":
                    my_graph.add((ven_subj, RDF.type, Book))

                elif row["venue_type"] == "proceedings":

                    my_graph.add((ven_subj, RDF.type, Proceedings))
                    if row["event"] != "":
                        my_graph.add((ven_subj, event, Literal(row["event"])))

                if row["publication_venue"] != "":
                    my_graph.add((ven_subj, title, Literal(row["publication_venue"])))

                my_graph.add((ven_subj, doiId,Literal(row["id"])))
                my_graph.add((ven_subj, doiPublisher, Literal(row["publisher"])))

                #adding organization to venues if they exist
                if org_dict.get(row["publisher"], None) != None:
                    org = org_dict[row["publisher"]][0]
                    my_graph.add((ven_subj, publisher, URIRef(org)))


                # adding authors to publications if they exist
                if auth_dict.get(row["id"], None) != None:
                    authorNew = auth_dict[row["id"]][0]
                    my_graph.add((subj, author, URIRef(authorNew)))



        store = SPARQLUpdateStore()

        # The URL of the SPARQL endpoint is the same URL of the Blazegraph
        # instance + '/sparql'
        endpoint = self.getEndpointUrl()

        # It opens the connection with the SPARQL endpoint instance
        store.open((endpoint, endpoint))

        for triple in my_graph.triples((None, None, None)):
            store.add(triple)

        # Once finished, remember to close the connection
        store.close()
        return True

class TriplestoreQueryProcessor(TriplestoreProcessor, QueryProcessor):
    def __init__(self):
        super().__init__()

    def getPublicationsPublishedInYear(self, year):

        publication_query = """
          SELECT ?internalId ?doi ?title ?pubyear ?pubissue ?pubvolume ?pubchapter ?authorinternalid ?authorid ?authorgivenName 
          ?authorfamilyName ?pubvenueinternalid ?pubvenueissn ?pubvenuetitle ?pubvenueevent ?pubvenuepublisherinternalid 
          ?pubvenuepublisherid ?pubvenuepublishername ?citesdoi
          WHERE {
              VALUES ?type {
                  <https://schema.org/ScholarlyArticle>
                  <https://schema.org/Chapter>
                  <https://schema.org/ProceedingsPaper>
              }
              ?internalId a ?type ;
                   <https://schema.org/identifier> ?doi;
                   <https://schema.org/name> ?title;
                   <https://schema.org/datePublished> ?pubyear;
                   <https://schema.org/datePublished> """ + str(year) + """;
                   <https://schema.org/authorf> ?authorinternalid;
                   <https://schema.org/isPartOf> ?pubvenueinternalid;
                   <https://schema.org/cites> ?cites.

              ?authorinternalid <https://schema.org/identifier> ?authorid;
                  <https://schema.org/givenName> ?authorgivenName;
                  <https://schema.org/familyName> ?authorfamilyName.

              ?pubvenueinternalid  <https://schema.org/identifier> ?pubvenueissn;
                   <https://schema.org/name> ?pubvenuetitle;
                   <https://schema.org/publisher> ?pubvenuepublisherinternalid.

              ?pubvenuepublisherinternalid <https://schema.org/identifier> ?pubvenuepublisherid;
                                           <https://schema.org/name> ?pubvenuepublishername.

              OPTIONAL{?internalId <https://schema.org/issueNumber> ?pubissue.}
              OPTIONAL{?internalId <https://schema.org/volumeNumber> ?pubvolume.}    
              OPTIONAL{?internalId <https://schema.org/chapterNumber> ?pubchapter.}        
              OPTIONAL{?pubvenueinternalid <https://schema.org/event> ?pubvenueevent.}    
              OPTIONAL{?cites <https://schema.org/identifier> ?citesdoi.}

           }
           """

        df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
        return df_publications_sparql

    def getPublicationsByAuthorId(self, id):
        publication_query = """
          SELECT ?internalId ?doi ?title ?pubyear ?pubissue ?pubvolume ?pubchapter ?authorinternalid ?authorid ?authorgivenName 
          ?authorfamilyName ?pubvenueinternalid ?pubvenueissn ?pubvenuetitle ?pubvenueevent ?pubvenuepublisherinternalid 
          ?pubvenuepublisherid ?pubvenuepublishername ?citesdoi
          WHERE {
              VALUES ?type {
                  <https://schema.org/ScholarlyArticle>
                  <https://schema.org/Chapter>
                  <https://schema.org/ProceedingsPaper>
              }
              ?internalId a ?type ;
                   <https://schema.org/identifier> ?doi;
                   <https://schema.org/name> ?title;
                   <https://schema.org/datePublished> ?pubyear;
                   <https://schema.org/authorf> ?authorinternalid;
                   <https://schema.org/isPartOf> ?pubvenueinternalid;
                   <https://schema.org/cites> ?cites.

              ?authorinternalid <https://schema.org/identifier> ?authorid;
                  <https://schema.org/identifier>  '""" + id + """';
                  <https://schema.org/givenName> ?authorgivenName;
                  <https://schema.org/familyName> ?authorfamilyName.

              ?pubvenueinternalid  <https://schema.org/identifier> ?pubvenueissn;
                   <https://schema.org/name> ?pubvenuetitle;
                   <https://schema.org/publisher> ?pubvenuepublisherinternalid.

              ?pubvenuepublisherinternalid <https://schema.org/identifier> ?pubvenuepublisherid;
                                           <https://schema.org/name> ?pubvenuepublishername.

              OPTIONAL{?internalId <https://schema.org/issueNumber> ?pubissue.}
              OPTIONAL{?internalId <https://schema.org/volumeNumber> ?pubvolume.}    
              OPTIONAL{?internalId <https://schema.org/chapterNumber> ?pubchapter.}        
              OPTIONAL{?pubvenueinternalid <https://schema.org/event> ?pubvenueevent.}    
              OPTIONAL{?cites <https://schema.org/identifier> ?citesdoi.}

           }
           """
        df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
        return df_publications_sparql

    def getVenuesByPublisherId(self, id):
        publication_query = """
        SELECT  ?internalId ?doi ?title ?publisher ?pubId  ?pubName ?event
        WHERE {
            VALUES ?type {
                <https://schema.org/Periodical>
                <https://schema.org/Book>
                <https://schema.org/Proceedings>
            }

            ?internalId a ?type ;
                <https://schema.org/name> ?title ;
                <https://schema.org/publisher> ?publisher.

            ?publisher <https://schema.org/identifier> '""" + id + """'.

        OPTIONAL {
            ?internalId
                <https://schema.org/event> ?event .

        } 
         OPTIONAL {
            ?internalId
                <https://schema.org/identifier>  ?doi.

        } 
         OPTIONAL {
            ?publisher
                <https://schema.org/name>  ?pubName.

        } 
         OPTIONAL {
            ?publisher
                <https://schema.org/identifier>  ?pubId.

        } 
        }
        """

        df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
        return df_publications_sparql

    def getPublicationInVenue(self, id):
        publication_query = """
          SELECT ?internalId ?doi ?title ?pubyear ?pubissue ?pubvolume ?pubchapter ?authorinternalid ?authorid ?authorgivenName 
          ?authorfamilyName ?pubvenueinternalid ?pubvenueissn ?pubvenuetitle ?pubvenueevent ?pubvenuepublisherinternalid 
          ?pubvenuepublisherid ?pubvenuepublishername ?citesdoi
          WHERE {
              VALUES ?type {
                  <https://schema.org/ScholarlyArticle>
                  <https://schema.org/Chapter>
                  <https://schema.org/ProceedingsPaper>
              }
              ?internalId a ?type ;
                   <https://schema.org/identifier> ?doi;
                   <https://schema.org/name> ?title;
                   <https://schema.org/datePublished> ?pubyear;
                   <https://schema.org/authorf> ?authorinternalid;
                   <https://schema.org/isPartOf> ?pubvenueinternalid;
                   <https://schema.org/cites> ?cites.

              ?authorinternalid <https://schema.org/identifier> ?authorid;
                  <https://schema.org/givenName> ?authorgivenName;
                  <https://schema.org/familyName> ?authorfamilyName.

              ?pubvenueinternalid  <https://schema.org/identifier> ?pubvenueissn;
                                <https://schema.org/identifier> '""" + id + """';
                   <https://schema.org/name> ?pubvenuetitle;
                   <https://schema.org/publisher> ?pubvenuepublisherinternalid.

              ?pubvenuepublisherinternalid <https://schema.org/identifier> ?pubvenuepublisherid;
                                           <https://schema.org/name> ?pubvenuepublishername.

              OPTIONAL{?internalId <https://schema.org/issueNumber> ?pubissue.}
              OPTIONAL{?internalId <https://schema.org/volumeNumber> ?pubvolume.}    
              OPTIONAL{?internalId <https://schema.org/chapterNumber> ?pubchapter.}        
              OPTIONAL{?pubvenueinternalid <https://schema.org/event> ?pubvenueevent.}    
              OPTIONAL{?cites <https://schema.org/identifier> ?citesdoi.}

           }
           """
        df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
        return df_publications_sparql

    def getJournalArticlesInIssue(self, issue, volume, journalid):
        publication_query = """
          SELECT ?internalId ?doi ?title ?pubyear ?pubissue ?pubvolume ?authorinternalid ?authorid ?authorgivenName 
          ?authorfamilyName ?pubvenueinternalid ?pubvenueissn ?pubvenuetitle ?pubvenuepublisherinternalid 
          ?pubvenuepublisherid ?pubvenuepublishername ?citesdoi
          WHERE {
              VALUES ?type {
                  <https://schema.org/ScholarlyArticle>
                  <https://schema.org/Chapter>
                  <https://schema.org/ProceedingsPaper>
              }
              ?internalId a ?type ;
                   <https://schema.org/identifier> ?doi;
                   <https://schema.org/name> ?title;
                   <https://schema.org/datePublished> ?pubyear;
                   <https://schema.org/authorf> ?authorinternalid;
                   <https://schema.org/isPartOf> ?pubvenueinternalid;
                   <https://schema.org/cites> ?cites;
                    <https://schema.org/issueNumber> ?pubissue;
                   <https://schema.org/issueNumber> '""" + issue + """' ;
                   <https://schema.org/volumeNumber> ?pubvolume;
                   <https://schema.org/volumeNumber> '""" + volume + """'.
                   
              ?authorinternalid <https://schema.org/identifier> ?authorid;
                  <https://schema.org/givenName> ?authorgivenName;
                  <https://schema.org/familyName> ?authorfamilyName.

              ?pubvenueinternalid  <https://schema.org/identifier> ?pubvenueissn;
                   <https://schema.org/identifier> '""" + journalid + """';
                   <https://schema.org/name> ?pubvenuetitle;
                   <https://schema.org/publisher> ?pubvenuepublisherinternalid.

              ?pubvenuepublisherinternalid <https://schema.org/identifier> ?pubvenuepublisherid;
                                           <https://schema.org/name> ?pubvenuepublishername.

              OPTIONAL{?cites <https://schema.org/identifier> ?citesdoi.}

           }
           """
        df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
        return df_publications_sparql

    def getJournalArticlesInVolume(self, volume, journalid):
        publication_query = """
                  SELECT ?internalId ?doi ?title ?pubyear ?pubissue ?pubvolume ?authorinternalid ?authorid ?authorgivenName 
                  ?authorfamilyName ?pubvenueinternalid ?pubvenueissn ?pubvenuetitle ?pubvenuepublisherinternalid 
                  ?pubvenuepublisherid ?pubvenuepublishername ?citesdoi
                  WHERE {
                      VALUES ?type {
                          <https://schema.org/ScholarlyArticle>
                          <https://schema.org/Chapter>
                          <https://schema.org/ProceedingsPaper>
                      }
                      ?internalId a ?type ;
                           <https://schema.org/identifier> ?doi;
                           <https://schema.org/name> ?title;
                           <https://schema.org/datePublished> ?pubyear;
                           <https://schema.org/authorf> ?authorinternalid;
                           <https://schema.org/isPartOf> ?pubvenueinternalid;
                           <https://schema.org/cites> ?cites;
                            <https://schema.org/issueNumber> ?pubissue;
                           <https://schema.org/volumeNumber> ?pubvolume;
                           <https://schema.org/volumeNumber> '""" + volume + """'.

                      ?authorinternalid <https://schema.org/identifier> ?authorid;
                          <https://schema.org/givenName> ?authorgivenName;
                          <https://schema.org/familyName> ?authorfamilyName.

                      ?pubvenueinternalid  <https://schema.org/identifier> ?pubvenueissn;
                           <https://schema.org/identifier> '""" + journalid + """';
                           <https://schema.org/name> ?pubvenuetitle;
                           <https://schema.org/publisher> ?pubvenuepublisherinternalid.

                      ?pubvenuepublisherinternalid <https://schema.org/identifier> ?pubvenuepublisherid;
                                                   <https://schema.org/name> ?pubvenuepublishername.

                      OPTIONAL{?cites <https://schema.org/identifier> ?citesdoi.}

                   }
                   """

        df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
        return df_publications_sparql

    def getJournalArticlesInJournal(self, journalid):

        publication_query = """
                  SELECT ?internalId ?doi ?title ?pubyear ?pubissue ?pubvolume ?authorinternalid ?authorid ?authorgivenName 
                  ?authorfamilyName ?pubvenueinternalid ?pubvenueissn ?pubvenuetitle ?pubvenuepublisherinternalid 
                  ?pubvenuepublisherid ?pubvenuepublishername ?citesdoi
                  WHERE {
                      VALUES ?type {
                          <https://schema.org/ScholarlyArticle>
                          <https://schema.org/Chapter>
                          <https://schema.org/ProceedingsPaper>
                      }
                      ?internalId a ?type ;
                           <https://schema.org/identifier> ?doi;
                           <https://schema.org/name> ?title;
                           <https://schema.org/datePublished> ?pubyear;
                           <https://schema.org/authorf> ?authorinternalid;
                           <https://schema.org/isPartOf> ?pubvenueinternalid;
                           <https://schema.org/cites> ?cites;
                            <https://schema.org/issueNumber> ?pubissue;
                           <https://schema.org/volumeNumber> ?pubvolume.

                      ?authorinternalid <https://schema.org/identifier> ?authorid;
                          <https://schema.org/givenName> ?authorgivenName;
                          <https://schema.org/familyName> ?authorfamilyName.

                      ?pubvenueinternalid  <https://schema.org/identifier> ?pubvenueissn;
                           <https://schema.org/identifier> '""" + journalid + """';
                           <https://schema.org/name> ?pubvenuetitle;
                           <https://schema.org/publisher> ?pubvenuepublisherinternalid.

                      ?pubvenuepublisherinternalid <https://schema.org/identifier> ?pubvenuepublisherid;
                                                   <https://schema.org/name> ?pubvenuepublishername.

                      OPTIONAL{?cites <https://schema.org/identifier> ?citesdoi.}

                   }
                   """

        df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
        return df_publications_sparql

    def getProceedingsByEvent(self, eventPartialName):
        publication_query = """
        SELECT  ?internalId ?id ?title ?publisher ?pubId ?pubName ?event 
        WHERE {
            ?internalId a <https://schema.org/Proceedings>;
                        <https://schema.org/name> ?title;
                        <https://schema.org/event> ?event;
                        <https://schema.org/publisher> ?publisher.
            ?publisher <https://schema.org/name> ?pubName.
            ?publisher <https://schema.org/identifier> ?pubId.
                        
            optional {?internalId <https://schema.org/identifier> ?id.}
            
            FILTER(REGEX(?event, '""" + eventPartialName + """', "i"))
            
        }
        """

        df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
        return df_publications_sparql

    def getPublicationAuthors(self, publicationid):
        publication_query = """
        SELECT  ?author ?id ?givenName ?familyName 
        WHERE {

        VALUES ?type {
                <https://schema.org/ScholarlyArticle>
                <https://schema.org/Chapter>
                <https://schema.org/ProceedingsPaper>
            }
            ?PubinternalId a ?type ;
                <https://schema.org/identifier> '""" + publicationid + """';
                <https://schema.org/authorf> ?author .


            ?author a  <https://schema.org/Person> ;
                <https://schema.org/identifier> ?id ;
                <https://schema.org/givenName> ?givenName ;
                <https://schema.org/familyName> ?familyName;

        }
        """

        df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
        return df_publications_sparql

    def getPublicationsByAuthorsName(self, authorPartialName):
        publication_query = """
        SELECT  ?internalId ?doi ?author ?cites ?publicationYear ?issue ?volume ?chapterN ?title ?publicationVenue 
                WHERE {
                    {
                    VALUES ?type {
                        <https://schema.org/ScholarlyArticle>
                        <https://schema.org/Chapter>
                        <https://schema.org/ProceedingsPaper>
                        }

                    ?internalId a ?type ;
                        <https://schema.org/identifier> ?doi ;
                        <https://schema.org/datePublished> ?publicationYear ;
                        <https://schema.org/name> ?title ;
                        <https://schema.org/authorf> ?author.


                    ?author <https://schema.org/givenName> ?givenName.
                    ?author <https://schema.org/familyName> ?familyName.

                    FILTER(REGEX(?givenName, '""" + authorPartialName + """', "i"))

                    OPTIONAL {?internalId <https://schema.org/issueNumber> ?issue.} 
                    OPTIONAL {?internalId <https://schema.org/volumeNumber> ?volume.} 
                    OPTIONAL {?internalId <https://schema.org/chapterNumber> ?chapterN.} 
                     OPTIONAL {?internalId <https://schema.org/cites> ?cites .}
                     OPTIONAL {?internalId  <https://schema.org/isPartOf> ?publicationVenue.}

                    }
                    UNION
                    {
                    VALUES ?type {
                        <https://schema.org/ScholarlyArticle>
                        <https://schema.org/Chapter>
                        <https://schema.org/ProceedingsPaper>
                    }

                    ?internalId a ?type ;
                       <https://schema.org/identifier> ?doi ;
                        <https://schema.org/datePublished> ?publicationYear ;
                        <https://schema.org/name> ?title ;
                        <https://schema.org/authorf> ?author.


                    ?author <https://schema.org/givenName> ?givenName.
                    ?author <https://schema.org/familyName> ?familyName.

                    FILTER(REGEX(?familyName, '""" + authorPartialName + """', "i"))
                     OPTIONAL {?internalId <https://schema.org/issueNumber> ?issue.} 
                    OPTIONAL {?internalId <https://schema.org/volumeNumber> ?volume.} 
                    OPTIONAL {?internalId <https://schema.org/chapterNumber> ?chapterN.} 
                     OPTIONAL {?internalId <https://schema.org/cites> ?cites .}
                     OPTIONAL {?internalId  <https://schema.org/isPartOf> ?publicationVenue.}
                    }

                }
                """

        publication_query = """
        
        SELECT ?internalId ?doi ?title ?pubyear ?pubissue ?pubvolume ?pubchapter ?authorinternalid ?authorid ?authorgivenName 
          ?authorfamilyName ?pubvenueinternalid ?pubvenueissn ?pubvenuetitle ?pubvenueevent ?pubvenuepublisherinternalid 
          ?pubvenuepublisherid ?pubvenuepublishername ?citesdoi
          WHERE 
          
          {
          
                  {
                  VALUES ?type {
                          <https://schema.org/ScholarlyArticle>
                          <https://schema.org/Chapter>
                          <https://schema.org/ProceedingsPaper>
                      }
                      ?internalId a ?type ;
                           <https://schema.org/identifier> ?doi;
                           <https://schema.org/name> ?title;
                           <https://schema.org/datePublished> ?pubyear;
                           <https://schema.org/authorf> ?authorinternalid;
                           <https://schema.org/isPartOf> ?pubvenueinternalid;
                           <https://schema.org/cites> ?cites.
                
                      ?authorinternalid <https://schema.org/identifier> ?authorid;
                          <https://schema.org/givenName> ?authorgivenName;
                          <https://schema.org/familyName> ?authorfamilyName.
                
                      ?pubvenueinternalid  <https://schema.org/identifier> ?pubvenueissn;
                           <https://schema.org/name> ?pubvenuetitle;
                           <https://schema.org/publisher> ?pubvenuepublisherinternalid.
                
                      ?pubvenuepublisherinternalid <https://schema.org/identifier> ?pubvenuepublisherid;
                                                   <https://schema.org/name> ?pubvenuepublishername.
                    
                      FILTER(REGEX(?authorgivenName, '""" + authorPartialName + """', "i"))
                      OPTIONAL{?internalId <https://schema.org/issueNumber> ?pubissue.}
                      OPTIONAL{?internalId <https://schema.org/volumeNumber> ?pubvolume.}    
                      OPTIONAL{?internalId <https://schema.org/chapterNumber> ?pubchapter.}        
                      OPTIONAL{?pubvenueinternalid <https://schema.org/event> ?pubvenueevent.}    
                      OPTIONAL{?cites <https://schema.org/identifier> ?citesdoi.}

                  }
                  UNION
                  {
                      VALUES ?type {
                              <https://schema.org/ScholarlyArticle>
                              <https://schema.org/Chapter>
                              <https://schema.org/ProceedingsPaper>
                          }
                          ?internalId a ?type ;
                               <https://schema.org/identifier> ?doi;
                               <https://schema.org/name> ?title;
                               <https://schema.org/datePublished> ?pubyear;
                               <https://schema.org/authorf> ?authorinternalid;
                               <https://schema.org/isPartOf> ?pubvenueinternalid;
                               <https://schema.org/cites> ?cites.
                    
                          ?authorinternalid <https://schema.org/identifier> ?authorid;
                              <https://schema.org/givenName> ?authorgivenName;
                              <https://schema.org/familyName> ?authorfamilyName.
                    
                          ?pubvenueinternalid  <https://schema.org/identifier> ?pubvenueissn;
                               <https://schema.org/name> ?pubvenuetitle;
                               <https://schema.org/publisher> ?pubvenuepublisherinternalid.
                    
                          ?pubvenuepublisherinternalid <https://schema.org/identifier> ?pubvenuepublisherid;
                                                       <https://schema.org/name> ?pubvenuepublishername.
                        
                          FILTER(REGEX(?authorfamilyName, '""" + authorPartialName + """', "i"))
                          OPTIONAL{?internalId <https://schema.org/issueNumber> ?pubissue.}
                          OPTIONAL{?internalId <https://schema.org/volumeNumber> ?pubvolume.}    
                          OPTIONAL{?internalId <https://schema.org/chapterNumber> ?pubchapter.}        
                          OPTIONAL{?pubvenueinternalid <https://schema.org/event> ?pubvenueevent.}    
                          OPTIONAL{?cites <https://schema.org/identifier> ?citesdoi.}
                  }
          
          }
        """
        df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
        return df_publications_sparql

    def getDistinctPublishersOfPublications(self, pubIdList):
        lst = []
        for i in pubIdList:
            publication_query = """
            SELECT  ?publisher ?id ?title
            WHERE {

              VALUES ?type {
                        <https://schema.org/ScholarlyArticle>
                        <https://schema.org/Chapter>
                        <https://schema.org/ProceedingsPaper>
                    }


                ?PubinternalId a ?type ;
                    <https://schema.org/identifier> '""" + i + """';
                    <https://schema.org/isPartOf> ?publicationVenue.

                ?publicationVenue <https://schema.org/publisher> ?publisher.
                ?publisher <https://schema.org/name> ?title.
                ?publisher <https://schema.org/identifier> ?id.

            }
            """
            df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
            lst.append(df_publications_sparql)
        final = pd.concat(lst)
        return final

    def getMostCitedPublication(self):
        publication_query = """
        SELECT ?citesM ?doi ?title ?pubyear ?pubissue ?pubvolume ?pubchapter ?authorinternalid ?authorid ?authorgivenName 
          ?authorfamilyName ?pubvenueinternalid ?pubvenueissn ?pubvenuetitle ?pubvenueevent ?pubvenuepublisherinternalid 
          ?pubvenuepublisherid ?pubvenuepublishername ?citesdoi
        
        Where
        {
                {
                    SELECT ?citesM
                    WHERE {
                        VALUES ?type {
                            <https://schema.org/ScholarlyArticle>
                            <https://schema.org/Chapter>
                            <https://schema.org/ProceedingsPaper>
                        }
                        ?internalId a ?type ;
                            <https://schema.org/cites> ?citesM.
                        OPTIONAL {?internalId <https://schema.org/issueNumber> ?issue.} 
                        OPTIONAL {?internalId <https://schema.org/volumeNumber> ?volume.} 
                        OPTIONAL {?internalId <https://schema.org/chapterNumber> ?chapterN.} 
                        FILTER (?citesM != 'NA')   
                    }
                    GROUP BY ?citesM
                    ORDER BY DESC(COUNT(?internalId )) 
                    LIMIT 1
                }
                
            ?citesM
               <https://schema.org/identifier> ?doi;
               <https://schema.org/name> ?title;
               <https://schema.org/datePublished> ?pubyear;
               <https://schema.org/authorf> ?authorinternalid;
               <https://schema.org/isPartOf> ?pubvenueinternalid;
               <https://schema.org/cites> ?cites.
               
          ?authorinternalid <https://schema.org/identifier> ?authorid;
              <https://schema.org/givenName> ?authorgivenName;
              <https://schema.org/familyName> ?authorfamilyName.

          ?pubvenueinternalid  <https://schema.org/identifier> ?pubvenueissn;
               <https://schema.org/name> ?pubvenuetitle;
               <https://schema.org/publisher> ?pubvenuepublisherinternalid.
    
          ?pubvenuepublisherinternalid <https://schema.org/identifier> ?pubvenuepublisherid;
                <https://schema.org/name> ?pubvenuepublishername.
            
          OPTIONAL{?citesM <https://schema.org/issueNumber> ?pubissue.}
          OPTIONAL{?citesM <https://schema.org/volumeNumber> ?pubvolume.}    
          OPTIONAL{?citesM <https://schema.org/chapterNumber> ?pubchapter.}        
          OPTIONAL{?pubvenueinternalid <https://schema.org/event> ?pubvenueevent.}    
          OPTIONAL{?cites <https://schema.org/identifier> ?citesdoi.}
        }
        """
        df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
        return df_publications_sparql

    def getMostCitedVenue(self):
        publication_query = """
                SELECT  ?venue ?id ?title ?publisher ?pubId ?pubName ?event 
                WHERE
                {
                    {
                    SELECT ?cites  
                    WHERE {
                        VALUES ?type {
                            <https://schema.org/ScholarlyArticle>
                            <https://schema.org/Chapter>
                            <https://schema.org/ProceedingsPaper>
                        }

                        ?internalId a ?type ;
                            <https://schema.org/cites> ?cites .

                    OPTIONAL {?internalId <https://schema.org/issueNumber> ?issue.} 
                    OPTIONAL {?internalId <https://schema.org/volumeNumber> ?volume.} 
                    OPTIONAL {?internalId <https://schema.org/chapterNumber> ?chapterN.} 
                    FILTER(?cites != 'NA')
                    }
                    GROUP BY ?cites 
                    ORDER BY DESC(COUNT(?internalId )) 
                    LIMIT 1
                    }
                    ?cites <https://schema.org/isPartOf> ?venue.
                    ?venue <https://schema.org/identifier> ?id.
                    ?venue <https://schema.org/name> ?title.


                    OPTIONAL {?venue <https://schema.org/event> ?event.}
                    OPTIONAL {?venue <https://schema.org/publisher> ?publisher.}
                    OPTIONAL {?publisher <https://schema.org/identifier> ?pubId.}
                    OPTIONAL {?publisher <https://schema.org/name> ?pubName.}

              }


                """
        df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
        return df_publications_sparql


    def getCitesDoi(self, doi,s, dic):

        if doi not in dic:
            publication_query = """
                                SELECT ?internalId ?doi ?title ?pubyear ?pubissue ?pubvolume ?pubchapter ?authorinternalid ?authorid ?authorgivenName 
                                ?authorfamilyName ?pubvenueinternalid ?pubvenueissn ?pubvenuetitle ?pubvenueevent ?pubvenuepublisherinternalid 
                                ?pubvenuepublisherid ?pubvenuepublishername ?citesdoi
                                WHERE {
                                    VALUES ?type {
                                        <https://schema.org/ScholarlyArticle>
                                        <https://schema.org/Chapter>
                                        <https://schema.org/ProceedingsPaper>
                                    }
                                    ?internalId a ?type ;
                                         <https://schema.org/identifier> ?doi;
                                         <https://schema.org/identifier> '""" + doi + """';
                                         <https://schema.org/name> ?title;
                                         <https://schema.org/datePublished> ?pubyear;
                                         <https://schema.org/authorf> ?authorinternalid;
                                         <https://schema.org/isPartOf> ?pubvenueinternalid;
                                         <https://schema.org/cites> ?cites.
                            
                                    ?authorinternalid <https://schema.org/identifier> ?authorid;
                                        <https://schema.org/givenName> ?authorgivenName;
                                        <https://schema.org/familyName> ?authorfamilyName.
                            
                                    ?pubvenueinternalid  <https://schema.org/identifier> ?pubvenueissn;
                                         <https://schema.org/name> ?pubvenuetitle;
                                         <https://schema.org/publisher> ?pubvenuepublisherinternalid.
                            
                                    ?pubvenuepublisherinternalid <https://schema.org/identifier> ?pubvenuepublisherid;
                                                                 <https://schema.org/name> ?pubvenuepublishername.
                            
                                    OPTIONAL{?internalId <https://schema.org/issueNumber> ?pubissue.}
                                    OPTIONAL{?internalId <https://schema.org/volumeNumber> ?pubvolume.}    
                                    OPTIONAL{?internalId <https://schema.org/chapterNumber> ?pubchapter.}        
                                    OPTIONAL{?pubvenueinternalid <https://schema.org/event> ?pubvenueevent.}    
                                    OPTIONAL{?cites <https://schema.org/identifier> ?citesdoi.}
                            
                                 }
                                 """

            df_publications_sparql = get(self.getEndpointUrl(), publication_query, True)
            df_publications_sparql = df_publications_sparql.fillna('NA')
            if len(df_publications_sparql) == 0:
                return None

            s =  s + list(set(df_publications_sparql.citesdoi.tolist()))

            if len(s)==0:
                cite='NA'
            else:
                cite='NA'
                for item in s:
                    if item != 'NA':

                        cite = s[s.index(item)]


            if cite == 'NA' or doi in s :
                df_final = df_publications_sparql.fillna('NA')
                df_final = df_final.values.tolist()
                authors = {}
                for i in df_final:
                    if authors.get(i[0], None) == None:
                        authors[i[0]] = [[i[7], i[8], i[9], i[10]]]
                    else:
                        if [i[7], i[8], i[9], i[10]] not in authors[i[0]]:
                            authors[i[0]].append([i[7], i[8], i[9], i[10]])

                venue = {}
                for i in df_final:
                    if venue.get(i[0], None) == None:
                        venue[i[0]] = [[i[11], i[12], i[13], i[14]]]
                    else:
                        if [i[11], i[12], i[13], i[14]] not in venue[i[0]]:
                            venue[i[0]].append([i[11], i[12], i[13], i[14]])

                publisher = {}
                for i in df_final:
                    if publisher.get(i[0], None) == None:
                        publisher[i[0]] = [[i[15], i[16], i[17]]]
                    else:
                        if [i[15], i[16], i[17]] not in publisher[i[0]]:
                            publisher[i[0]].append([i[15], i[16], i[17]])

                pubs = {}
                for i in df_final:
                    if pubs.get(i[0], None) == None:
                        pubs[i[0]] = [[i[1], i[2], i[3], i[4], i[5], i[6]]]
                    else:
                        if [i[1], i[2], i[3], i[4], i[5], i[6]] not in pubs[i[0]]:
                            pubs[i[0]].append([i[1], i[2], i[3], i[4], i[5], i[6]])

                for item in pubs:
                    lst_author = []
                    lst_ven_id = []
                    for a in authors[item]:
                        lst_author.append(Person([a[1]], a[2], a[3]))

                    for b in venue[item]:
                        lst_ven_id.append(b[1])


                    publish = Organization([publisher[item][0][1]], publisher[item][0][2])
                    ven = Venue(lst_ven_id, venue[item][0][2], publish)


                    f_pub = pubs[item][0]

                    if (f_pub[3] !='NA' or f_pub[4] !='NA' and f_pub[5]== 'NA'):
                        dic[doi] =  JournalArticle([f_pub[0]],f_pub[2],f_pub[1],ven,[],lst_author,f_pub[3],f_pub[4])
                    elif f_pub[5]!= 'NA':
                        dic[doi] = BookChapter([f_pub[0]], f_pub[2], f_pub[1], ven, [], lst_author,f_pub[5])
                    else:
                        dic[doi] = ProceedingsPaper([f_pub[0]], f_pub[2], f_pub[1], ven, [],lst_author)


            else:
                df_final = df_publications_sparql.fillna('NA')
                df_final = df_final.values.tolist()
                authors = {}
                for i in df_final:
                    if authors.get(i[0], None) == None:
                        authors[i[0]] = [[i[7], i[8], i[9], i[10]]]
                    else:
                        if [i[7], i[8], i[9], i[10]] not in authors[i[0]]:
                            authors[i[0]].append([i[7], i[8], i[9], i[10]])

                venue = {}
                for i in df_final:
                    if venue.get(i[0], None) == None:
                        venue[i[0]] = [[i[11], i[12], i[13], i[14]]]
                    else:
                        if [i[11], i[12], i[13], i[14]] not in venue[i[0]]:
                            venue[i[0]].append([i[11], i[12], i[13], i[14]])

                publisher = {}
                for i in df_final:
                    if publisher.get(i[0], None) == None:
                        publisher[i[0]] = [[i[15], i[16], i[17]]]
                    else:
                        if [i[15], i[16], i[17]] not in publisher[i[0]]:
                            publisher[i[0]].append([i[15], i[16], i[17]])

                pubs = {}
                for i in df_final:
                    if pubs.get(i[0], None) == None:
                        pubs[i[0]] = [[i[1], i[2], i[3], i[4], i[5], i[6]]]
                    else:
                        if [i[1], i[2], i[3], i[4], i[5], i[6]] not in pubs[i[0]]:
                            pubs[i[0]].append([i[1], i[2], i[3], i[4], i[5], i[6]])

                for item in pubs:
                    lst_author = []
                    lst_ven_id = []
                    for a in authors[item]:
                        lst_author.append(Person([a[1]], a[2], a[3]))

                    for b in venue[item]:
                        lst_ven_id.append(b[1])


                    publish = Organization([publisher[item][0][1]], publisher[item][0][2])
                    ven = Venue(lst_ven_id, venue[item][0][2], publish)


                    f_pub = pubs[item][0]

                    if (f_pub[3] != 'NA' or f_pub[4] != 'NA' and f_pub[5] == 'NA'):
                        dic[doi]  = JournalArticle([f_pub[0]], f_pub[2], f_pub[1], ven, [self.getCitesDoi(cite,s,dic)], lst_author, f_pub[3], f_pub[4])
                    elif f_pub[5] != 'NA':
                        dic[doi]  = BookChapter([f_pub[0]], f_pub[2], f_pub[1], ven, [self.getCitesDoi(cite,s,dic)], lst_author, f_pub[5])
                    else:
                        dic[doi]  = ProceedingsPaper([f_pub[0]], f_pub[2], f_pub[1], ven, [self.getCitesDoi(cite,s,dic)], lst_author)

        return dic[doi]

