# 1) Importing all the classes for handling the relational database
from impl import RelationalDataProcessor, RelationalQueryProcessor

# 2) Importing all the classes for handling RDF database
from impl import TriplestoreDataProcessor, TriplestoreQueryProcessor

# 3) Importing the class for dealing with generic queries
from impl import GenericQueryProcessor

from datetime import datetime

#creating the relational database using the related source data
rel_path = "relational.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)

print("Uploading csv data for relational...")
if (rel_dp.uploadData("data/relational_publications.csv")):
    print("Upload completed for relational csv.")


print("Uploading json data for relational...")
if (rel_dp.uploadData("data/relational_other_data.json")):
    print("Upload completed for relational json.")

#Creating the RDF triplestore
grp_endpoint = "http://192.168.1.198:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)

print("Uploading csv data for triplestore...")
if (grp_dp.uploadData("data/graph_publications.csv")):
    print("Upload completed for triplestore csv.")

print("Uploading json data for triplestore...")
if (grp_dp.uploadData("data/graph_other_data.json")):
    print("Upload completed for triplestore json.")

# Creating query processors for both
# the databases, using the related classes
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setEndpointUrl(grp_endpoint)

# Finally, creating a generic query processor for asking
# about data
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)



#Some sample queries
result_q1 = generic.getPublicationsByAuthorsName("daniel")
print("result of query 1: \n", result_q1)

result_q2 = generic.getPublicationAuthors("doi:10.1162/qss_a_00023")
print("result of query 2 \n",result_q2)

result_q3 = generic.getProceedingsByEvent("web")
print("result of query 3: \n",result_q3)

result_q4 = generic.getJournalArticlesInJournal("issn:0219-1377")
print("result of query 4: \n",result_q4)

result_q5 = generic.getJournalArticlesInVolume("62","issn:0219-1377")
print("result of query 5: \n",result_q5)

result_q6 = generic.getJournalArticlesInIssue("3","55","issn:0219-1377")
print("result of query 6: \n",result_q6)

result_q7 = generic.getPublicationInVenue("issn:1570-8268")
print("result of query 7: \n",result_q7)

result_q8 = generic.getVenuesByPublisherId("crossref:292")
print("result of query 8: \n",result_q8)

result_q9 = generic.getPublicationsPublishedInYear(2018)
print("result of query 9: \n",result_q9)

result_q10 = generic.getPublicationsByAuthorId("0000-0001-5506-523X")
print("result of query 10: \n",result_q10)

result_q11 = generic.getMostCitedVenue()
print("result of query 11: \n",result_q11)

result_q12 = generic.getMostCitedPublication()
print("result of query 12: \n",result_q12)

result_q13 = generic.getDistinctPublishersOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ])
print("result of query 13: \n",result_q13)