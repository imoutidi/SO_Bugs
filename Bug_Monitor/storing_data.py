from pymongo import MongoClient, ASCENDING
import pprint

from Tool_Pack import tools


client = MongoClient('localhost', 27017)
db = client.Archive
collection = db.Q_ids_to_tags


# all_Qs = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\SO_New\IO_Files\Questions\Archives\Qid_to_tag_list")


print(db.get_collection("Q_ids_to_tags").estimated_document_count())

# result = db.profiles.create_index([('user_id', ASCENDING)],
#                                   unique=True)
# sorted(list(db.profiles.index_information()))

print(collection.find_one({"Q_id": 845}))


#
