from Tool_Pack import tools
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient, ASCENDING, errors
from collections import defaultdict


class QuestionArchiver:
    def __init__(self):
        self.start_date_obj = parse("2008-08-01T00:00:00.000")
        self.end_date_obj = parse("2022-12-01T00:00:00.000")
        self.io_path = r"C:\Users\irmo\PycharmProjects\SO_New\IO_Files\Questions\\"
        self.month_delta = relativedelta(months=1)
        self.tag_dictionary = dict()

    def create_tag_database(self):
        client = MongoClient('localhost', 27017)
        db = client.Archive
        collection = db.Q_ids_to_tags

        current_date_obj = self.start_date_obj
        while current_date_obj <= self.end_date_obj:
            print(current_date_obj.strftime("%Y-%m"))
            current_year = current_date_obj.strftime("%Y")
            current_month = current_date_obj.strftime("%m")
            if len(current_month) == 2:
                if current_month[0] == '0':
                    current_month = current_month.replace("0", "")
            month_posts = tools.load_pickle(self.io_path + "Parsed stuff/" + current_year + "\\"
                                            + current_year + "-" + current_month)
            for m_post in month_posts:
                if m_post[0] == '1':
                    tags = [x.replace(">", "") for x in m_post[2].split("<")[1:]]
                    try:
                        collection.insert_one({"Q_id": int(m_post[3]), "Tags": tags})
                    except errors.DuplicateKeyError as key_error:
                        print(key_error)
                        print(m_post[3], tags)
                        print(m_post)

                    # self.tag_dictionary[int(m_post[3])] = tags
            current_date_obj += self.month_delta
        # tools.save_pickle(self.io_path + "Archives/int_Qid_to_tag_list", self.tag_dictionary)


class TagBugMatcher:
    def __init__(self):
        self.io_path = r"C:\Users\irmo\PycharmProjects\SO_Bugs\I_O\\"
        self.start_date_obj = parse("2008-08-01T00:00:00.000")
        self.end_date_obj = parse("2022-12-01T00:00:00.000")
        self.month_delta = relativedelta(months=1)
        # The info of the dict is Post ID, Date and Score
        self.tag_to_info = dict()
        # Database
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.Archive
        self.collection = self.db.Q_ids_to_tags


    @staticmethod
    def initial_dictionary():
        t_dict = dict()
        t_dict["Post_ID"] = list()
        t_dict["Date"] = list()
        t_dict["Score"] = list()
        return t_dict

    def save_to_tag_dict(self, tag_container, post_id, score, date_obj):
        for tag in tag_container:
            if tag in self.tag_to_info:
                self.tag_to_info[tag]["Post_ID"].append(post_id)
                self.tag_to_info[tag]["Score"].append(score)
                self.tag_to_info[tag]["Date"].append(date_obj)
            else:
                temp_dict = self.initial_dictionary()
                temp_dict["Post_ID"].append(post_id)
                temp_dict["Score"].append(score)
                temp_dict["Date"].append(date_obj)
                self.tag_to_info[tag] = temp_dict

    def match_bug_and_tag(self):
        current_date_obj = self.start_date_obj
        vidited_questions = set()
        while current_date_obj <= self.end_date_obj:
            print(current_date_obj.strftime("%Y-%m"))
            current_year = current_date_obj.strftime("%Y")
            current_month = current_date_obj.strftime("%m")
            if len(current_month) == 2:
                if current_month[0] == '0':
                    current_month = current_month.replace("0", "")
            bug_posts = tools.load_pickle(self.io_path + "Posts with bug/" + current_year + "\\"
                                          + current_year + "-" + current_month)
            for b_post in bug_posts:
                if b_post[0] == "1":
                    tags = [x.replace(">", "") for x in b_post[2].split("<")[1:]]
                    post_id = b_post[3]
                    score = int(b_post[8])
                    date_obj = parse(b_post[7])
                    self.save_to_tag_dict(tags, post_id, score, date_obj)

                if b_post[0] == "2":
                    result = self.collection.find_one({"Q_id": int(b_post[2])})
                    if result:
                        post_id = b_post[3]
                        score = int(b_post[6])
                        date_obj = parse(b_post[5])
                        try:
                            self.save_to_tag_dict(result["Tags"], post_id, score, date_obj)
                        except Exception as e:
                            print()
            current_date_obj += self.month_delta
        tools.save_pickle(self.io_path + "Posts with bug/tag_to_info", self.tag_to_info)


if __name__ == "__main__":
    # archiver = QuestionArchiver()
    # archiver.create_tag_database()
    # a = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\SO_New\IO_Files\Questions\Archives\Qid_to_tag_list")
    # input("a")
    # matcher = TagBugMatcher()
    # matcher.match_bug_and_tag()
    a = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\SO_Bugs\I_O\Posts with bug\tag_to_info")
    print()
