from Tool_Pack import tools
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from collections import defaultdict


class QuestionArchiver:
    def __init__(self):
        self.start_date_obj = parse("2008-08-01T00:00:00.000")
        self.end_date_obj = parse("2022-12-01T00:00:00.000")
        self.io_path = r"C:\Users\irmo\PycharmProjects\SO_New\IO_Files\Questions\\"
        self.month_delta = relativedelta(months=1)
        self.tag_dictionary = dict()

    def create_tag_dictionary(self):
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
                    self.tag_dictionary[m_post[3]] = tags
            current_date_obj += self.month_delta
        tools.save_pickle(self.io_path + "Archives/Qid_to_tag_list", self.tag_dictionary)


if __name__ == "__main__":
    archiver = QuestionArchiver()
    # archiver.create_tag_dictionary()
    a = tools.load_pickle(r"C:\Users\irmo\PycharmProjects\SO_New\IO_Files\Questions\Archives\Qid_to_tag_list")
    input("a")
    print()
