import os
import xml.etree.ElementTree as eTree
from xml.etree.ElementTree import ParseError
from Tool_Pack import tools
# library = python-dateutil
from dateutil.parser import parse
from collections import defaultdict

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta


class StackParser:
    def __init__(self):
        self.folder_path = "/home/iraklis/Desktop/Stack Overflow/"
        self.io_path = "/home/iraklis/PycharmProjects/SO_New/IO_Files/Questions/"
        self.users_dict = dict()
        self.incremental_end_signal = 1

    def parse_posts2(self, number_of_posts_to_process, number_of_processed_posts, batch_id):
        self.incremental_end_signal = 0
        with open(self.folder_path + "Posts.xml") as xml_file:
            # Skipping the posts that have already been parsed
            for i in range(number_of_processed_posts):
                next(xml_file)
            post_date_dict = defaultdict(list)
            post_count = 0
            for line in xml_file:
                if post_count % 1000000 == 0:
                    print(post_count)
                post_count += 1
                try:
                    post_info = eTree.fromstring(line)

                except UnicodeDecodeError as ue:
                    encoded_line = line.encode("latin-1", "ignore")
                    post_info = eTree.fromstring(encoded_line)

                except ParseError as pe:
                    print("Parse error occurred " + line)
                    break
                # The owner is the user that created the post.
                if "OwnerUserId" in post_info.attrib:
                    owner = post_info.attrib["OwnerUserId"]
                else:
                    owner = -99
                post_date_obj = parse(post_info.attrib["CreationDate"])
                year_month = str(post_date_obj.year) + "-" + str(post_date_obj.month)

                # It is a question
                if post_info.attrib["PostTypeId"] == "1":
                    post_date_dict[year_month].append((post_info.attrib["PostTypeId"],
                                                       owner,
                                                       post_info.attrib["Tags"],
                                                       post_info.attrib["Id"],
                                                       post_info.attrib["Title"],
                                                       post_info.attrib["Body"],
                                                       post_info.attrib["ViewCount"],
                                                       post_info.attrib["CreationDate"],
                                                       post_info.attrib["Score"],
                                                       post_info.attrib["AnswerCount"]))

                # It is an answer
                elif post_info.attrib["PostTypeId"] == "2":
                    post_date_dict[year_month].append((post_info.attrib["PostTypeId"],
                                                       owner,
                                                       post_info.attrib["ParentId"],
                                                       post_info.attrib["Id"],
                                                       post_info.attrib["Body"],
                                                       post_info.attrib["CreationDate"],
                                                       post_info.attrib["Score"]))

                if post_count > number_of_posts_to_process:
                    self.incremental_end_signal = 1
                    break
            tools.save_pickle("/home/iraklis/PycharmProjects/SO_New/IO_Files/"
                              "Questions/Temp files/posts_dict_" + str(batch_id), post_date_dict)
            # print(post_count)

        # for year_month, post_list in post_date_dict.items():
        #     tools.save_pickle("/home/iraklis/PycharmProjects/SO_New/IO_Files/Questions/Parsed stuff/" +
        #                       year_month + " " + batch_id, post_list)

    def incremental_parsing_posts(self):
        batch_size = 1721540
        total_parsed_posts = 56000008
        iteration_id = 0
        while self.incremental_end_signal == 1:
            print("iteration id " + str(iteration_id))
            self.parse_posts2(batch_size, total_parsed_posts, 29)
            iteration_id += 1
            total_parsed_posts += batch_size

    def group_per_year(self, current_year, current_year_month):
        if not os.path.exists(self.io_path + "Parsed stuff/" + current_year):
            os.makedirs(self.io_path + "Parsed stuff/" + current_year)
        current_post_list = list()
        for i in range(30):
            so_posts = tools.load_pickle("/home/iraklis/PycharmProjects/SO_New/IO_Files/"
                                         "Questions/Temp files/posts_dict_" + str(i))
            if current_year_month in so_posts:
                current_post_list += so_posts[current_year_month]
        tools.save_pickle(self.io_path + "Parsed stuff/" + current_year + "/"
                          + current_year_month, current_post_list)

    def iterate_dates(self):
        start_date_obj = parse("2014-12-01T00:00:00.000")
        end_date_obj = parse("2022-12-01T00:00:00.000")
        month_delta = relativedelta(months=1)
        current_date_obj = start_date_obj
        while current_date_obj <= end_date_obj:
            current_year = current_date_obj.strftime("%Y")
            current_year_month = current_date_obj.strftime("%Y-%-m")
            self.group_per_year(current_year, current_year_month)
            current_date_obj += month_delta


if __name__ == "__main__":
    # scikit - learn
    s_parser = StackParser()
    # s_parser.parse_users()
    # s_parser.parse_posts2()
    # s_parser.incremental_parsing_posts()
    # s_parser.group_per_year(2008)
    s_parser.iterate_dates()
    # a = tools.load_pickle("/home/iraklis/PycharmProjects/SO_New/IO_Files/Questions/Parsed stuff/2014/2014-11")

    print()