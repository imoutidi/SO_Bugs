import os
import re

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from Tool_Pack import tools


class Scanner:
    def __init__(self):
        self.start_date_obj = parse("2008-08-01T00:00:00.000")
        self.end_date_obj = parse("2022-12-01T00:00:00.000")
        self.io_path = r"C:\Users\irmo\PycharmProjects\SO_Bugs\I_O\\"
        self.month_delta = relativedelta(months=1)

    def iterate_dates(self):
        current_date_obj = self.start_date_obj
        while current_date_obj <= self.end_date_obj:
            print(current_date_obj.strftime("%Y-%m"))
            current_year = current_date_obj.strftime("%Y")
            if not os.path.exists(self.io_path + "Parsed stuff/" + current_year):
                os.makedirs(self.io_path + "Parsed stuff/" + current_year)
            current_month = current_date_obj.strftime("%m")
            if len(current_month) == 2:
                if current_month[0] == '0':
                    current_month = current_month.replace("0", "")
            self.scan_for_keyword(current_year, current_month)
            current_date_obj += self.month_delta

    def scan_for_keyword(self, c_year, c_month):
        if not os.path.exists(self.io_path + "Posts with bug/" + c_year):
            os.makedirs(self.io_path + "Posts with bug/" + c_year)
        bug_list = list()
        month_posts = tools.load_pickle(self.io_path + "Parsed stuff/" + c_year + "\\" + c_year + "-" + c_month)
        pattern = r"(?<![^\W\d_])bug(?![^\W\d_])|([^\w\s]|^)bug([^\w\s]|$)"
        for post in month_posts:
            a = None
            if post[0] == "1":
                a = re.search(pattern, post[5])
            if post[0] == "2":
                a = re.search(pattern, post[4])
            if a != None:
                bug_list.append(post)
        tools.save_pickle(self.io_path + "Posts with bug/" + c_year + "/"
                          + c_year + "-" + c_month, bug_list)

    def count_bugs(self):
        current_date_obj = self.start_date_obj
        total_bugs = 0
        max_month = ["month", 0]
        min_month = ["month", 500]
        with open(self.io_path + "Posts with bug/report", "a") as report_file:
            while current_date_obj <= self.end_date_obj:
                print(current_date_obj.strftime("%Y-%m"))
                current_year = current_date_obj.strftime("%Y")
                current_month = current_date_obj.strftime("%m")
                if len(current_month) == 2:
                    if current_month[0] == '0':
                        current_month = current_month.replace("0", "")
                month_bugs = tools.load_pickle(self.io_path + "Posts with bug/" + current_year + "\\"
                                               + current_year + "-" + current_month)
                report_file.write(current_year + "-" + current_month + ": " + str(len(month_bugs)) + "\n")
                total_bugs += len(month_bugs)
                if max_month[1] < len(month_bugs):
                    max_month[0] = current_year + "-" + current_month
                    max_month[1] = len(month_bugs)
                if min_month[1] > len(month_bugs):
                    min_month[0] = current_year + "-" + current_month
                    min_month[1] = len(month_bugs)
                current_date_obj += self.month_delta
            report_file.write("\nTotal number of bugs: " + str(total_bugs) + "\n")
            report_file.write("Month with most bugs: " + str(max_month) + "\n")
            report_file.write("Month with least bugs: " + str(min_month))


if __name__ == "__main__":
    bug_scanner = Scanner()
    bug_scanner.iterate_dates()
    bug_scanner.count_bugs()