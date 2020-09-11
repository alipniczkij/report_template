import sqlite3
import pdb


class Database:
    def __init__(self):
        self.connector = sqlite3.connect("/path/to/file/de_test.db")
        self.cursor = self.connector.cursor()

    def execute_request(self, request):
        try:
            self.cursor.execute(request)
            self.connector.commit()
            return self.cursor.fetchall()
        except sqlite3.OperationalError as e:
            print(e)
            return None

    def executemany_request(self, request, data):
        try:
            self.cursor.executemany(request, data)
            self.connector.commit()
        except sqlite3.OperationalError as e:
            print(e)
            return None


class Config:
    def __init__(self, config):
        self.period = config["period"]
        self.category = config["kkt_category"]
        self.sections = config["sections"]
        self.title = []
        self._select = ""
        self._from_tables = ""
        self._where = ""
        self._group = ""
        self.from_tables()
        self.where()
        self.group()
        self.select()
        self.request = "{} {} {} {}".format(self._select, self._from_tables, self._where, self._group).strip(", ")

    def select(self):
        self._select = "SELECT "
        if self.sections["receipt_date"]:
            self._select += "receipt_date, "
            self.title.append("receipt_date")
        if self.sections["region"]:
            self._select += "region, "
            self.title.append("region")
        if self.sections["channel"]:
            self._select += "CASE WHEN channel_table.count < 3 THEN 'nonchain' WHEN channel_table.count >= 3 THEN 'chain' END channel, "
            self.title.append("channel")
        if self.sections["brand"]:
            self._select += "brand, "
            self.title.append("brand")
        self._select += "SUM(total_sum), ROUND(SUM(total_sum) / CAST((tot_sum.all_sum) AS FLOAT), 2) AS total_sum_pct "
        self.title += ["total_sum", "total_sum_pct"]

    def from_tables(self):
        self._from_tables = "FROM sales JOIN product_names ON sales.product_name_hash = " \
                            "product_names.product_name_hashes JOIN (SELECT SUM(total_sum) as all_sum FROM sales) as " \
                            "tot_sum "
        if any([self.sections[key] if key != "brand" else "" for key in self.sections]) or self.category:
            self._from_tables += "JOIN kkt_info ON sales.kkt_number = kkt_info.kkt_number "
        if self.category:
            self._from_tables += "LEFT JOIN (SELECT kkt_number, category, version FROM kkt_categories WHERE version = " \
                                 "(SELECT MAX(version) FROM kkt_categories) GROUP BY kkt_number) as categories ON " \
                                 "categories.kkt_number = sales.kkt_number "
        if self.sections["channel"]:
            self._from_tables += "JOIN (SELECT org_inn, COUNT(*) AS count, kkt_number FROM kkt_info JOIN kkt_activity " \
                                 "USING (kkt_number) WHERE ((kkt_activity.receipt_date_min BETWEEN '{}' AND " \
                                 "'{}') OR (kkt_activity.receipt_date_max BETWEEN '{}' AND " \
                                 "'{}')) AND ((kkt_info.date_from <= '{}') AND kkt_info.date_till > " \
                                 "'{}') GROUP BY org_inn) as channel_table ON channel_table.kkt_number = sales.kkt_number".format(self.period["date_from"],
                                                                                  self.period["date_to"],
                                                                                  self.period["date_from"],
                                                                                  self.period["date_to"],
                                                                                  self.period["date_from"],
                                                                                  self.period["date_to"])

    def where(self):
        self._where = "WHERE (sales.receipt_date BETWEEN '{}' AND '{}') ".format(self.period["date_from"],
                                                                                 self.period["date_to"])
        if self.category:
            self._where += "AND categories.category = '{}' ".format(self.category)

    def group(self):
        self._group = "GROUP BY product_names.brand, "
        if self.sections["receipt_date"]:
            self._group += "sales.receipt_date, "
        if self.sections["region"]:
            self._group += "kkt_info.region, "
