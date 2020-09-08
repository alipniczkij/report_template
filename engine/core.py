import json
import csv
import report_db
import pdb


def make_report(db, config):
    conf = report_db.Config(config)
    print("Конфиг: \n", config)
    print("Запрос: \n", conf.request)
    result = db.execute_request(conf.request)
    print("Результат: \n", result)
    write_csv(config["name"], result, conf.title)


def write_csv(filename, rows, title):
    with open("../" + filename, 'w') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(title)
        writer.writerows(rows)


def read_json(file_path):
    with open(file_path, "r") as json_file:
        return json.loads(json_file.read())


def read_our_csv(file_path):
    product_names = []
    with open(file_path, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        for row in reader:
            product_names.append((row[0], row[1]))
    return product_names
