import core
import report_db
import time


def main():
    config = core.read_json("/path/to/file/config.json")  # Представим, что мы получили json после POST запроса
    product_names = core.read_our_csv(config["file_path"])
    db = report_db.Database()
    check = db.execute_request("""SELECT * FROM product_names""")
    if not check:
        db.execute_request("""CREATE TABLE product_names (brand text, product_name_hashes text)""")
        db.executemany_request("""INSERT INTO product_names VALUES (?, ?)""", product_names)

    for config in config["data"]:
        core.make_report(db, config)
        print("Wait 5 seconds")
        time.sleep(5)


if __name__ == "__main__":
    main()
