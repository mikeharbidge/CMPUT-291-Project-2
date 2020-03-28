from bsddb3 import db

REVIEWS_DB = "CMPUT-291-Project-2/reviews.db"
REVIEWS_TXT = "CMPUT-291-Project-2/reviews.txt"
PTERMS_DB = "CMPUT-291-Project-2/pterms.db"
PTERMS_TXT = "CMPUT-291-Project-2/pterms-sorted.txt"
RTERMS_DB = "CMPUT-291-Project-2/rterms.db"
RTERMS_TXT = "CMPUT-291-Project-2/rterms-sorted.txt"
SCORES_DB = "CMPUT-291-Project-2/scores.db"
SCORES_TXT = "CMPUT-291-Project-2/scores-sorted.txt"


def create_database(dbPath, txtPath, model):
    database = db.DB()
    reviews_tfile = open(txtPath, 'r')
    database.open(dbPath, None, model, db.DB_CREATE)
    cursor = database.cursor()
    review_line = reviews_tfile.readline()
    while review_line:
        review = review_line.split(",", 1)
        database.put(review[0].encode("utf-8"), review[1].encode("utf-8"))
        review_line = reviews_tfile.readline()
    reviews_tfile.close()
    cursor.close()
    database.close()


if __name__ == "__main__":
    create_database(REVIEWS_DB, REVIEWS_TXT, db.DB_HASH)
    create_database(PTERMS_DB, PTERMS_TXT, db.DB_BTREE)
    create_database(SCORES_DB, SCORES_TXT, db.DB_BTREE)
    create_database(RTERMS_DB, RTERMS_TXT, db.DB_BTREE)