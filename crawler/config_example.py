# place a valid base host with credentials in here
# for example
# db_creds = 'postgresql://username:password@host:5432/'

db_creds = "CHANGE_ME"


def db_uri(db_name):
    return db_creds + db_name
