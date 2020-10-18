from sqlalchemy import create_engine

class SqlApi:
    def __init__(self):
        self.engine = create_engine("mysql://fydata:money@localhost/fypy")

    def init_db(self):
        conn = self.engine.connect()
        

    