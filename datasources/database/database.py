from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datasources.database.model import Base
from contextlib import contextmanager
import os


class Database:
    def __init__(self, db_dir='output/', db_name='database', reset_db=False):
        self.db_path = f'{db_dir}{db_name}.db'

        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
        elif os.path.isfile(self.db_path) and reset_db:
            os.remove(self.db_path)

        self.engine = create_engine('sqlite:///' + self.db_path)
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)

    @contextmanager
    def session_scope(self):
        # Provide a transactional scope around a series of operations
        session = self.session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


# current_path = os.path.dirname(__file__)
# db_path = os.path.join(current_path, 'output/')
db = Database('output/')
