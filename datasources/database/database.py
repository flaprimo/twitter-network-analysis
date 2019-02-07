from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datasources.database.model import Base
from contextlib import contextmanager
import os


class Database:
    def __init__(self, output_path, db_name='database', reset_db=False):
        output_db_dir = os.path.join(output_path, 'db')
        self.output_db_path = os.path.join(output_db_dir, f'{db_name}.db')

        if not os.path.exists(output_db_dir):
            os.makedirs(output_db_dir)
        elif os.path.isfile(self.output_db_path) and reset_db:
            os.remove(self.output_db_path)

        self.engine = create_engine('sqlite:///' + self.output_db_path)
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
