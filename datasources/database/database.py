from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, scoped_session
from datasources.database.model import Base
from contextlib import contextmanager
import os


class Database:
    def __init__(self, output_path, db_name='database', reset_db=True):
        output_db_dir = os.path.join(output_path, 'db')
        self.output_db_path = os.path.join(output_db_dir, f'{db_name}.db')

        if not os.path.exists(output_db_dir):
            os.makedirs(output_db_dir)
        elif os.path.isfile(self.output_db_path) and reset_db:
            os.remove(self.output_db_path)

        self.engine = create_engine('sqlite:///' + self.output_db_path)
        Base.metadata.create_all(self.engine)
        self.session_factory = sessionmaker(bind=self.engine)
        self.session = scoped_session(self.session_factory)

    @contextmanager
    def session_scope(self):
        # Provide a transactional scope around a series of operations
        session = self.session()
        try:
            yield session
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            raise
        finally:
            session.close()
