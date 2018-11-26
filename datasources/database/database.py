from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datasources.database.model import Base
from contextlib import contextmanager
import os

db_path = 'output/db.db'
if os.path.exists(db_path):
    os.remove(db_path)
engine = create_engine('sqlite:///' + db_path)
# engine = create_engine('sqlite:///:memory:', echo=True)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
