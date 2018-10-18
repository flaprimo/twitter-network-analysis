from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# engine = create_engine('sqlite:///output/db/db.db')
engine = create_engine('sqlite:///:memory:', echo=True)

def get_session():
    return sessionmaker(bind=engine)