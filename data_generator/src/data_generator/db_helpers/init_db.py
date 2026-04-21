import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base


def init_db(db_path: str = './database.db'):
    """Initialize database and create all tables."""
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)
    print(f"Database initialized at: {Path(db_path).resolve().as_posix()}")


def get_engine(db_path: str = './database.db'):
    """Get SQLAlchemy engine for the database."""
    # Make the directory if it does not exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    engine = create_engine(f'sqlite:///{db_path}')
    return engine


def get_session(db_path: str = './database.db'):
    """Get a SQLAlchemy session for database operations."""
    engine = get_engine(db_path)
    Session = sessionmaker(bind=engine)
    return Session()
