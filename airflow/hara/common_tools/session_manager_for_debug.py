from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from functools import wraps
from contextlib import contextmanager


# Context manager to create and manage sessions
@contextmanager
def create_session():
    DATABASE_URL = 'sqlite:///example.db'
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)

    session = Session()
    try:
        yield session
        session.commit()  # Commit if no exceptions
    except Exception as e:
        session.rollback()  # Rollback in case of error
        raise e  # Reraise the exception after rollback
    finally:
        session.close()  # Always close the session


# Decorator to provide a session if not passed
def provide_session_for_debug(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Check if session is already passed in kwargs
        session = kwargs.get('session', None)

        if session is None:
            # No session provided, create one using context manager
            with create_session() as session:
                # Pass session to the function as keyword argument
                kwargs['session'] = session
                return func(*args, **kwargs)
        else:
            # Session already provided, just call the function
            return func(*args, **kwargs)

    return wrapper
