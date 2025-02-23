import pytest
from sqlalchemy import create_engine, Integer, String, Column
from sqlalchemy.orm import sessionmaker, Session

from main import Base

TEST_DATABASE_URL = "sqlite:///:memory:"

@pytest.fixture(scope="module")
def test_engine():
    engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="function")
def test_session(test_engine):
    connection = test_engine.connect()
    transaction = connection.begin()
    TestingSessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=connection
    )
    session = TestingSessionLocal()
    yield session
    session.close()
    transaction.rollback()
    connection.close()

def test_create_engine(test_engine):
    try:
        with test_engine.connect() as connection:  # Use a context manager
            assert connection is not None  # Check if a connection object is obtained
            assert not connection.closed # Check if the connection is open
    except Exception as e:
        assert False, f"Connection failed: {e}"

def test_create_session(test_session, test_engine):
    assert test_session is not None
    assert isinstance(test_session, Session)
    assert not test_session.bind.closed  # Check if the connection is open
    
    test_session.close()  # Close the session explicitly
    assert test_session.is_active is True



def create_dynamic_table(table_name):
    class DynamicTable(Base):
        __tablename__ = table_name

        id = Column(Integer, primary_key=True, index=True)
        name = Column(String)
        description = Column(String)

    return DynamicTable

def test_dynamic_db_interaction(test_session, test_engine): # test_session and test_engine
    table_name = "my_dynamic_table"
    DynamicTable = create_dynamic_table(table_name)

    DynamicTable.__table__.create(bind=test_engine)  # Create table using test_engine

    item_data = {"name": "Dynamic Item", "description": "A dynamic test item"}

    # Use the injected test_session CORRECTLY:
    new_item = DynamicTable(**item_data)
    test_session.add(new_item)
    test_session.commit()
    test_session.refresh(new_item)

    retrieved_item = test_session.query(DynamicTable).filter_by(name=item_data["name"]).first()

    assert retrieved_item is not None
    assert retrieved_item.name == item_data["name"]
    assert retrieved_item.description == item_data["description"]
    assert retrieved_item.id is not None

    DynamicTable.__table__.drop(bind=test_engine)  # Drop table using test_engine

