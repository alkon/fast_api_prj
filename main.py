from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.orm.session import Session

app = FastAPI()

DATABASE_URL = "sqlite:///./mydatabase.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    description = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

class ItemCreate(BaseModel):
    name: str
    description: str | None = None

class ItemResponse(BaseModel):
    id: int
    name: str
    description: str | None = None

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/items/", status_code=201)
def create_item(item: ItemCreate, db: Session = Depends(get_db)):
    db_item = Item(**item.dict())
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return ItemResponse(**db_item.__dict__)

@app.get("/items/", response_model=list[ItemResponse])
def read_items(db: Session = Depends(get_db)):
    items = db.query(Item).all()
    return [ItemResponse(**item.__dict__) for item in items]

@app.get("/items/{item_id}", response_model=ItemResponse)
def read_item(item_id: int, db: Session = Depends(get_db)):
    db_item = db.query(Item).filter(Item.id == item_id).first()
    if db_item is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return ItemResponse(**db_item.__dict__)