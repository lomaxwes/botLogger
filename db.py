import os
from urllib.parse import quote
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, inspect
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DATABASE_NAME")

encoded_password = quote(db_password)

DATABASE_URL = f"postgresql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}"

Base = declarative_base()

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(engine, autocommit=False, autoflush=False)

class User(Base):
    __tablename__ = 'users'

    userId = Column(Integer, primary_key=True)
    username = Column(String)
    firstName = Column(String)
    lastName = Column(String)

    def __repr__(self):
        return f"<User(userId={self.userId}, username={self.username})>"

class BlockedUser(Base):
    __tablename__ = 'blockUsers'

    userId = Column(Integer, primary_key=True)
    reason = Column(String)

    def __repr__(self):
        return f"<BlockedUser(userId={self.userId}, reason={self.reason})>"

def createTables():
    inspector = inspect(engine)
    if not inspector.has_table("users"):
        try:
            Base.metadata.create_all(engine)
            print("Таблицы успешно созданы!")
        except OperationalError:
            print("Ошибка при создании таблиц.")
    else:
        print("Таблицы уже существуют.")

def checkUserInDb(userId, db):
    return db.query(User).filter(User.userId == userId).first()

def addUserToDb(userId, username, firstName, lastName):
    with SessionLocal() as db:
        try:
            newUser = User(userId=userId, username=username, firstName=firstName, lastName=lastName)
            db.add(newUser)
            db.commit()
            db.refresh(newUser)
        except Exception as e:
            db.rollback()
            print(f"Ошибка при добавлении пользователя: {e}")

def checkUserInBlocked(userId, db):
    return db.query(BlockedUser).filter(BlockedUser.userId == userId).first()

  








