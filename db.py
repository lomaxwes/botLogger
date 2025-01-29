import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, inspect
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError


load_dotenv()

Base = declarative_base()

DB_NAME = os.getenv("DB_NAME")

syncEngine = create_engine(DB_NAME, connect_args={"check_same_thread": False})

SessionLocal = sessionmaker(syncEngine, autocommit=False, autoflush=False)

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
    inspector = inspect(syncEngine)
    if not inspector.has_table("users"):
        try:
            Base.metadata.create_all(syncEngine)
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

  








