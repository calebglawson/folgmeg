import enum
from sqlalchemy import Column, Integer, Enum, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Status(enum.Enum):
    pending = 1
    following = 2
    mutual = 3
    expired = 4


class ScriptedFollowingStatus(Base):
    __tablename__ = 'scripted_following_status'

    id = Column(Integer, primary_key=True)
    status = Column(Enum(Status), nullable=False)
    next_due = Column(DateTime)


class Follower(Base):
    __tablename__ = 'followers'

    id = Column(Integer, primary_key=True)


class OrganicFollowing(Base):
    __tablename__ = 'organic_following'

    id = Column(Integer, primary_key=True)
