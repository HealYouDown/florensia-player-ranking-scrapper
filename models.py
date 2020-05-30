from sqlalchemy import Column, ForeignKey, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Guild(Base):
    __tablename__ = "guild"
    id = Column(Integer, primary_key=True)
    name = Column(String, primary_key=True)
    name_hash = Column(String, unique=True)
    server = Column(String)
    members = relationship("Player")
    number_of_members = Column(Integer)
    avg_rank = Column(Float)


class Player(Base):
    __tablename__ = "player"
    id = Column(Integer, primary_key=True, autoincrement=True)
    rank = Column(Integer)
    name = Column(String)
    class_ = Column(String)
    level_land = Column(Integer)
    level_sea = Column(Integer)
    server = Column(String)
    guild = Column(String, ForeignKey("guild.name"))
