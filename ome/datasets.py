"""Module to implement ORM for the experimental portion of the OME database"""

from ome.base import *

from sqlalchemy.orm import relationship, backref, column_property
from sqlalchemy import Table, MetaData, create_engine, Column, Integer, \
    String, Float, ForeignKey, ForeignKeyConstraint, PrimaryKeyConstraint, \
    select, and_, or_
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql.expression import join
from sqlalchemy import func

from math import ceil
import simplejson as json


class Dataset(Base):
    """
    A light weight dataset object that is unique on name and allows
    for optional storage of JSON MetaData.
    """
    __tablename__ = 'dataset'

    id = Column(Integer, Sequence('wids'), primary_key=True)
    name = Column(String(100), primary_key=True)
    description = Column(String)
    metadata = Column(JSON)
    type = Column(String(40))

    __mapper_args__ = {'polymorphic_identity': 'dataset',
                       'polymorphic_on': type}

    __table_args__ = (UniqueConstraint('name'),{})

    def __repr__(self):
        return "Data Set (#%d):  %s" % \
            (self.id, self.name)


class GenomeData(Base):
    __tablename__ = 'genome_data'

    dataset_id = Column(Integer, ForeignKey('dataset.id', ondelete="CASCADE"), primary_key=True)
    dataset = relationship('Dataset')
    genome_region_id = Column(Integer, ForeignKey('genome_region.id'), primary_key=True)
    genome_region = relationship('GenomeRegion', backref='data')
    value = Column(Float)
    type = Column(String(20))

    __table_args__ = (UniqueConstraint('dataset_id','genome_region_id'),{})

    @hybrid_property
    def all_data(self):
        return [x['value'] for x in query_genome_data([self.dataset_id], self.genome_region.leftpos, self.genome_region.rightpos)]


    __mapper_args__ = {'polymorphic_identity': 'genome_data',
                       'polymorphic_on': type}

    def __repr__(self):
        return "%s: %5.2f -- %s" % \
            (self.genome_region, self.value, self.dataset.name)



class ReactionData(Base):
    __tablename__ = 'reaction_data'

    dataset_id = Column(Integer, ForeignKey('dataset.id', ondelete="CASCADE"), primary_key=True)
    dataset = relationship('Dataset')
    reaction_id = Column(Integer, ForeignKey('reaction.id'), primary_key=True)
    reaction = relationship('Reaction', backref='data')
    value = Column(Float)
    type = Column(String(20))

    __table_args__ = (UniqueConstraint('dataset_id','reaction_id'),{})


    __mapper_args__ = {'polymorphic_identity': 'reaction_data',
                       'polymorphic_on': type}

    def __repr__(self):
        return "%s: %5.2f -- %s" % \
            (self.reaction, self.value, self.dataset.name)


class MetaboliteData(Base):
    __tablename__ = 'metabolite_data'

    dataset_id = Column(Integer, ForeignKey('dataset.id', ondelete="CASCADE"), primary_key=True)
    dataset = relationship('Dataset')
    metabolite_id = Column(Integer, ForeignKey('metabolite.id'), primary_key=True)
    metabolite = relationship('Metabolite', backref='data')
    value = Column(Float)
    type = Column(String(20))

    __table_args__ = (UniqueConstraint('dataset_id','metabolite_id'),{})


    __mapper_args__ = {'polymorphic_identity': 'metabolite_data',
                       'polymorphic_on': type}

    def __repr__(self):
        return "%s: %5.2f -- %s" % \
            (self.metabolite, self.value, self.dataset.name)

