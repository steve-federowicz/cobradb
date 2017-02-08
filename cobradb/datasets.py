"""Module to implement ORM for the experimental portion of the OME database"""

from cobradb.base import (Base, DataSource, Session)
from cobradb.util import get_or_create

from sqlalchemy.orm import relationship
from sqlalchemy import (Table, Column, Integer, String, Float, ForeignKey)
from sqlalchemy.schema import UniqueConstraint, Sequence
from sqlalchemy.types import JSON, Text



class Dataset(Base):
    """
    Core table which functions as a flexible template for polymorphic
    analysis tables/classes. Currently errors on the side of
    storing variable metadata in a JSON field instead of extensively
    typing specific analyses.
    """
    __tablename__ = 'dataset'

    id = Column(Integer, Sequence('wids'), primary_key=True)
    name = Column(String(100))
    type = Column(String(40))

    group_name = Column(Text)
    attributes = Column(JSON)

    data_source_id = Column(Integer, ForeignKey('data_source.id', ondelete='CASCADE'))
    data_source = relationship("DataSource")

    __mapper_args__ = {'polymorphic_identity': 'dataset',
                       'polymorphic_on': type}

    __table_args__ = (UniqueConstraint('name', 'group_name'), {})

    def __repr__(self):
        return "Data Set (#%d):  %s" % \
            (self.id, self.name)


    def __init__(self, name, data_source_id=None, group_name=None, attributes=None):

        session = Session()
        if data_source_id is None:
            data_source, exists = get_or_create(session, DataSource, cobra_id='-1',
                                                name='generic', url_prefix='')
            data_source_id = data_source.id
        session.close()

        self.name = name
        self.data_source_id = data_source_id
        self.group_name = group_name
        self.attributes = attributes


class AnalysisComposition(Base):
    """
    A many to many table which allows for graph-like nesting of analysis
    objects.
    """
    __tablename__ = 'analysis_composition'

    analysis_id = Column(Integer, ForeignKey('analysis.id'), primary_key=True)
    dataset_id = Column(Integer, ForeignKey('dataset.id'), primary_key=True)

    __table_args__ = (UniqueConstraint('analysis_id', 'dataset_id'), {})

    def __init__(self, analysis_id, dataset_id):
        self.analysis_id = analysis_id
        self.dataset_id = dataset_id


class Analysis(Dataset):
    """
    Base class for analysis types which inerits from Dataset and provides
    convenience methods to retrieve analysis history.
    """
    __tablename__ = 'analysis'

    id = Column(Integer, ForeignKey('dataset.id', ondelete='CASCADE'), primary_key=True)
    type = Column(String(40))
    children = relationship("Dataset", secondary="analysis_composition",
                            primaryjoin=id == AnalysisComposition.analysis_id,
                            backref="parent")

    __mapper_args__ = {'polymorphic_identity': 'analysis',
                       'polymorphic_on': 'type'}

    def __init__(self, name, group_name=None, meta_data=None):
        super(Analysis, self).__init__(name, group_name=group_name, meta_data=meta_data)

    def __repr__(self):
        return "Analysis (#%d):  %s" % \
            (self.id, self.name)


class GenomeData(Base):
    """
    Class/table that maps a dataset to genome_region for storage of data relative to
    any genome_region.
    """
    __tablename__ = 'genome_data'

    dataset_id = Column(Integer, ForeignKey('dataset.id', ondelete="CASCADE"), primary_key=True)
    dataset = relationship('Dataset')
    genome_region_id = Column(Integer, ForeignKey('genome_region.id'), primary_key=True)
    genome_region = relationship('GenomeRegion', backref='data')
    value = Column(Float)
    type = Column(String(20))

    __table_args__ = (UniqueConstraint('dataset_id', 'genome_region_id'), {})


    __mapper_args__ = {'polymorphic_identity': 'genome_data',
                       'polymorphic_on': type}

    def __repr__(self):
        return "%s: %5.2f -- %s" % \
            (self.genome_region, self.value, self.dataset.name)


    def __init__(self, dataset_id, genome_region_id, value):
        self.dataset_id = dataset_id
        self.genome_region_id = genome_region_id
        self.value = value
