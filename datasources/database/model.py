from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, Boolean, UniqueConstraint, Date, ForeignKey, CheckConstraint
from sqlalchemy.orm import relationship

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    user_name = Column(String(20), unique=True)
    join_date = Column(Date)

    profile = relationship('Profile', uselist=False, back_populates='user')

    def __repr__(self):
        return f'<User(user_name={self.user_name}, user_name={self.join_date})>'


class Profile(Base):
    __tablename__ = 'profiles'

    id = Column(Integer, primary_key=True)
    # user_id = Column(Integer, ForeignKey('user.id'))
    rank = Column(Float, CheckConstraint('rank>=0'))

    user = relationship('User', uselist=False, back_populates='profile')

    def __repr__(self):
        return f'<User(user={self.user}, rank={self.rank})>'


class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True)
    # graph_id = Column(Integer, ForeignKey('graph.id'))
    name = Column(String(20))
    date = Column(Date)
    location = Column(String(20))
    hashtags = Column(String(20))

    graph = relationship('Graph', uselist=False, back_populates='event')

    def __repr__(self):
        return f'<Event(' \
            f'name={self.name}, ' \
            f'date={self.date}, ' \
            f'location={self.location}, ' \
            f'hashtags={self.hashtags})>'


class Graph(Base):
    __tablename__ = 'graphs'

    id = Column(Integer, primary_key=True)
    # event_id = Column(Integer, ForeignKey('event.id'))
    # partition_id = Column(Integer, ForeignKey('partition.id'))
    no_nodes = Column(Integer, CheckConstraint('no_nodes>=0'))
    no_edges = Column(Integer, CheckConstraint('no_edges>=0'))
    avg_degree = Column(Float, CheckConstraint('avg_degree>=0'))
    avg_weight_degree = Column(Float, CheckConstraint('avg_weight_degree>=0'))
    density = Column(Float, CheckConstraint('density>=0'))
    connected = Column(Boolean)
    strongly_conn_component = Column(Float, CheckConstraint('strongly_conn_component>=0'))
    avg_clustering = Column(Float, CheckConstraint('avg_clustering>=0'))
    assortativity = Column(Float, CheckConstraint('assortativity>=0'))

    event = relationship('Event', uselist=False, back_populates='graph')
    partition = relationship('Partition', uselist=False, back_populates='graph')

    def __repr__(self):
        return f'<Graph(' \
            f'no_nodes={self.no_nodes}, ' \
            f'no_edges={self.no_edges}, ' \
            f'avg_degree={self.avg_degree}, ' \
            f'avg_degree={self.avg_degree}, ' \
            f'avg_weight_degree={self.avg_weight_degree}, ' \
            f'density={self.density}, ' \
            f'connected={self.connected}, ' \
            f'strongly_conn_component={self.strongly_conn_component}, ' \
            f'avg_clustering={self.avg_clustering})>'


class Partition(Base):
    __tablename__ = 'partitions'

    id = Column(Integer, primary_key=True)
    # graph_id = Column(Integer, ForeignKey('graph.id'))
    internal_degree = Column(Float, CheckConstraint('internal_degree>=0'))
    edges_inside = Column(Float, CheckConstraint('edges_inside>=0'))
    normalized_cut = Column(Float, CheckConstraint('normalized_cut>=0'))
    avg_degree = Column(Float, CheckConstraint('avg_degree>=0'))
    fomd = Column(Float, CheckConstraint('fomd>=0'))
    expansion = Column(Float, CheckConstraint('expansion>=0'))
    cut_ratio = Column(Float, CheckConstraint('cut_ratio>=0'))
    conductance = Column(Float, CheckConstraint('conductance>=0'))
    max_odf = Column(Float, CheckConstraint('max_odf>=0'))
    avg_odf = Column(Float, CheckConstraint('avg_odf>=0'))
    flake_odf = Column(Float, CheckConstraint('flake_odf>=0'))

    graph = relationship('Graph', uselist=False, back_populates='partition')
    community = relationship('Community', uselist=False, back_populates='partition')

    def __repr__(self):
        return f'<Partition(' \
            f'internal_degree={self.internal_degree}, ' \
            f'edges_inside={self.edges_inside}, ' \
            f'normalized_cut={self.normalized_cut}, ' \
            f'avg_degree={self.avg_degree}, ' \
            f'fomd={self.fomd}, ' \
            f'expansion={self.expansion}, ' \
            f'cut_ratio={self.cut_ratio}, ' \
            f'conductance={self.conductance}, ' \
            f'max_odf={self.max_odf}, ' \
            f'avg_odf={self.avg_odf}, ' \
            f'flake_odf={self.flake_odf})>'


class Community(Base):
    __tablename__ = 'communities'

    id = Column(Integer, primary_key=True)
    partition_id = Column(Integer, ForeignKey('partition.id'))
    name = Column(Integer, CheckConstraint('name>=0'))

    partition = relationship('Partition', back_populates='community')

    def __repr__(self):
        return f'<Event(name={self.name})>'


class UserCommunity(Base):
    __tablename__ = 'user_community'

    id = Column(Integer, primary_key=True)
    rel_indegree = Column(Float, CheckConstraint('rel_indegree>=0'))
    rel_indegree_centrality = Column(Float, CheckConstraint('rel_indegree_centrality>=0'))
    rel_hindex = Column(Float, CheckConstraint('rel_hindex>=0'))

    user = relationship('User', back_populates='user_community')
    community = relationship('Community', back_populates='user_community')

    def __repr__(self):
        return f'<UserCommunity(' \
            f'rel_indegree={self.rel_indegree}, ' \
            f'rel_indegree_centrality={self.rel_indegree_centrality}, ' \
            f'rel_hindex={self.rel_hindex})>'
