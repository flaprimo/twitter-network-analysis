from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, Boolean, Date, ForeignKey, CheckConstraint
from sqlalchemy.orm import relationship

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    user_name = Column(String(20), unique=True)
    join_date = Column(Date)

    profile = relationship('Profile', uselist=False, back_populates='user',
                           cascade='all, delete-orphan', single_parent=True)
    user_communities = relationship('UserCommunity', back_populates='user',
                                    cascade='all, delete-orphan', single_parent=True)

    def __repr__(self):
        return f'<User(user_name={self.user_name}, join_date={self.join_date})>'


class Profile(Base):
    __tablename__ = 'profiles'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    rank = Column(Float, CheckConstraint('rank>=0'))

    user = relationship('User', back_populates='profile')

    def __repr__(self):
        return f'<User(user={self.user}, rank={self.rank})>'


class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True)
    name = Column(String(20), unique=True)
    start_date = Column(Date)
    end_date = Column(Date)
    location = Column(String(20))
    hashtags = Column(String(20))

    graph = relationship('Graph', uselist=False, back_populates='event',
                         cascade='all, delete-orphan', single_parent=True)

    def __repr__(self):
        return f'<Event(' \
            f'name={self.name}, ' \
            f'start_date={self.start_date}, ' \
            f'end_date={self.end_date}, ' \
            f'location={self.location}, ' \
            f'hashtags={self.hashtags})>'


class Graph(Base):
    __tablename__ = 'graphs'

    id = Column(Integer, primary_key=True)
    event_id = Column(Integer, ForeignKey('events.id'))
    no_nodes = Column(Integer, CheckConstraint('no_nodes>=0'))
    no_edges = Column(Integer, CheckConstraint('no_edges>=0'))
    avg_degree = Column(Float, CheckConstraint('avg_degree>=0'))
    avg_weighted_degree = Column(Float, CheckConstraint('avg_weighted_degree>=0'))
    density = Column(Float, CheckConstraint('density>=0'))
    connected = Column(Boolean)
    strongly_conn_components = Column(Float, CheckConstraint('strongly_conn_components>=0'))
    avg_clustering = Column(Float, CheckConstraint('avg_clustering>=0'))
    assortativity = Column(Float)

    event = relationship('Event', back_populates='graph')
    partition = relationship('Partition', uselist=False, back_populates='graph',
                             cascade='all, delete-orphan', single_parent=True)

    def __repr__(self):
        return f'<Graph(' \
            f'no_nodes={self.no_nodes}, ' \
            f'no_edges={self.no_edges}, ' \
            f'avg_degree={self.avg_degree}, ' \
            f'avg_degree={self.avg_degree}, ' \
            f'avg_weighted_degree={self.avg_weighted_degree}, ' \
            f'density={self.density}, ' \
            f'connected={self.connected}, ' \
            f'strongly_conn_components={self.strongly_conn_components}, ' \
            f'avg_clustering={self.avg_clustering})>'


class Partition(Base):
    __tablename__ = 'partitions'

    id = Column(Integer, primary_key=True)
    graph_id = Column(Integer, ForeignKey('graphs.id'))
    internal_density = Column(Float, CheckConstraint('internal_density>=0'))
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

    graph = relationship('Graph', back_populates='partition')
    communities = relationship('Community', back_populates='partition', cascade='all, delete-orphan')

    def __repr__(self):
        return f'<Partition(' \
            f'internal_density={self.internal_density}, ' \
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
    partition_id = Column(Integer, ForeignKey('partitions.id'))
    name = Column(Integer, CheckConstraint('name>=0'))

    partition = relationship('Partition', back_populates='communities')
    user_communities = relationship('UserCommunity', back_populates='community',
                                    cascade='all, delete-orphan', single_parent=True)

    def __repr__(self):
        return f'<Community(name={self.name})>'


class UserCommunity(Base):
    __tablename__ = 'user_community'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    community_id = Column(Integer, ForeignKey('communities.id'))
    indegree = Column(Integer, CheckConstraint('indegree>=0'))
    indegree_centrality = Column(Float, CheckConstraint('indegree_centrality>=0'))
    hindex = Column(Integer, CheckConstraint('hindex>=0'))

    user = relationship('User', back_populates='user_communities')
    community = relationship('Community', back_populates='user_communities')

    def __repr__(self):
        return f'<UserCommunity(' \
            f'indegree={self.indegree}, ' \
            f'indegree_centrality={self.indegree_centrality}, ' \
            f'hindex={self.hindex})>'
