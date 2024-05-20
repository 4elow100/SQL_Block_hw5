import json
import os
import sqlalchemy
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

Base = declarative_base()


class Publisher(Base):
    __tablename__ = 'publisher'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), nullable=False)


class Book(Base):
    __tablename__ = 'book'

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=80), nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey('publisher.id'), nullable=False)
    publisher = relationship(Publisher, backref="books")


class Shop(Base):
    __tablename__ = 'shop'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), nullable=False)


class Stock(Base):
    __tablename__ = 'stock'

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey('book.id'), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey('shop.id'), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)
    book = relationship(Book, backref='stocks')
    shop = relationship(Shop, backref='stocks')


class Sale(Base):
    __tablename__ = 'sale'

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float, nullable=False)
    date_sale = sq.Column(sq.DateTime, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey('stock.id'), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)
    stock = relationship(Stock, backref='sales')


def create_tables():
    user = 'postgres'
    password = '1253'
    host = 'localhost'
    port = '5432'
    database = 'netology_db'
    DSN = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = sqlalchemy.create_engine(DSN)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def create_data(session):
    with open(f"{os.getcwd()}/data.json") as f:
        data = json.load(f)

    for model in data:
        match model['model']:
            case 'publisher':
                query = Publisher(id=model['pk'],
                                  name=model['fields']['name'])
            case 'book':
                query = Book(id=model['pk'],
                             title=model['fields']['title'],
                             id_publisher=model['fields']['id_publisher'],)
            case 'shop':
                query = Shop(id=model['pk'],
                             name=model['fields']['name'])
            case 'stock':
                query = Stock(id=model['pk'],
                              id_book=model['fields']['id_book'],
                              id_shop=model['fields']['id_shop'],
                              count=model['fields']['count'])
            case 'sale':
                query = Sale(id=model['pk'],
                             price=model['fields']['price'],
                             date_sale=model['fields']['date_sale'],
                             id_stock=model['fields']['id_stock'],
                             count=model['fields']['count'])
        session.add(query)
    session.commit()


def select_data(session, publisher):
    if publisher.isdigit():
        query = session.query(Publisher).join(Book.publisher).filter(Publisher.id == publisher)
    else:
        query = session.query(Publisher).join(Book.publisher).filter(Publisher.name == publisher)
    for pub in query.all():
        for book in pub.books:
            for stock in book.stocks:
                for sale in stock.sales:
                    for shop in session.query(Shop).join(Shop.stocks).filter(Shop.id == stock.id_shop):
                        print(f'{book.title} | {shop.name} |{sale.price} | {sale.date_sale.date()}')


session = create_tables()
create_data(session)
publisher = input('Введите имя или идентификатор автора: ')
select_data(session, publisher)


