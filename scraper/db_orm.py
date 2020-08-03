from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Text, Numeric
from sqlalchemy import ForeignKeyConstraint, PrimaryKeyConstraint

Base = declarative_base()


class ProductInfo(Base):
    __tablename__ = 'product_info'

    product_id = Column(Integer, primary_key=True)
    category = Column(String, primary_key=True)
    name = Column(String)
    model = Column(String)
    brand = Column(String)
    supplier = Column(String)
    summary = Column(Text)

    product_status = relationship("ProductStatus", backref='product_info')

    __table_args = (PrimaryKeyConstraint('product_id', 'category'), {})


class ProductStatus(Base):
    __tablename__ = 'product_status'

    status_id = Column(Integer, primary_key=True)
    date = Column(Date)
    price = Column(Numeric)
    quantity = Column(Integer)

    product_id = Column(Integer, nullable=False)
    category = Column(String, nullable=False)

    __table_args__ = (ForeignKeyConstraint(
                            ['product_id', 'category'],
                            ['product_info.product_id', 'product_info.category']),
                      {}
                      )
