from sqlalchemy import ForeignKey, Column, Integer, String, BigInteger, Numeric, Index
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class BaciTradeRow(Base):
    __tablename__ = 'baci_trade_rows'
    year = Column(Integer, primary_key=True)
    exporter_code = Column(String, ForeignKey('baci_countries.code'), primary_key=True)
    importer_code = Column(String, ForeignKey('baci_countries.code'), primary_key=True)
    product_code = Column(String, ForeignKey('baci_products.code'), primary_key=True)
    dollar_value = Column(BigInteger, nullable=False)
    metric_tonne_quantity = Column(Numeric(precision=20, scale=3), nullable=False)
    importer = relationship("BaciCountry", foreign_keys=[importer_code])
    exporter = relationship("BaciCountry", foreign_keys=[exporter_code])
    product = relationship("BaciProduct", foreign_keys=[product_code])
    
    # Indexes for trade balance calculations
    __table_args__ = (
        Index('idx_export_aggregation', 'product_code', 'year', 'exporter_code'),
        Index('idx_import_aggregation', 'product_code', 'year', 'importer_code'),
    )


class BaciProduct(Base):
    __tablename__ = 'baci_products'
    code = Column(String, primary_key=True)
    description = Column(String, nullable=False)
    trade_rows = relationship("BaciTradeRow", back_populates="product", order_by=BaciTradeRow.year)


class BaciCountry(Base):
    __tablename__ = 'baci_countries'
    code = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    iso2 = Column(String, nullable=False)
    iso3 = Column(String, nullable=False)
    import_trade_rows = relationship("BaciTradeRow", back_populates="importer", order_by=BaciTradeRow.year, foreign_keys=[BaciTradeRow.importer_code])
    export_trade_rows = relationship("BaciTradeRow", back_populates="exporter", order_by=BaciTradeRow.year, foreign_keys=[BaciTradeRow.exporter_code])
