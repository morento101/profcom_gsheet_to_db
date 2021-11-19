from sqlalchemy import Column, String, ForeignKey, Integer, Text, DateTime, Table, MetaData
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import datetime

Base = declarative_base()
meta = MetaData()


directions = Table(
    'directions', meta,
    Column('id', Integer, primary_key=True),
    Column('name', String(20)),
)

# class Direction(Base):
#     __tablename__ = 'direction'
#
#     id = Column(Integer, primary_key=True)
#     name = Column(String(90))


# class Vacancy(Base):
#     __tablename__ = 'vacancies'
#
#     id = Column(Integer, primary_key=True)
#     company_name = Column(String(45))
#     company_short_description = Column(Text, nullable=True)
#     company_direction_id = Column(Integer, ForeignKey('direction.id'))
#     vacancy_name = Column(String(90))
#     vacancy_description = Column(Text)
#     vacancy_requirements = Column(Text)
#     vacancy_working_conditions = Column(Text)
#     vacancy_salary = Column(String(20))
#     vacancy_benefits = Column(Text)
#     vacancy_contacts = Column(Text)
#     company_website = Column(Text)
#     vacancy_date_added = Column(DateTime, default=datetime.datetime.now())
#     direction = relationship('Direction')


direction_names = (
    'IT, комп\'ютери, інтернет',
    'Адмiнiстрацiя, керівництво середньої ланки',
    'Будівництво, архітектура',
    'Бухгалтерія, аудит, секретаріат, діловодство, АГВ',
    'Готельно-ресторанний бізнес, туризм, сфера обслуговування',
    'Дизайн, творчість',
    'ЗМІ, видавництво, поліграфія',
    'Краса, фітнес, спорт',
    'Культура, музика, шоу-бізнес',
    'Логістика, склад, ЗЕД',
    'Маркетинг, реклама, PR, телекомунікації та зв\'язок',
    'Медицина, фармацевтика',
    'Нерухомість',
    'Освіта, наука',
    'Охорона, безпека',
    'Продаж, закупівля',
    'Робочі спеціальності, виробництво',
    'Роздрібна торгівля',
    'Сільське господарство, агробізнес',
    'Транспорт, автобізнес',
    'Фінанси, банк',
    'Управління персоналом, HR',
    'Юриспруденція',
)
