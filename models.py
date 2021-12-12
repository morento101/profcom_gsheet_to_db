from sqlalchemy import Column, String, ForeignKey, Integer, Text, DateTime, MetaData
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import datetime

Base = declarative_base()
meta = MetaData()


class Direction(Base):
    __tablename__ = 'directions'

    id = Column(Integer, primary_key=True)
    name = Column(String(120))
    vacancy = relationship('Vacancy')


class Vacancy(Base):
    __tablename__ = 'vacancies'

    id = Column(Integer, primary_key=True)
    company_name = Column(String(120))
    company_short_description = Column(Text, nullable=True)
    company_direction_id = Column(Integer, ForeignKey(Direction.id, ondelete='RESTRICT'))
    vacancy_name = Column(String(120))
    vacancy_description = Column(Text)
    vacancy_requirements = Column(Text)
    vacancy_working_conditions = Column(Text)
    vacancy_salary = Column(String(200))
    vacancy_benefits = Column(Text)
    vacancy_contacts = Column(Text)
    company_website = Column(Text, nullable=True)
    degree = Column(String(120))
    minimal_english_level = Column(String(20))
    working_time = Column(String(120))
    working_experience = Column(String(120))
    vacancy_date_added = Column(DateTime, default=datetime.datetime.now())


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
