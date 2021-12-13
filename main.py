import os.path
from loguru import logger

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# Do not remove this import!!!
import psycopg2

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from models import Direction, Vacancy, direction_names, Base
from sqlalchemy.ext.automap import automap_base

# If modifying these scopes, delete the file token.json
SCOPES = ('https://www.googleapis.com/auth/spreadsheets',)

# Data titles
TITLES = (
        'company_name', 'company_short_description', 'company_direction', 'vacancy_name',
        'vacancy_description',  'vacancy_requirements', 'vacancy_working_conditions',
        'vacancy_salary', 'vacancy_benefits', 'vacancy_contacts', 'company_website',
        'degree', 'minimal_english_level', 'working_time', 'working_experience'
)

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '1nU3wT-ywI5ePxhRVEksmxQDem-TJfCbsoZBsBerdnOM'
RANGE_NAME = 'B2:P'


class SheetToDbTransporter:
    """Class for transporting data from google sheet to database"""

    @logger.catch
    def __init__(self, scopes=SCOPES, spreadsheet_id=SPREADSHEET_ID, sheet_range=RANGE_NAME):
        """"Creating connection to Google Sheet API, getting scopes,
        spreadsheet's id and range. Checking if credentials and tokens are valid"""

        self.scopes = scopes
        self.spreadsheet_id = spreadsheet_id
        self.sheet_range = sheet_range
        self.creds = None

        if os.path.exists('access_to_sheet/token.json'):
            self.creds = Credentials.from_authorized_user_file('access_to_sheet/token.json', self.scopes)

        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('access_to_sheet/credentials.json', self.scopes)
                self.creds = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open('access_to_sheet/token.json', 'w') as token:
                token.write(self.creds.to_json())

        # Connect to spreadsheet
        self.service = build('sheets', 'v4', credentials=self.creds)
        self.spreadsheet = self.service.spreadsheets()

    def get_tuple_of_sheets(self):
        """Gets all names of sheets in spreadsheet"""

        # Get data  about spreadsheet
        sheet_metadata = self.spreadsheet.get(spreadsheetId=self.spreadsheet_id).execute()
        g_sheets = sheet_metadata.get('sheets', '')

        titles = (g_sheets[index].get("properties", {}).get("title", "") for index in range(len(g_sheets)))

        return titles

    def get_vacancies_from_sheet(self, sheet_title):
        """Collecting vacancies data from spreadsheet"""

        spreadsheet = self.spreadsheet

        # Getting data from sheet
        values_response = spreadsheet.values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"{sheet_title}!{self.sheet_range}"
        ).execute()
        values = values_response.get('values', [])

        if not values:
            print(f'No values in {sheet_title}')
            return None

        # Create dict with vacancies
        vacancies_dict = list()
        for value in values:
            vacancy_details = {TITLES[i]: value[i] for i in range(len(value))}
            vacancies_dict.append(vacancy_details)

        return vacancies_dict

    @staticmethod
    @logger.catch
    def connect_to_db(user, password, host, port, db_name):
        """Connecting to Postgresql database"""

        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}')
        session = sessionmaker(bind=engine)()
        return session, engine

    @staticmethod
    @logger.catch
    def setup_for_db(session, engine):
        """Use this method for database without vacancies
        and directions tables"""

        if not inspect(engine).has_table('directions') and not inspect(engine).has_table('vacancies'):

            Base.metadata.create_all(engine)

            for name in direction_names:
                direction = Direction(name=name)
                session.add(direction)
            session.commit()

            return True

    @staticmethod
    @logger.catch
    def check_if_vacancy_exists_db(session):
        exists = session.query(session.query(Vacancy).filter_by(
            company_name=vacancy_from_dict.get('company_name', ''),
            company_short_description=vacancy_from_dict.get('company_short_description', ''),
            company_direction_id=db_session.query(Direction).filter_by(
                name=vacancy_from_dict.get('company_direction', '')
            ).one().id,
            vacancy_name=vacancy_from_dict.get('vacancy_name', ''),
            vacancy_description=vacancy_from_dict.get('vacancy_description', ''),
            vacancy_requirements=vacancy_from_dict.get('vacancy_requirements', ''),
            vacancy_working_conditions=vacancy_from_dict.get('vacancy_working_conditions', ''),
            vacancy_salary=vacancy_from_dict.get('vacancy_salary', ''),
            vacancy_benefits=vacancy_from_dict.get('vacancy_benefits', ''),
            vacancy_contacts=vacancy_from_dict.get('vacancy_contacts', ''),
            company_website=vacancy_from_dict.get('company_website', ''),
            degree=vacancy_from_dict.get('degree', ''),
            minimal_english_level=vacancy_from_dict.get('minimal_english_level', ''),
            working_time=vacancy_from_dict.get('working_time', ''),
            working_experience=vacancy_from_dict.get('working_experience', ''),
        ).exists()).scalar()

        return exists

    @staticmethod
    @logger.catch
    def add_vacancy_to_db(session, vacancy, vacancy_cls):
        vacancy_to_db = vacancy_cls(
            company_name=vacancy.get('company_name', ''),
            company_short_description=vacancy.get('company_short_description', ''),
            company_direction_id=db_session.query(Direction).filter_by(
                name=vacancy.get('company_direction', '')
            ).one().id,
            vacancy_name=vacancy.get('vacancy_name', ''),
            vacancy_description=vacancy.get('vacancy_description', ''),
            vacancy_requirements=vacancy.get('vacancy_requirements', ''),
            vacancy_working_conditions=vacancy.get('vacancy_working_conditions', ''),
            vacancy_salary=vacancy.get('vacancy_salary', ''),
            vacancy_benefits=vacancy.get('vacancy_benefits', ''),
            vacancy_contacts=vacancy.get('vacancy_contacts', ''),
            company_website=vacancy.get('company_website', ''),
            degree=vacancy.get('degree', ''),
            minimal_english_level=vacancy.get('minimal_english_level', ''),
            working_time=vacancy.get('working_time', ''),
            working_experience=vacancy.get('working_experience', ''),
        )

        session.add(vacancy_to_db)


if __name__ == '__main__':
    # Config logs: file, format of the log, level, max size 50 KB, after 50 KB compress to zip
    logger.add("logs.log", format="{time} {level} {message}", level="DEBUG", rotation="50 KB", compression="zip")

    logger.info("Скрипт розпочав роботу!")
    transporter = SheetToDbTransporter()  # Utilizing custom class
    logger.info("Підключилисьдо Google Sheet!")
    sheets = transporter.get_tuple_of_sheets()  # Getting all sheets' titles from spreadsheet
    logger.info("Дані з Google Sheet були зібрані!")

    # Creating connection to db
    db_session, db_engine = transporter.connect_to_db('postgres', 'postgres', '127.0.0.1', '5432', 'test_sheet')
    logger.info("Підключились до бази данних!")

    # Method below works only if tables are not creates
    if transporter.setup_for_db(db_session, db_engine):
        logger.info("Таблиці в базі даних не знайдені, були створенні власні!")

    # Getting existing tables' names
    Base = automap_base()
    Base.prepare(db_engine, reflect=True)
    Direction = Base.classes.directions
    Vacancy = Base.classes.vacancies
    logger.info("Потрібні таблиці в базі даних знайдені!")

    for title in sheets:
        if title:

            # Get data from Google Sheet
            vacancies = transporter.get_vacancies_from_sheet(title)
            if vacancies:
                for vacancy_from_dict in vacancies:

                    # Check if such vacancy already exists, if yes, than continue iteration
                    vacancy_exists = transporter.check_if_vacancy_exists_db(db_session)
                    if vacancy_exists:
                        continue

                    # Else add vacancy to database
                    transporter.add_vacancy_to_db(db_session, vacancy_from_dict, Vacancy)
                    logger.info(
                        f"Було додано вакансію {vacancy_from_dict.get('vacancy_name', '')}, від {vacancy_from_dict.get('company_name', '')}"
                    )

                # Confirm adding new data to database
                db_session.commit()

    logger.info("Скрипт завершив свою роботу!")
