import os
# from pprint import pprint
import smtplib
import time
import psutil

from loguru import logger

from dotenv import load_dotenv
load_dotenv()

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
SPREADSHEET_ID = '1nU3wT-ywI5ePxhRVEksmxQDem-TJfCbsoZBsBerdnOM' # profcom sheet

RANGE_NAME = 'B2:P'


class SheetToDbTransporter:
    """Class for transporting data from google sheet to database"""

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
        vacancies_dicts = list()
        for value in values:
            vacancy_details = {TITLES[i]: value[i] for i in range(len(value))}
            vacancies_dicts.append(vacancy_details)

        return vacancies_dicts

    @staticmethod
    def connect_to_db(user, password, host, port, db_name):
        """Connecting to Postgresql database"""

        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}')
        session = sessionmaker(bind=engine)()
        return session, engine

    @staticmethod
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
    def check_if_vacancy_exists_db(session, vacancy, vacancy_cls):
        exists = session.query(session.query(vacancy_cls).filter_by(**vacancy).exists()).scalar()
        return exists

    @staticmethod
    def add_vacancy_to_db(session, vacancy, vacancy_cls):
        vacancy_to_db = vacancy_cls(**vacancy)
        session.add(vacancy_to_db)


def main():
    logger.info("Скрипт розпочав роботу!")

    transporter = SheetToDbTransporter()  # Utilizing custom class
    logger.info("Підключилисьдо Google Sheet!")

    sheets = transporter.get_tuple_of_sheets()  # Getting all sheets' titles from spreadsheet
    logger.info("Дані з Google Sheet були зібрані!")

    # Creating connection to db
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_session, db_engine = transporter.connect_to_db(db_user, db_password, db_host, db_port, 'test_sheet')

    # Method below works only if tables are not created
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
                    # pprint(vacancy_from_dict)

                    vacancy_from_dict['company_direction'] = db_session.query(Direction).filter_by(name=vacancy_from_dict.get('company_direction', '')).one().id

                    # Check if such vacancy already exists, if yes, than continue iteration
                    vacancy_exists = transporter.check_if_vacancy_exists_db(db_session, vacancy_from_dict, Vacancy)
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

if __name__ == '__main__':
    
    try:
        # Config logs: file, format of the log, level, max size 50 KB, after 50 KB compress to zip
        logger.add("logs.log", format="{time} {level} {message}", level="DEBUG", rotation="50 KB", compression="zip")

        while True:
            start = time.perf_counter()
            main()
            end = time.perf_counter()
            logger.success(f"Витрачено часу: {end - start} секунд")
            logger.success(f"Витрачено пам'яті {psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2} МБ")
            time.sleep(15)


    except Exception as e:
        logger.exception(f"{e}")

        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()

            sender_email = os.getenv("SENDER_EMAIL")
            sender_email_password = os.getenv("SENDER_EMAIL_PASSWORD")

            smtp.login(sender_email, sender_email_password)

            subject = "GSHEET TO DB SCRIPT IS DOWN"
            body = "check it"
            msg = f"Subject: {subject}\n\n{body}"

            receiver_email = os.getenv("RECEIVER_EMAIL")

            smtp.sendmail(sender_email, receiver_email, msg)
