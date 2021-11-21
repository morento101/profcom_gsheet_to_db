import os.path

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from pprint import pprint

# Do not remove this import!!!
import psycopg2

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from models import Direction, Vacancy, direction_names, Base

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
        sheets = sheet_metadata.get('sheets', '')

        titles = (sheets[index].get("properties", {}).get("title", "") for index in range(len(sheets)))

        return titles

    def get_vacancies_from_sheet(self, sheet_title):
        """Collecting vacancies data from spreadsheet"""

        spreadsheet = self.spreadsheet

        # Getting data from sheet
        values_response = spreadsheet.values().get(spreadsheetId=self.spreadsheet_id,
                                             range=f"{sheet_title}!{self.sheet_range}").execute()
        values = values_response.get('values', [])

        if not values:
            print(f'No values in {sheet_title}')
            return None

        # Create dict with vacancies
        vacancies = list()
        for value in values:
            vacancy_details = {TITLES[i]: value[i] for i in range(len(value))}
            vacancies.append(vacancy_details)

        return vacancies

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

    @staticmethod
    def connect_to_db(user, password, host, port, db_name):
        """Connecting to Postgresql database"""

        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}')
        session = sessionmaker(bind=engine)()
        return session, engine


if __name__ == '__main__':
    transporter = SheetToDbTransporter()  # Utilizing custom class
    sheets = transporter.get_tuple_of_sheets()  # Getting all sheets' titles from spreadsheet

    # Creating connection to db
    db_session, db_engine = transporter.connect_to_db('postgres', 'postgres', '127.0.0.1', '5432', 'test_sheet')

    # Method below works only if tables are not creates
    transporter.setup_for_db(db_session, db_engine)

    # Getting existing tables' names
    directions_table = Base.metadata.tables['directions']
    vacancies_table = Base.metadata.tables['vacancies']

    for title in sheets:
        if title:

            # Get data from Google Sheet
            vacancies = transporter.get_vacancies_from_sheet(title)
            if vacancies:
                for vacancy in vacancies:

                    # Check if such vacancy already exists, if yes, than continue iteration
                    if db_session.query(db_session.query(vacancies_table).filter(
                        vacancies_table.c.company_name == vacancy.get('company_name', ''),
                        vacancies_table.c.vacancy_name == vacancy.get('vacancy_name', ''),
                        vacancies_table.c.vacancy_description == vacancy.get('vacancy_description', ''),
                        vacancies_table.c.vacancy_salary == vacancy.get('vacancy_salary', ''),
                    ).exists()).scalar():
                        continue

                    # Else add vacancy to database
                    vacancy_db = Vacancy(
                        company_name=vacancy.get('company_name', ''),
                        company_short_description=vacancy.get('company_short_description', ''),
                        company_direction_id=db_session.query(directions_table).filter(
                            directions_table.c.name == vacancy.get('company_direction', '')
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

                    db_session.add(vacancy_db)

                # Confirm adding new data to database
                db_session.commit()
