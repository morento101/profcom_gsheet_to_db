import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from pprint import pprint
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import direction_names
from sqlalchemy import Column, String, ForeignKey, Integer, Text, DateTime, Table, MetaData
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import datetime

import psycopg2

# If modifying these scopes, delete the file token.json
SCOPES = ('https://www.googleapis.com/auth/spreadsheets',)

# Data titles
TITLES = (
        'company_name', 'company_short_description', 'company_direction', 'vacancy_name',
        'vacancy_description',  'vacancy_requirements', 'vacancy_working_conditions',
        'vacancy_salary', 'vacancy_benefits', 'vacancy_contacts', 'company_website'
)

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '1X-h7LDj--O2FFNCwsY97kohDVbLsCW5LCq-I0d7l3r8'
RANGE_NAME = 'B:L'


class SheetToDbTransporter:
    """Class for transporting data from google sheet to database"""

    def __init__(self, scopes=SCOPES, spreadsheet_id=SPREADSHEET_ID, sheet_range=RANGE_NAME):
        self.scopes = scopes
        self.spreadsheet_id = spreadsheet_id
        self.sheet_range = sheet_range
        self.creds = None

        if os.path.exists('token.json'):
            self.creds = Credentials.from_authorized_user_file('token.json', self.scopes)

        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', self.scopes)
                self.creds = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(self.creds.to_json())

        # Connect to spreadsheet
        self.service = build('sheets', 'v4', credentials=self.creds)
        self.spreadsheet = self.service.spreadsheets()

    def get_tuple_of_sheets(self):
        # Get data  about spreadsheet
        sheet_metadata = self.spreadsheet.get(spreadsheetId=self.spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')

        titles = (sheets[index].get("properties", {}).get("title", "") for index in range(len(sheets)))

        return titles

    def get_vacancies_from_sheet(self, sheet_title):
        spreadsheet = self.spreadsheet
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

    def setup_for_db(self, session, engine):
        meta = MetaData()

        directions = Table(
            'directions', meta,
            Column('id', Integer, primary_key=True),
            Column('name', String(90)),
        )

        db_vacancies = Table(
            'vacancies', meta,
            Column('id', Integer, primary_key=True),
            Column('company_name', String(200)),
            Column('company_short_description', Text, nullable=True),
            Column('company_direction_id', Integer, ForeignKey('directions.id')),
            Column('vacancy_name', String(200)),
            Column('vacancy_description', Text),
            Column('vacancy_requirements', Text),
            Column('vacancy_working_conditions', Text),
            Column('vacancy_salary', String(200)),
            Column('vacancy_benefits', Text),
            Column('vacancy_contacts', Text),
            Column('company_website', Text),
            Column('vacancy_date_added', DateTime, default=datetime.datetime.now()),
        )

        meta.create_all(engine)

        for name in direction_names:
            engine.execute(directions.insert(), name=name)

        return directions, db_vacancies

    def connect_to_db(self, user, password, host, port, db_name):
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}')
        session = sessionmaker(engine)()
        return session, engine


if __name__ == '__main__':
    transporter = SheetToDbTransporter()
    sheets = transporter.get_tuple_of_sheets()
    db_session, db_engine = transporter.connect_to_db('postgres', 'postgres', '127.0.0.1', '5432', 'test_sheet')
    directions, db_vacancies = transporter.setup_for_db(db_session, db_engine)

    for title in sheets:
        if title:
            vacancies = transporter.get_vacancies_from_sheet(title)
            if vacancies:
                for vacancy in vacancies:
                    print(vacancy)
                    db_engine.execute(db_vacancies.insert(),
                                      company_name=vacancy.get('company_name', ''),
                                      company_short_description=vacancy.get('company_short_description', ''),
                                      company_direction=vacancy.get('company_direction', ''),
                                      vacancy_name=vacancy.get('vacancy_name', ''),
                                      vacancy_description=vacancy.get('vacancy_description', ''),
                                      vacancy_requirements=vacancy.get('vacancy_requirements', ''),
                                      vacancy_working_conditions=vacancy.get('vacancy_working_conditions', ''),
                                      vacancy_salary=vacancy.get('vacancy_salary', ''),
                                      vacancy_benefits=vacancy.get('vacancy_benefits', ''),
                                      vacancy_contacts=vacancy.get('vacancy_contacts', ''),
                                      company_website=vacancy.get('company_website', '')
                                      )
