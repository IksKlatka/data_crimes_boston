import matplotlib.pyplot as plt
import pandas as pd
from data.cleaning_algorithms import file_to_df


def incidents_quantity_by_day_of_week(df: pd.DataFrame):
    data = df[['INCIDENT_NUMBER', 'DAY_OF_WEEK']]
    crimes_by_day = data.groupby('DAY_OF_WEEK')['INCIDENT_NUMBER'].count()

    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    plt.figure(figsize=(10, 6))

    crimes_by_day.plot(kind= 'bar', color='#AC14E3')
    plt.title('Incident count by day of week. (2015-2022)')
    plt.xlabel('day of week')
    plt.xticks(range(len(days)), days,  rotation=45)
    plt.ylabel('incident count')

    plt.tight_layout()
    plt.show()

def shooting_quantity_by_hour(df: pd.DataFrame):
    df['HOUR'] = df['HOUR'].replace(0,24)
    data = df[['SHOOTING', 'HOUR']]

    shootings = data[data['SHOOTING'] == True]
    shootings_by_hour = shootings.groupby('HOUR').size()

    plt.figure(figsize=(10, 6))
    plt.bar(shootings_by_hour.index, shootings_by_hour.values)

    plt.title('Quantity of incidents involving shooting grouped by hour. (2015-2022)')
    plt.xlabel('hour')
    plt.ylabel('incident count')

    plt.xticks([_ for _ in range (1,25)] , rotation=45)
    plt.tight_layout()
    plt.show()


def most_shooting_by_district(df: pd.DataFrame):
    shootings = df[df['SHOOTING'] == True]

    district_counts = shootings['DISTRICT'].value_counts()

    plt.figure(figsize=(10, 5))
    district_counts.plot(kind='bar', color='#E35614')
    plt.title('Quantity of incidents involving shooting grouped by districts.')
    plt.xlabel('district')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('incident count')
    plt.tight_layout()
    plt.show()

def shootings_quantity_by_year(df: pd.DataFrame):

    shootings = df[df['SHOOTING'] == True]
    year_counts = shootings['YEAR'].value_counts()

    plt.figure(figsize=(12, 6))
    year_counts.plot(kind='bar', color='#E11541')
    plt.title('Quantity of shootings in years 2015-2022')
    plt.xlabel('year')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('shooting count')
    plt.tight_layout()
    plt.show()

def homicides_by_year(df: pd.DataFrame):
    homicides = df[df['OFFENSE_CODE'] == 111]
    homicides_by_year = homicides.groupby('YEAR')['INCIDENT_NUMBER'].count()

    plt.figure(figsize=(10, 6))
    homicides_by_year.plot(kind='bar', color='#FF5703')
    plt.title('Homicides by year.')
    plt.xlabel('year')
    plt.ylabel('homicide count')

    plt.legend()
    plt.tight_layout()
    plt.show()

def drug_violation_by_year(df: pd.DataFrame):

    drugs = df[df['OFFENSE_CODE_GROUP'] == 'Drug Violation']

    drugs_by_year = drugs.groupby('YEAR')['INCIDENT_NUMBER'].count()

    plt.figure(figsize=(10, 6))
    drugs_by_year.plot(kind='bar', color='#EDAE09')

    plt.title('Drug violation quantity by year')
    plt.xlabel('year')
    plt.ylabel('drug violation count')

    plt.tight_layout()
    plt.show()



if __name__ == '__main__':


    df = file_to_df('ALL_YEARS', separator=';')
    incidents_quantity_by_day_of_week(df)
    most_shooting_by_district(df)
    shooting_quantity_by_hour(df)
    shootings_quantity_by_year(df)
    homicides_by_year(df)
    drug_violation_by_year(df)