import datetime
from sqlalchemy import create_engine
import pandas as pd
import os
import json

abs_path = os.path.dirname(os.path.realpath(__file__))
fp = open(abs_path + "\\data_access.json", encoding='utf-8', mode='r')
data_acc = json.load(fp)

database = 'business_inteligence'
mysql_user = data_acc['user']
mysql_password = data_acc['password']
mysql_host = '195.35.17.83'
connection_string = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{database}'
engine = create_engine(connection_string, echo=True)
dtoday = str(datetime.datetime.today()).split()[0]

events_fat = {
'abf_franchising_expo': 'A&B',
'feimec': 'Industria, Agro e Print',
'fispal_food': 'A&B',
'fispal_tec': 'A&B',
'formobile': 'Industria, Agro e Print',
'hospitalar': 'Saude',
'intermodal': 'Infra & Tech',
'tecnocarne': 'A&B'
}

events = {
'agrishow': 'Industria, Agro e Print',
'bwp': 'Energia',
'concreteshow': 'Infra & Tech',
'eaml': 'Energia',
'energysolutionsshow': 'Energia',
'expomafe': 'Industria, Agro e Print',
'futureprint': 'Industria, Agro e Print',
'his': 'Saude',
'ntexpo': 'Infra & Tech',
'plastico_brasil': 'Industria, Agro e Print',
}

query = f"""
SELECT
edition,
exhibition_code,
'$title_name$' as event_name,
'$portfolio$' as portfolio,
form_type,
registrationStatus,
'NA' as product,
DATE(creationDate) as registration_date,
COUNT(registration_id) as credenciados
From bi_maestro.$event_name$_registration r 
WHERE 1=1
group by 1, 2, 3, 4, 5, 6, 7, 8
"""

query_fat = f"""
SELECT
r.edition,
r.exhibition_code,
'$title_name$' as event_name,
'$portfolio$' as portfolio,
r.form_type,
r.registrationStatus,
f.product,
DATE(r.creationDate) as registration_date,
COUNT(r.registration_id) as credenciados
From bi_maestro.$event_name$_registration r 
LEFT JOIN bi_maestro.2026_$event_name$_faturamento f ON r.credential = f.codigo_beneficiario
WHERE 1=1
group by 1, 2, 3, 4, 5, 6, 7, 8
"""


list_dfs = []
for e in events:
    title_name = e.replace('_', ' ').title()
    connection_string = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{database}'
    engine = create_engine(connection_string, echo=True)
    dn = pd.read_sql(query.replace('$event_name$', e).replace('$portfolio$', events[e]).replace('$title_name$', title_name), con=engine)
    list_dfs.append(dn)
for e in events_fat:
    title_name = e.replace('_', ' ').title()
    connection_string = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{database}'
    engine = create_engine(connection_string, echo=True)
    dn = pd.read_sql(query_fat.replace('$event_name$', e).replace('$portfolio$', events_fat[e]).replace('$title_name$', title_name), con=engine)
    list_dfs.append(dn)

# df = pd.read_sql(query, con=engine)
dx = pd.concat(list_dfs, ignore_index=True)

df = dx.copy()
df['credenciados'] = df.credenciados.astype(int)

database = 'bi_aux'
connection_string = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{database}'
# Create a SQLAlchemy engine
engine = create_engine(connection_string)

# Upload the DataFrame to MySQL
table_name = '2026_overview_visitation_by_date'  # Replace with your desired table name
df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)