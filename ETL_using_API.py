import requests
import json
from datetime import datetime
import sqlalchemy
import oracledb
import cx_Oracle

url = f"https://pokeapi.co/api/v2/generation/1"

response_json = requests.get(url=url).json()


region_name = response_json['main_region']['name']
alias = response_json['name']
no_of_species = len(response_json['pokemon_species'])

time_start = datetime.now()

pkmns = []
for pkmn in response_json['pokemon_species']:
    pkmn_name = pkmn['name']
    pkmn_obj = requests.get(f'https://pokeapi.co/api/v2/pokemon/{pkmn_name}').json()
    pkmn_id = pkmn_obj['id']
    pkmn_type = pkmn_obj['types'][0]['type']['name']
    pkmns.append({
        'id' : pkmn_id,
        'name' : pkmn_name,
        'type' : pkmn_type
    })
    # break

result_dict = {
    'Region_Name' : region_name,
    'Alias' : alias,
    'Total_Species' : no_of_species,
    'Pokemons' : pkmns
}
# print(result_dict)

'''
Writing output into a text file:
    res = json.dumps(result_dict, indent=4)
    with open('pokemons_in_gen1.txt', 'w') as fp:
        fp.write(res)
'''

# End time:  time_end = datetime.now()

''' 
Elapsed time:
    print(time_start)
    print(time_end)
    print(f"Time taken to extract and write into the file is {time_end - time_start}")
'''


# oracledb and cx_oracle --> for connecting to the Oracle DB

from sqlalchemy import MetaData, Integer, String, Column, Table

conn_url = "oracle+cx_oracle://sys:Sai1234@localhost:1521/orcl?mode=SYSDBA"
engine = sqlalchemy.create_engine(conn_url)

metadata = MetaData()
gen1_pokemon_table = Table(
    'gen1_pokemon_table',
    metadata, 
    Column('ID_NO', Integer, primary_key=True),
    Column('Name', String(255)),
    Column('Type', String(255))
)
metadata.create_all(engine)
print("--- Created a Table ---")



from sqlalchemy import insert

values_to_be_inserted = result_dict['Pokemons']

# In the insert statement, while giving values, make sure that the correct column name is given becoz they are cas-sensitive
with engine.connect() as conn:
    for val_obj in values_to_be_inserted:
        stmt = gen1_pokemon_table.insert().values(ID_NO=int(val_obj['id']),Name=str(val_obj['name']),Type=str(val_obj['type']))
        res = conn.execute(stmt)
    conn.commit()
print("--- Inserted into the Table ---")