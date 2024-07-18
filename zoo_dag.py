
import pandas as pd

import pendulum

from airflow.decorators import dag, task
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def main():
   
    @task()
    def extract():

        csv1 = pd.read_csv("~/Downloads/zoo_animals.csv")
        csv2 = pd.read_csv("~/Downloads/zoo_health_records.csv")
        out = [csv1, csv2]
        return out


    @task
    def transform(csvs):
        
        merged = pd.merge(csvs[0], csvs[1], on="animal_id")

        merged = merged[merged['age'] >= 2]

        merged = merged.rename(columns={'animal_name': 'title'})

        merged = pd.concat([ merged[merged['health_status'] == 'Healthy'],
                            merged[merged['health_status'] == 'Needs Attention']] )

        return merged

    @task
    def aggregate(df):

        no_healthy = len(df[df['health_status'] == 'Healthy'])
        no_needs_att = len(df[df['health_status'] == 'Needs Attention'])

        species_dict = dict()
        for sp in df['species'].unique():
            species_dict[sp] = len(df[df['species'] == sp])

        return (df, (no_healthy, no_needs_att, species_dict))

    @task
    def validate(data):
        df = data[0]
        no_healthy = data[1][0]
        no_needs_att = data[1][1]

        species_dict = data[1][2]

        if no_healthy + no_needs_att != len(df):
            raise Exception("Validation error: health status")

        count_spec = 0
        for key in species_dict.keys():
            count_spec += species_dict[key]
        
        #count_spec+=2
        if count_spec != len(df):
            raise Exception("Validation error: species count")

        return df

    @task()
    def load(df):
    	
        df.to_csv("~/airflow/dags/final_zoo_data.csv", index=False)
        

    data = extract()
    transformed = transform(data)
    aggregated = aggregate(transformed)
    validated = validate(aggregated)
    load(validated)

main()
