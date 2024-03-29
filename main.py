﻿import requests
import base64
import time
from google.cloud import secretmanager
import pandas as pd

def access_secret(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    project_id = "igneous-axiom-405407"  # Remplace par ton ID de projet Google Cloud
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_lemlist_activities(campaign_id, api_key):
    all_activities = []
    offset = 0
    while True:
        url = f"https://api.lemlist.com/api/activities"
        params = {
            "campaignId": campaign_id,
            "type": "emailsOpened",
            "limit": 100,
            "offset": offset
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Basic " + base64.b64encode(f":{api_key}".encode()).decode()
        }

        response = requests.get(url, params=params, headers=headers)
        data = response.json()

        if response.status_code != 200 or not data:
            break

        all_activities.extend(data)
        offset += 100

        # Pause pour respecter la limite de fréquence d'appel
        time.sleep(0.1)  # Ajuste cette valeur si nécessaire

    return all_activities


def create_csv_from_data(data, campaign_id):
    # Transformer les données en DataFrame
    df = pd.DataFrame(data)

    # Convertir 'createdAt' en datetime pour la dernière date d'ouverture
    df['createdAt'] = pd.to_datetime(df['createdAt'])

    # Grouper par email et séquence, et compter les occurrences
    df_count = df.groupby(['leadEmail', 'sequenceStep']).size().reset_index(name='Count')

    # Identifier les séquences avec plus d'une ouverture pour chaque email
    sequences_multiple_openings = df_count[df_count['Count'] > 1].groupby('leadEmail')['sequenceStep'].apply(lambda x: '/'.join(map(str, x.unique()))).reset_index(name='MultipleOpeningsSequenceSteps')

    # Calculer le nombre total d'ouvertures et la dernière date d'ouverture pour chaque email
    total_openings_and_last_date = df.groupby('leadEmail').agg({'createdAt': 'max', 'leadLastName': 'first', 'leadFirstName': 'first', 'sequenceStep': 'count'}).reset_index()

    # Fusionner les DataFrame
    df_merged = pd.merge(total_openings_and_last_date, sequences_multiple_openings, on='leadEmail', how='left').fillna('')

    # Renommer les colonnes pour plus de clarté
    df_merged.rename(columns={'sequenceStep': 'TotalOpenings', 'createdAt': 'LastOpened', 'MultipleOpeningsSequenceSteps': 'MultipleOpeningsSequenceSteps'}, inplace=True)

    # Trier par nombre total d'ouvertures en ordre décroissant
    df_merged = df_merged.sort_values(by='TotalOpenings', ascending=False)


    # Exporter en CSV avec le nom incluant l'ID de la campagne
    csv_filename = f'lemlist_activities_{campaign_id}.csv'
    df_merged.to_csv(csv_filename, index=False)
    print(f"CSV créé : {csv_filename}")


def main():
    campaign_id = input("Entrez le campaignID : ")
    api_key = access_secret("LEMLIST")

    activities_data = get_lemlist_activities(campaign_id, api_key)

    if activities_data:
        create_csv_from_data(activities_data, campaign_id)
    else:
        print("Erreur lors de la récupération des données ou aucune donnée disponible.")

main()
