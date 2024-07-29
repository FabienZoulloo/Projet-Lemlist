import requests
import base64
import time
from google.cloud import secretmanager
import pandas as pd
import os

def access_secret(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    project_id = "igneous-axiom-405407"  # Remplace par ton ID de projet Google Cloud
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_all_campaigns(api_key):
    offset = 0
    all_campaigns = []
    while True:
        url = f"https://api.lemlist.com/api/campaigns"
        params = {
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

        all_campaigns.extend(data)
        offset += 100

        time.sleep(0.1)  # Ajuste cette valeur si nécessaire

    return all_campaigns

def get_lemlist_activities(campaign_id, api_key, activity_type):
    all_activities = []
    offset = 0
    campaign_name = ''  # Initialiser campaign_name avant la boucle
    while True:
        url = f"https://api.lemlist.com/api/activities"
        params = {
            "campaignId": campaign_id,
            "type": activity_type,
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

        if not campaign_name and data:  # Récupérer le nom de la campagne à partir du premier objet
            campaign_name = data[0].get('campaignName', '')

        all_activities.extend(data)
        offset += 100

        time.sleep(0.1)  # Ajuste cette valeur si nécessaire

    return all_activities, campaign_name

def filter_emails_bounced(data):
    return [activity for activity in data if activity.get('type') == 'emailsBounced' and activity.get('type') != 'linkedinSendFailed']

def create_csv_from_data(data, campaign_id, campaign_name, activity_type, combined=False):
    # Transformer les données en DataFrame
    df = pd.DataFrame(data)

    if activity_type == "emailsOpened":
        # Convertir 'createdAt' en datetime pour la dernière date d'ouverture
        df['createdAt'] = pd.to_datetime(df['createdAt'])

        # Grouper par email et séquence, et compter les occurrences
        df_count = df.groupby(['leadEmail', 'sequenceStep']).size().reset_index(name='Count')

        # Identifier les séquences avec plus d'une ouverture pour chaque email
        sequences_multiple_openings = df_count[df_count['Count'] > 1].groupby('leadEmail')['sequenceStep'].apply(lambda x: '/'.join(map(str, x.unique()))).reset_index(name='MultipleOpeningsSequenceSteps')

        # Calculer le nombre total d'ouvertures et la dernière date d'ouverture pour chaque email
        total_openings_and_last_date = df.groupby('leadEmail').agg({'createdAt': 'max', 'leadLastName': 'first', 'leadFirstName': 'first', 'sequenceStep': 'count', 'leadCompanyName': 'first'}).reset_index()

        # Fusionner les DataFrame
        df_merged = pd.merge(total_openings_and_last_date, sequences_multiple_openings, on='leadEmail', how='left').fillna('')

        # Renommer les colonnes pour plus de clarté
        df_merged.rename(columns={'sequenceStep': 'TotalOpenings', 'createdAt': 'LastOpened', 'MultipleOpeningsSequenceSteps': 'MultipleOpeningsSequenceSteps'}, inplace=True)

        # Trier par nombre total d'ouvertures en ordre décroissant
        df_merged = df_merged.sort_values(by='TotalOpenings', ascending=False)
    else:  # For bounced emails
        # Sélectionner les colonnes pertinentes pour les emails bounced
        relevant_columns = ['leadEmail', 'createdAt', 'sequenceStep', 'leadCompanyName']
        if 'leadLastName' in df.columns and 'leadFirstName' in df.columns:
            relevant_columns.extend(['leadLastName', 'leadFirstName'])
        
        df_merged = df[relevant_columns].copy()
        df_merged['createdAt'] = pd.to_datetime(df_merged['createdAt'])
        df_merged.rename(columns={'sequenceStep': 'SequenceStep', 'createdAt': 'BouncedAt'}, inplace=True)
        df_merged = df_merged.sort_values(by='BouncedAt', ascending=False)

    # Formatage du nom de la campagne pour le nom de fichier (suppression des caractères spéciaux, espaces, etc.)
    campaign_name_formatted = "".join(c for c in campaign_name if c.isalnum())
    # Construction du nom de fichier avec la date, le nom de la campagne et l'ID
    today = pd.Timestamp('today').strftime('%Y%m')
    
    if combined:
        csv_filename = f"{today}_{activity_type}_all_campaigns.csv"
    else:
        csv_filename = f"{today}_{campaign_name_formatted}_{campaign_id}_{activity_type}.csv"

    # Vérification de l'existence du dossier 'output' et des sous-dossiers 'Bounced' et 'Opened', sinon les créer
    output_dir = os.path.join("output", "Bounced" if activity_type == "emailsBounced" else "Opened")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Chemin complet du fichier
    full_path = os.path.join(output_dir, csv_filename)

    # Exporter en CSV
    df_merged.to_csv(full_path, index=False)
    print(f"CSV créé : {full_path}")

def main():
    api_key = access_secret("LEMLIST")

    action_choice = input("Voulez-vous récupérer les informations pour une campagne spécifique (1) ou pour toutes les campagnes (2) ? ")
    activity_type_choice = input("Voulez-vous récupérer les performances (emails ouverts) (1) ou les emails bounced (2) ? ")

    if activity_type_choice == "1":
        activity_type = "emailsOpened"
    elif activity_type_choice == "2":
        activity_type = "emailsBounced"
    else:
        print("Choix invalide.")
        return

    if action_choice == "1":
        campaign_id = input("Entrez le campaignID : ")
        activities_data, campaign_name = get_lemlist_activities(campaign_id, api_key, activity_type)
        if activities_data:
            if activity_type == "emailsBounced":
                activities_data = filter_emails_bounced(activities_data)
            create_csv_from_data(activities_data, campaign_id, campaign_name, activity_type)
        else:
            print("Erreur lors de la récupération des données ou aucune donnée disponible.")
    elif action_choice == "2":
        all_campaigns = get_all_campaigns(api_key)
        combined_data = []
        for campaign in all_campaigns:
            campaign_id = campaign['_id']
            campaign_name = campaign['name']
            activities_data, _ = get_lemlist_activities(campaign_id, api_key, activity_type)
            if activities_data:
                if activity_type == "emailsBounced":
                    activities_data = filter_emails_bounced(activities_data)
                combined_data.extend(activities_data)
            else:
                print(f"Aucune donnée disponible pour la campagne {campaign_name} ({campaign_id}).")
        if combined_data:
            create_csv_from_data(combined_data, "all_campaigns", "all_campaigns", activity_type, combined=True)
    else:
        print("Choix invalide.")

if __name__ == "__main__":
    main()
