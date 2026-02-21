import pandas as pd
import numpy as np
from faker import Faker
import random
import os
import argparse
from datetime import datetime, timedelta

fake = Faker('ru_RU')

def get_master_data(output_dir, num_clients, num_campaigns):
    
    clients_path = os.path.join(output_dir, 'clients.csv')
    campaigns_path = os.path.join(output_dir, 'campaigns.csv')
    
    if os.path.exists(clients_path):
        df_clients = pd.read_csv(clients_path)
    else:
        industries = ['E-commerce', 'Real Estate', 'Auto', 'EdTech', 'FinTech']
        managers = ['Иванов И.П.', 'Петров Р.Ф.', 'Сидорова Е.А.', 'Кузнецова А.А.']
        clients_data = []
        for i in range(1, num_clients + 1):
            clients_data.append({
                'id': f'CL_{i:03d}',
                'name': fake.company(),
                'website': f"site-{i}.ru",
                'industry': random.choice(industries),
                'manager': random.choice(managers)
            })
        df_clients = pd.DataFrame(clients_data)
        os.makedirs(output_dir, exist_ok=True)
        df_clients.to_csv(clients_path, index=False)

    if os.path.exists(campaigns_path):
        df_campaigns = pd.read_csv(campaigns_path)
    else:
        client_ids = df_clients['id'].tolist()
        campaigns_data = []
        
        PLATFORM_TYPES = {
            'Yandex':   ['Поиск', 'Сетевая', 'Ретаргетинг', 'Медийная'],
            'Google':   ['Поиск', 'Сетевая', 'YouTube Video'],
            'VK':       ['Таргетированная', 'Тизерная', 'VK Клипы'],
            'Telegram': ['Telegram Ads', 'Нативная']
        }
        
        platforms_list = list(PLATFORM_TYPES.keys())
        
        for i in range(1, num_campaigns + 1):
            platform = random.choice(platforms_list)
            # Выбираем тип ТОЛЬКО из допустимых для этой платформы
            camp_type = random.choice(PLATFORM_TYPES[platform])
            
            campaigns_data.append({
                'id': f'CMP_{i:04d}',
                'client_id': random.choice(client_ids),
                'platform': platform,
                'type': camp_type,
                'start_date': '2025-01-01'
            })
            
        df_campaigns = pd.DataFrame(campaigns_data)
        os.makedirs(output_dir, exist_ok=True)
        df_campaigns.to_csv(campaigns_path, index=False)
            
    return df_clients, df_campaigns


def generate_daily_facts(date_str, df_clients, df_campaigns, output_dir):
    process_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    random.seed(process_date.toordinal())
    
    stats_data = []
    active_campaigns = df_campaigns.sample(frac=0.8, random_state=random.randint(1, 1000))
    
    for _, camp in active_campaigns.iterrows():
        impressions = random.randint(100, 5000)
        clicks = int(impressions * random.uniform(0.01, 0.05))
        cost = round(clicks * random.uniform(10, 100), 2)
        conversions = int(clicks * random.uniform(0.0, 0.1))
        
        stats_data.append({
            'date': date_str,
            'campaign_id': camp['id'],
            'impressions': impressions,
            'clicks': clicks,
            'cost': cost,
            'conversions': conversions
        })
    
    monitor_data = []
    for _, client in df_clients.iterrows():
        has_issue = (random.random() > 0.95)
        load_time = random.randint(1500, 5000) if has_issue else random.randint(200, 800)
        errors = random.randint(5, 50) if has_issue else 0
        uptime = round(random.uniform(90.0, 99.0), 2) if has_issue else 100.0
        
        monitor_data.append({
            'date': date_str,
            'client_id': client['id'],
            'load_time_ms': load_time,
            'uptime_pct': uptime,
            'server_errors': errors
        })
        
    pd.DataFrame(stats_data).to_csv(os.path.join(output_dir, f'ad_stats_{date_str}.csv'), index=False)
    pd.DataFrame(monitor_data).to_csv(os.path.join(output_dir, f'site_monitoring_{date_str}.csv'), index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, default=(datetime.now()).strftime('%Y-%m-%d'))
    parser.add_argument('--clients', type=int, default=200)
    parser.add_argument('--campaigns', type=int, default=500)
    parser.add_argument('--output', type=str, default='dataset')
    args = parser.parse_args()
    
    df_cl, df_cmp = get_master_data(args.output, args.clients, args.campaigns)
    generate_daily_facts(args.date, df_cl, df_cmp, args.output)
