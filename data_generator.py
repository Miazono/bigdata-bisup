import pandas as pd
import numpy as np
from faker import Faker
import random
import os
import argparse
from datetime import datetime, timedelta

fake = Faker("ru_RU")

INDUSTRIES = [
    "Электронная коммерция",
    "Недвижимость",
    "Автомобили и сервис",
    "Образование",
    "Финансовые услуги",
    "Медицина и клиники",
    "Туризм и гостиницы",
    "B2B-услуги",
]

LEGACY_INDUSTRY_MAP = {
    "E-commerce": "Электронная коммерция",
    "Real Estate": "Недвижимость",
    "Auto": "Автомобили и сервис",
    "EdTech": "Образование",
    "FinTech": "Финансовые услуги",
}

MANAGERS = [
    "Иванов И.П.",
    "Петров Р.Ф.",
    "Сидорова Е.А.",
    "Кузнецова А.А.",
    "Смирнова Н.В.",
    "Волков Д.С.",
    "Орлова М.А.",
]

PLATFORM_TYPES = {
    "Yandex": [
        "Поиск",
        "РСЯ",
        "Мастер кампаний",
        "Товарные кампании",
        "Медийная реклама",
        "Ретаргетинг",
    ],
    "Google": [
        "Поиск",
        "КМС",
        "Performance Max",
        "YouTube Ads",
        "Товарные кампании",
        "Ремаркетинг",
    ],
    "VK": [
        "Таргетированная реклама",
        "Промопосты",
        "VK Клипы",
        "Лид-формы",
        "Ремаркетинг",
    ],
    "Telegram": [
        "Telegram Ads",
        "Посевы в каналах",
        "Нативные интеграции",
        "Лидогенерация через боты",
    ],
    "Прочее": [
        "Программатик",
        "CPA-сети",
        "Реклама на маркетплейсах",
        "Influencer Ads",
        "Авито Продвижение",
        "Email-рассылки",
    ],
}

PLATFORM_WEIGHTS = {
    "Yandex": 0.35,
    "VK": 0.25,
    "Google": 0.18,
    "Telegram": 0.12,
    "Прочее": 0.10,
}


def get_id_number(value):
    return int(str(value).split("_")[-1])


def assign_manager(client_id):
    return MANAGERS[(get_id_number(client_id) - 1) % len(MANAGERS)]


def assign_industry(client_id):
    rand = random.Random(get_id_number(client_id))
    return rand.choice(INDUSTRIES)


def assign_site_id(client_id):
    return f"ST_{get_id_number(client_id):03d}"


def pick_platform(rand):
    platforms = list(PLATFORM_WEIGHTS.keys())
    weights = list(PLATFORM_WEIGHTS.values())
    return rand.choices(platforms, weights=weights, k=1)[0]


def pick_campaign_profile(seed_value):
    rand = random.Random(seed_value)
    platform = pick_platform(rand)
    camp_type = rand.choice(PLATFORM_TYPES[platform])
    return platform, camp_type


def normalize_clients(df_clients):
    if "website" not in df_clients.columns and "site_url" in df_clients.columns:
        df_clients["website"] = df_clients["site_url"]

    if "site_id" not in df_clients.columns:
        df_clients["site_id"] = df_clients["id"].apply(assign_site_id)
    else:
        df_clients["site_id"] = df_clients["id"].apply(assign_site_id)

    if "site_url" not in df_clients.columns:
        df_clients["site_url"] = df_clients["website"]
    else:
        df_clients["site_url"] = df_clients["site_url"].fillna(df_clients["website"])

    df_clients["site_name"] = df_clients["site_url"].apply(lambda x: f"Сайт {x}")

    df_clients["industry"] = df_clients["industry"].replace(LEGACY_INDUSTRY_MAP)
    df_clients["industry"] = df_clients["id"].apply(assign_industry)
    df_clients["manager"] = df_clients["id"].apply(assign_manager)

    return df_clients[
        [
            "id",
            "name",
            "website",
            "site_id",
            "site_name",
            "site_url",
            "industry",
            "manager",
        ]
    ]


def normalize_campaigns(df_campaigns):
    platforms = []
    camp_types = []

    for _, campaign in df_campaigns.iterrows():
        platform, camp_type = pick_campaign_profile(get_id_number(campaign["id"]))
        platforms.append(platform)
        camp_types.append(camp_type)

    df_campaigns["platform"] = platforms
    df_campaigns["type"] = camp_types

    return df_campaigns


def get_master_data(output_dir, num_clients, num_campaigns):

    clients_path = os.path.join(output_dir, "clients.csv")
    campaigns_path = os.path.join(output_dir, "campaigns.csv")

    if os.path.exists(clients_path):
        df_clients = pd.read_csv(clients_path)
        df_clients = normalize_clients(df_clients)
        df_clients.to_csv(clients_path, index=False)
    else:
        clients_data = []
        for i in range(1, num_clients + 1):
            clients_data.append(
                {
                    "id": f"CL_{i:03d}",
                    "name": fake.company(),
                    "website": f"site-{i}.ru",
                    "site_id": f"ST_{i:03d}",
                    "site_name": f"Сайт site-{i}.ru",
                    "site_url": f"site-{i}.ru",
                    "industry": assign_industry(f"CL_{i:03d}"),
                    "manager": assign_manager(f"CL_{i:03d}"),
                }
            )
        df_clients = pd.DataFrame(clients_data)
        df_clients = normalize_clients(df_clients)
        os.makedirs(output_dir, exist_ok=True)
        df_clients.to_csv(clients_path, index=False)

    if os.path.exists(campaigns_path):
        df_campaigns = pd.read_csv(campaigns_path)
        df_campaigns = normalize_campaigns(df_campaigns)
        df_campaigns.to_csv(campaigns_path, index=False)
    else:
        client_ids = df_clients["id"].tolist()
        campaigns_data = []

        for i in range(1, num_campaigns + 1):
            platform, camp_type = pick_campaign_profile(i)

            campaigns_data.append(
                {
                    "id": f"CMP_{i:04d}",
                    "client_id": random.choice(client_ids),
                    "platform": platform,
                    "type": camp_type,
                    "start_date": "2025-01-01",
                }
            )

        df_campaigns = pd.DataFrame(campaigns_data)
        os.makedirs(output_dir, exist_ok=True)
        df_campaigns.to_csv(campaigns_path, index=False)

    return df_clients, df_campaigns


def generate_daily_facts(date_str, df_clients, df_campaigns, output_dir):
    process_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    random.seed(process_date.toordinal())

    stats_data = []
    active_campaigns = df_campaigns.sample(
        frac=0.8, random_state=random.randint(1, 1000)
    )

    for _, camp in active_campaigns.iterrows():
        impressions = random.randint(100, 5000)
        clicks = int(impressions * random.uniform(0.01, 0.05))
        cost = round(clicks * random.uniform(10, 100), 2)
        conversions = int(clicks * random.uniform(0.0, 0.1))

        stats_data.append(
            {
                "date": date_str,
                "campaign_id": camp["id"],
                "impressions": impressions,
                "clicks": clicks,
                "cost": cost,
                "conversions": conversions,
            }
        )

    monitor_data = []
    for _, client in df_clients.iterrows():
        has_issue = random.random() > 0.95
        load_time = (
            random.randint(1500, 5000) if has_issue else random.randint(200, 800)
        )
        errors = random.randint(5, 50) if has_issue else 0
        uptime = round(random.uniform(90.0, 99.0), 2) if has_issue else 100.0

        monitor_data.append(
            {
                "date": date_str,
                "site_id": client["site_id"],
                "site_name": client["site_name"],
                "site_url": client["site_url"],
                "load_time_ms": load_time,
                "uptime_pct": uptime,
                "server_errors": errors,
            }
        )

    pd.DataFrame(stats_data).to_csv(
        os.path.join(output_dir, f"ad_stats_{date_str}.csv"), index=False
    )
    pd.DataFrame(monitor_data).to_csv(
        os.path.join(output_dir, f"site_monitoring_{date_str}.csv"), index=False
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date", type=str, default=(datetime.now()).strftime("%Y-%m-%d")
    )
    parser.add_argument("--clients", type=int, default=300)
    parser.add_argument("--campaigns", type=int, default=1230)
    parser.add_argument("--output", type=str, default="dataset")
    args = parser.parse_args()

    df_cl, df_cmp = get_master_data(args.output, args.clients, args.campaigns)
    generate_daily_facts(args.date, df_cl, df_cmp, args.output)
