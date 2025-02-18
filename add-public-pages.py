import asyncio
import csv
from pathlib import Path
from urllib.parse import urlparse

from bson import ObjectId
import requests
from utils.mongodb import CONTENT, SYSTEM_SUGGESTION_TYPES, count_documents, db, AUDITS, delete_document, find_documents, insert_document, update_document

PYTHON_API_BASE_URL = "https://hipa-ai-api.azurewebsites.net"
ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI2NzE3NzBhMGVkMGU5MWZjNzY0ZWEwYWIiLCJleHAiOjI2ODY0OTUzNjQsImlzcyI6ImxvY2FsaG9zdDo3MjQ5IiwiYXVkIjoibG9jYWxob3N0OjcyNDkifQ.LOF_yLvxRg3WYrC79f-LPNxvJeI4LSPi-AgW_12WGrY"

async def main(web_page: str):
    print(f"web page: {web_page}")
    domain = extract_domain(web_page)
    slug = generate_public_page_slug(web_page)
    if not slug:
        return
    
    print(f"domain: {domain}, slug: {slug}")
    count = count_documents(AUDITS, {"article_slug": slug, "domain_name": domain})
    if count > 0:
        print(f"already added public page with domain {domain} and slug {slug}")
        return

    public_page = {
        "article_slug": slug,
        "domain_name": domain,
        "web_page": web_page
        }
    public_page_id = insert_document(AUDITS, public_page)
    content_id = create_public_page_content(domain, web_page)
    print(f"public page id: {public_page_id}, content id: {content_id}")
    if not content_id:
        return

    update_document(AUDITS, public_page_id, {"content_id": ObjectId(content_id)})
    update_document(CONTENT, ObjectId(content_id), {"is_public_page": True})

    generate_public_page_suggestions(content_id)
    print(f"{web_page}: generated suggestions")
    intro = generate_public_page_intro(str(public_page_id))

    update_document(AUDITS, public_page_id, {"intro": intro, "public": True})
    print(f"Created public page available at https://nt.hipa.ai/audit/{domain}/{slug}")

async def main_async():
    # clean_public_pages() #for test only
    csv_file_path = "pages.csv"

    lines = read_csv(csv_file_path)
    batch_size = 5
    for i in range(0, len(lines), batch_size):
        tasks = []
        batch = lines[i:i + batch_size]
        for line in batch:
            try:
                web_page = line[0]
                tasks.append(asyncio.create_task(main(web_page)))
            except Exception as ex:
                print(str(ex))
        await asyncio.gather(*tasks)
    
    public_pages = find_documents(AUDITS, {"public": True}, {"article_slug": 1, "domain_name": 1})
    urls = [f"https://hipa.ai/audit/{p['domain_name']}/{p['article_slug']}" for p in public_pages]
    update_sitemap(urls)

def clean_public_pages():
    audits = find_documents(AUDITS, {})
    for audit in audits:
        delete_document(AUDITS, audit['_id'])
        delete_document(CONTENT, audit['content_id'])


def update_sitemap(urls: list[str]):
    sitemap = ['<?xml version="1.0" encoding="UTF-8"?>']
    sitemap.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    
    for url in urls:
        sitemap.append("    <url>")
        sitemap.append(f"        <loc>{url}</loc>")
        sitemap.append("    </url>")
    
    sitemap.append("</urlset>")
    
    with open("audit-sitemap.xml", "w", encoding="utf-8") as f:
        f.write("\n".join(sitemap))

    print("Sitemap generated: audit-sitemap.xml")

def create_public_page_content(domain: str, web_page: str):
    try:
        payload = {
            "site": domain,
            "web_page": web_page,
        }
        headers = {
            "Authorization": f"Bearer {ACCESS_TOKEN}",
            "Content-Type": "application/json" 
            }
        
        response = requests.post(f"{PYTHON_API_BASE_URL}/content/create", json=payload, headers=headers)
        response.raise_for_status()
        
        response_data = response.json()
        
        if "id" in response_data:
            return response_data["id"]
        else:
            print("content id not found in the response.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def generate_public_page_suggestions(content_id: str):
    try:

        suggestion_types = find_documents(SYSTEM_SUGGESTION_TYPES, {"enabled": True})
        positive_ids = [str(t['_id']) for t in suggestion_types]
        headers = {
            "Authorization": f"Bearer {ACCESS_TOKEN}",
            "Content-Type": "application/json" 
            }
        payload = {
            "positive_ids": positive_ids,
            "instructions": "Generate 8-9 suggestions"
        }
        
        response = requests.post(f"{PYTHON_API_BASE_URL}/content/{content_id}/generate-suggestions", json=payload, headers=headers)
        response.raise_for_status()
        
        response_data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

def generate_public_page_intro(public_page_id: str):
    try:
        response = requests.post(f"{PYTHON_API_BASE_URL}/audits/{public_page_id}/intro")
        response.raise_for_status()
        
        response_data = response.json()
        if "intro" in response_data:
            return response_data["intro"]
        else:
            print("intro not found in the response.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None
    
def generate_public_page_slug(url: str):
    try:
        payload = {
            "web_page": url
        }
        response = requests.post(f"{PYTHON_API_BASE_URL}/audits/slug", json=payload)
        response.raise_for_status()
        
        response_data = response.json()
        if "slug" in response_data:
            return response_data["slug"]
        else:
            print("slug not found in the response.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None
        
    
def extract_domain(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    return domain




def read_csv(file_path):
    lines = []
    with open(file_path, mode='r', newline='') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            lines.append(row)
    return lines

if __name__ == "__main__":
    asyncio.run(main_async())