import requests
import xml.etree.ElementTree as ET

url = "https://it.jooble.org/affiliate_feed/DQAGKw4aAREdNTAtEg==.xml"

try:
    # Download XML feed
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    
    with open("jooble_feed.xml", "wb") as f:
        f.write(resp.content)
    print("✅ Feed saved as jooble_feed.xml")

    # Parse XML
    root = ET.fromstring(resp.content)
    jobs = root.findall(".//job")
    print(f"Total jobs found: {len(jobs)}")

    # Print first job with all tags
    if jobs:
        print("\n--- First Job Details ---")
        for child in jobs[0]:
            print(f"{child.tag}: {child.text}")
    else:
        print("⚠️ No <job> elements found.")

except Exception as e:
    print(f"❌ Error: {e}")
