import json
import os
import requests
import hashlib
import re
import boto3

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
from datetime import datetime
import urllib3
from urllib.parse import urlparse
from html import unescape
from typing import List, Optional, Tuple, Dict

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FETCH_TOPIC = os.environ.get("KAFKA_TOPIC_JOB_FETCH", "job_postings.fetch_jobs")
GROUP_ID = os.environ.get("KAFKA_GROUP_JOB_WORKER", "job-postings-worker-group")
DLQ_TOPIC = os.environ.get("KAFKA_TOPIC_JOB_DLQ", "job_postings.dlq")
MAX_RETRY_COUNT = int(os.environ.get("MAX_RETRY_COUNT", "3"))
S3_BUCKET = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")


def create_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def upload_json_to_s3(s3_client, bucket: str, target_key: str, data: dict) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=target_key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )


def upload_raw_html_to_s3(s3_client, bucket: str, raw_s3_key: str, html: str) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=raw_s3_key,
        Body=html.encode("utf-8"),
        ContentType="text/html; charset=utf-8",
    )


def send_to_dlq(producer: Producer, job: dict, error: Exception, stage: str):
    next_retry_count = int(job.get("retry_count", 0)) + 1

    failed_job = {
        **job,
        "retry_count": next_retry_count,
    }

    dlq_message = {
        "job": failed_job,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "failed_stage": stage,
        "failed_at": datetime.utcnow().isoformat() + "Z",
    }

    producer.produce(
        DLQ_TOPIC,
        key=job["job_id"].encode("utf-8"),
        value=json.dumps(dlq_message, ensure_ascii=False).encode("utf-8"),
    )
    producer.flush()

    print("[worker] sent to DLQ:")
    print(json.dumps(dlq_message, indent=2, ensure_ascii=False))


def has_exceeded_retry_limit(job: dict) -> bool:
    current_retry = int(job.get("retry_count", 0))
    return current_retry >= MAX_RETRY_COUNT


def create_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def compute_content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def extract_title_tag(html: str) -> Optional[str]:
    match = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return unescape(match.group(1)).strip()


def extract_meta_content(
    html: str,
    name: Optional[str] = None,
    property_name: Optional[str] = None,
) -> Optional[str]:
    if name:
        pattern = rf'<meta[^>]+name=["\']{re.escape(name)}["\'][^>]+content=["\'](.*?)["\']'
    elif property_name:
        pattern = rf'<meta[^>]+property=["\']{re.escape(property_name)}["\'][^>]+content=["\'](.*?)["\']'
    else:
        return None

    match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return unescape(match.group(1)).strip()


def extract_json_ld_objects(html: str) -> List[Dict]:
    matches = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.IGNORECASE | re.DOTALL,
    )

    objects = []
    for raw in matches:
        raw = raw.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict):
                        objects.append(item)
            elif isinstance(parsed, dict):
                objects.append(parsed)
        except Exception:
            continue

    return objects


def extract_jobposting_json_ld(html: str) -> Optional[dict]:
    objects = extract_json_ld_objects(html)
    for obj in objects:
        if obj.get("@type") == "JobPosting":
            return obj
    return None


def strip_html_tags(text: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>|</div>|</li>|</h1>|</h2>|</h3>|</h4>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def normalize_whitespace(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def extract_company_from_bracket_title(title: Optional[str]) -> Optional[str]:
    if not title:
        return None
    match = re.match(r"^\[(.*?)\]", title.strip())
    if match:
        return match.group(1).strip()
    return None


def parse_wanted_title_meta(title_text: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not title_text:
        return None, None

    title_text = re.sub(r"\s*\|\s*원티드\s*$", "", title_text).strip()
    title_text = re.sub(r"\s*채용 공고\s*$", "", title_text).strip()

    company = extract_company_from_bracket_title(title_text)
    title = title_text

    if company:
        title = re.sub(rf"^\[{re.escape(company)}\]\s*", "", title).strip()

    return normalize_whitespace(company), normalize_whitespace(title)


def clean_saramin_title(title: Optional[str]) -> Optional[str]:
    if not title:
        return None

    title = re.sub(r"\s*-\s*사람인\s*$", "", title).strip()
    title = re.sub(r"\(D-\d+\)\s*$", "", title).strip()

    match = re.match(r"^\[(.*?)\]\s*(.*)$", title)
    if match:
        remainder = match.group(2).strip()
        return remainder or title

    return title


def parse_saramin_meta_description(desc: Optional[str]) -> dict:
    result = {
        "company_name": None,
        "title": None,
        "experience_level": None,
        "location": None,
        "description_text": None,
    }

    if not desc:
        return result

    desc = normalize_whitespace(desc)
    if not desc:
        return result

    parts = [p.strip() for p in desc.split(",") if p.strip()]
    if len(parts) >= 1:
        result["company_name"] = parts[0]
    if len(parts) >= 2:
        result["title"] = parts[1]

    exp_match = re.search(r"경력:\s*([^,]+)", desc)
    if exp_match:
        result["experience_level"] = exp_match.group(1).strip()

    result["description_text"] = desc
    return result


def extract_canonical_url(html: str) -> Optional[str]:
    match = re.search(
        r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\'](.*?)["\']',
        html,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    return unescape(match.group(1)).strip()


def clean_jobkorea_title(title: Optional[str]) -> Optional[str]:
    if not title:
        return None

    title = re.sub(r"\s*\|\s*잡코리아\s*$", "", title).strip()

    if " 채용 - " in title:
        _, remainder = title.split(" 채용 - ", 1)
    else:
        remainder = title

    return normalize_whitespace(remainder.strip())


def clean_catch_title(title: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not title:
        return None, None

    title = re.sub(r"\s*\|\s*캐치\s*$", "", title).strip()
    company = extract_company_from_bracket_title(title)

    if company:
        title = re.sub(rf"^\[{re.escape(company)}\]\s*", "", title).strip()

    title = re.sub(r"\s*채용\s*$", "", title).strip()
    title = re.sub(r"\s*\(~[^)]*\)\s*$", "", title).strip()

    return normalize_whitespace(company), normalize_whitespace(title)


def extract_wanted_fields(html: str) -> dict:
    result = {
        "company_name": None,
        "title": None,
        "location": None,
        "employment_type": None,
        "experience_level": None,
        "description_text": None,
    }

    jobposting = extract_jobposting_json_ld(html)
    if jobposting:
        result["title"] = normalize_whitespace(jobposting.get("title"))

        org = jobposting.get("hiringOrganization") or {}
        if isinstance(org, dict):
            result["company_name"] = normalize_whitespace(org.get("name"))

        location = None
        job_location = jobposting.get("jobLocation") or {}
        if isinstance(job_location, dict):
            address = job_location.get("address") or {}
            if isinstance(address, dict):
                region = normalize_whitespace(address.get("addressRegion"))
                locality = normalize_whitespace(address.get("addressLocality"))
                if region and locality and region != locality:
                    location = f"{region} {locality}"
                else:
                    location = region or locality
        result["location"] = location

        emp = jobposting.get("employmentType")
        if isinstance(emp, list):
            result["employment_type"] = ", ".join([str(x) for x in emp if x])
        elif emp:
            result["employment_type"] = str(emp)

        exp = jobposting.get("experienceRequirements")
        if isinstance(exp, list):
            result["experience_level"] = ", ".join([str(x) for x in exp if x])
        elif exp:
            result["experience_level"] = str(exp)

        desc = jobposting.get("description")
        if desc:
            result["description_text"] = strip_html_tags(desc)

    if not result["title"] or not result["company_name"]:
        title_tag = extract_title_tag(html)
        company, title = parse_wanted_title_meta(title_tag)
        result["company_name"] = result["company_name"] or company
        result["title"] = result["title"] or title

    if not result["description_text"]:
        meta_desc = extract_meta_content(html, name="description")
        result["description_text"] = normalize_whitespace(meta_desc)

    return result


def extract_groupby_fields(html: str) -> dict:
    result = {
        "company_name": None,
        "title": None,
        "location": None,
        "employment_type": None,
        "experience_level": None,
        "description_text": None,
    }

    jobposting = extract_jobposting_json_ld(html)
    if jobposting:
        result["title"] = normalize_whitespace(jobposting.get("title"))

        org = jobposting.get("hiringOrganization") or {}
        if isinstance(org, dict):
            result["company_name"] = normalize_whitespace(org.get("name"))

        job_location = jobposting.get("jobLocation") or {}
        if isinstance(job_location, dict):
            address = job_location.get("address") or {}
            if isinstance(address, dict):
                region = normalize_whitespace(address.get("addressRegion"))
                locality = normalize_whitespace(address.get("addressLocality"))
                if region and locality and region != locality:
                    result["location"] = f"{region} {locality}"
                else:
                    result["location"] = region or locality

        emp = jobposting.get("employmentType")
        if isinstance(emp, list):
            result["employment_type"] = ", ".join([str(x) for x in emp if x])
        elif emp:
            result["employment_type"] = str(emp)

        desc = jobposting.get("description")
        if desc:
            result["description_text"] = strip_html_tags(desc)

    title_tag = extract_title_tag(html)
    og_title = extract_meta_content(html, property_name="og:title")
    meta_desc = extract_meta_content(html, name="description")

    if not result["title"]:
        result["title"] = normalize_whitespace(
            (og_title or title_tag or "").replace("무관 ", "").strip()
        )

    if not result["experience_level"] and title_tag:
        if title_tag.startswith("무관 "):
            result["experience_level"] = "무관"
        else:
            exp_match = re.search(r"\(([^)]*년 이하|신입|경력무관)\)", title_tag)
            if exp_match:
                result["experience_level"] = exp_match.group(1).strip()

    if not result["description_text"]:
        result["description_text"] = normalize_whitespace(meta_desc)

    return result


def extract_saramin_fields(html: str) -> dict:
    result = {
        "company_name": None,
        "title": None,
        "location": None,
        "employment_type": None,
        "experience_level": None,
        "description_text": None,
    }

    title_tag = extract_title_tag(html)
    og_title = extract_meta_content(html, property_name="og:title")
    meta_desc = extract_meta_content(html, name="description")

    title_source = og_title or title_tag
    if title_source:
        result["company_name"] = extract_company_from_bracket_title(title_source)
        result["title"] = clean_saramin_title(title_source)

    parsed_desc = parse_saramin_meta_description(meta_desc)
    result["company_name"] = result["company_name"] or parsed_desc["company_name"]
    result["title"] = result["title"] or parsed_desc["title"]
    result["experience_level"] = parsed_desc["experience_level"]
    result["description_text"] = parsed_desc["description_text"]

    return result


def extract_catch_fields(html: str) -> dict:
    result = {
        "company_name": None,
        "title": None,
        "location": None,
        "employment_type": None,
        "experience_level": None,
        "description_text": None,
    }

    jobposting = extract_jobposting_json_ld(html)
    if jobposting:
        result["title"] = normalize_whitespace(jobposting.get("title"))

        org = jobposting.get("hiringOrganization") or {}
        if isinstance(org, dict):
            result["company_name"] = normalize_whitespace(org.get("name"))

        job_location = jobposting.get("jobLocation")
        if isinstance(job_location, list) and job_location:
            first_location = job_location[0]
            if isinstance(first_location, dict):
                address = first_location.get("address") or {}
                if isinstance(address, dict):
                    region = normalize_whitespace(address.get("addressRegion"))
                    locality = normalize_whitespace(address.get("addressLocality"))
                    if region and locality and region != locality:
                        result["location"] = f"{region} {locality}"
                    else:
                        result["location"] = region or locality

        emp = jobposting.get("employmentType")
        if isinstance(emp, list):
            result["employment_type"] = ", ".join([str(x) for x in emp if x])
        elif emp:
            result["employment_type"] = str(emp)

        exp = jobposting.get("experienceRequirements")
        if isinstance(exp, list):
            result["experience_level"] = ", ".join([str(x) for x in exp if x])
        elif exp:
            result["experience_level"] = str(exp)

        desc = jobposting.get("description")
        if desc:
            result["description_text"] = normalize_whitespace(desc)

    title_tag = extract_title_tag(html)
    og_title = extract_meta_content(html, property_name="og:title")
    meta_desc = extract_meta_content(html, name="description")

    if not result["company_name"] or not result["title"]:
        company, title = clean_catch_title(og_title or title_tag)
        result["company_name"] = result["company_name"] or company
        result["title"] = result["title"] or title

    if result["company_name"] and result["title"]:
        result["title"] = re.sub(
            rf"^\[{re.escape(result['company_name'])}\]\s*",
            "",
            result["title"],
        ).strip()

    if meta_desc:
        meta_desc = normalize_whitespace(meta_desc)
        parts = [p.strip() for p in meta_desc.split("|") if p.strip()]

        if not result["experience_level"] and len(parts) >= 1:
            result["experience_level"] = parts[0]

        if not result["employment_type"] and len(parts) >= 2:
            result["employment_type"] = parts[1]

        if not result["location"] and len(parts) >= 5:
            result["location"] = parts[4]

        if not result["description_text"]:
            result["description_text"] = meta_desc

    return result


def extract_jobkorea_fields(html: str) -> dict:
    result = {
        "company_name": None,
        "title": None,
        "location": None,
        "employment_type": None,
        "experience_level": None,
        "description_text": None,
    }

    jobposting = extract_jobposting_json_ld(html)
    if jobposting:
        result["title"] = normalize_whitespace(jobposting.get("title"))

        org = jobposting.get("hiringOrganization") or {}
        if isinstance(org, dict):
            result["company_name"] = normalize_whitespace(org.get("name"))

        job_location = jobposting.get("jobLocation") or {}
        if isinstance(job_location, dict):
            address = job_location.get("address") or {}
            if isinstance(address, dict):
                locality = normalize_whitespace(address.get("addressLocality"))
                region = normalize_whitespace(address.get("addressRegion"))
                if region and locality and region != locality:
                    result["location"] = f"{region} {locality}"
                else:
                    result["location"] = locality or region

        exp = jobposting.get("experienceRequirements")
        if isinstance(exp, list):
            result["experience_level"] = ", ".join([str(x) for x in exp if x])
        elif exp:
            result["experience_level"] = str(exp)

        desc = jobposting.get("description")
        if desc:
            result["description_text"] = normalize_whitespace(desc)

    title_tag = extract_title_tag(html)
    og_title = extract_meta_content(html, property_name="og:title")
    meta_desc = extract_meta_content(html, name="description")

    if not result["company_name"] or not result["title"]:
        title_source = og_title or title_tag
        cleaned = clean_jobkorea_title(title_source)

        if cleaned:
            match = re.match(r"^(.*?)\s+(\[.*\].*)$", cleaned)
            if match:
                result["company_name"] = result["company_name"] or normalize_whitespace(match.group(1))
                result["title"] = result["title"] or normalize_whitespace(match.group(2))
            else:
                result["title"] = result["title"] or cleaned

    if result["company_name"] and result["title"]:
        result["title"] = re.sub(
            rf"^{re.escape(result['company_name'])}\s*",
            "",
            result["title"],
        ).strip()

    if meta_desc and not result["description_text"]:
        result["description_text"] = normalize_whitespace(meta_desc)

    if meta_desc and not result["experience_level"]:
        exp_match = re.search(r"경력\s*:\s*([^,]+)", meta_desc)
        if exp_match:
            result["experience_level"] = normalize_whitespace(exp_match.group(1))

    return result


def extract_fields_by_domain(url: str, html: str) -> dict:
    hostname = urlparse(url).netloc.lower()

    if "wanted.co.kr" in hostname:
        return extract_wanted_fields(html)
    if "saramin.co.kr" in hostname:
        return extract_saramin_fields(html)
    if "groupby.kr" in hostname:
        return extract_groupby_fields(html)
    if "jobkorea.co.kr" in hostname:
        return extract_jobkorea_fields(html)
    if "catch.co.kr" in hostname:
        return extract_catch_fields(html)

    title_tag = extract_title_tag(html)
    og_title = extract_meta_content(html, property_name="og:title")
    meta_desc = extract_meta_content(html, name="description")

    return {
        "company_name": None,
        "title": normalize_whitespace(og_title or title_tag),
        "location": None,
        "employment_type": None,
        "experience_level": None,
        "description_text": normalize_whitespace(meta_desc) or strip_html_tags(html)[:2000],
    }


def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )


def build_processed_document(job: dict, s3_paths: dict, html: str) -> dict:
    preview = html[:500]

    return {
        "job_id": job["job_id"],
        "source": job["source"],
        "url": job["url"],
        "collected_at": job["collected_at"],
        "raw_s3_key": s3_paths["raw_s3_key"],
        "processed_at": datetime.utcnow().isoformat() + "Z",
        "html_length": len(html),
        "text_preview": preview,
    }


def refetch_saramin_with_canonical(url: str, html: str, timeout: int = 10) -> str:
    hostname = urlparse(url).netloc.lower()
    if "saramin.co.kr" not in hostname:
        return html

    canonical_url = extract_canonical_url(html)
    if not canonical_url:
        return html

    if canonical_url == url:
        return html

    try:
        response = requests.get(
            canonical_url,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; LeadInsightBot/0.1)"},
            verify=False,
        )
        response.raise_for_status()
        print(f"[worker] refetched saramin canonical url: {canonical_url}")
        return response.text
    except Exception as e:
        print(f"[worker] failed to refetch saramin canonical url: {e}")
        return html


def build_curated_document(job: dict, s3_paths: dict, html: str) -> dict:
    extracted = extract_fields_by_domain(job["url"], html)

    description_text = extracted.get("description_text") or strip_html_tags(html)[:2000]
    description_text = normalize_whitespace(description_text) or ""
    description_text = description_text[:4000]

    content_hash = compute_content_hash(description_text)

    return {
        "posting_id": job["job_id"],
        "source": job["source"],
        "original_url": job["url"],
        "company_name": extracted.get("company_name"),
        "title": extracted.get("title"),
        "location": extracted.get("location"),
        "employment_type": extracted.get("employment_type"),
        "experience_level": extracted.get("experience_level"),
        "description_text": description_text,
        "skills": json.dumps([], ensure_ascii=False),
        "collected_at": job["collected_at"],
        "content_hash": content_hash,
        "raw_s3_key": s3_paths["raw_s3_key"],
        "processed_s3_key": s3_paths["processed_s3_key"],
        "curated_s3_key": s3_paths["curated_s3_key"],
    }


def fetch_html(url: str, timeout: int = 10) -> str:
    response = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0 (compatible; LeadInsightBot/0.1)"},
        verify=False,
    )
    response.raise_for_status()
    return response.text


def build_s3_paths(job: dict) -> dict:
    collected_at = job["collected_at"]
    dt = datetime.fromisoformat(collected_at.replace("Z", "+00:00")).date().isoformat()

    source = job["source"]
    job_id = job["job_id"]

    return {
        "raw_s3_key": f"raw/job_postings/source={source}/dt={dt}/{job_id}.html",
        "processed_s3_key": f"processed/job_postings/source={source}/dt={dt}/{job_id}.json",
        "curated_s3_key": f"curated/job_postings/dt={dt}/{job_id}.json",
    }


def main():
    print(f"[worker] kafka_bootstrap={KAFKA_BOOTSTRAP}, topic={FETCH_TOPIC}, group_id={GROUP_ID}")

    consumer = create_consumer()
    producer = create_producer()
    s3_client = create_s3_client()
    consumer.subscribe([FETCH_TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"[worker] consume error: {msg.error()}")
                continue

            raw_value = msg.value().decode("utf-8")
            job = json.loads(raw_value)

            print("[worker] received job:")
            print(json.dumps(job, indent=2, ensure_ascii=False))

            if has_exceeded_retry_limit(job):
                print("[worker] retry limit exceeded. skip")
                consumer.commit(message=msg, asynchronous=False)
                continue


            s3_paths = build_s3_paths(job)

            print("[worker] computed S3 paths:")
            print(json.dumps(s3_paths, indent=2, ensure_ascii=False))


            try:
                html = fetch_html(job["url"])
                html = refetch_saramin_with_canonical(job["url"], html)

                print(f"[worker] fetched html length={len(html)}")
            except Exception as e:
                print(f"[worker] fetch failed")
                send_to_dlq(producer, job, e, "fetch")

                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                upload_raw_html_to_s3(s3_client, S3_BUCKET, s3_paths["raw_s3_key"], html)
                print(f"[worker] uploaded raw html to s3://{S3_BUCKET}/{s3_paths['raw_s3_key']}")
            
            except Exception as e:
                print("[worker] raw upload failed")
                send_to_dlq(producer, job, e, "raw_upload")

                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                processed_doc = build_processed_document(job, s3_paths, html)
                upload_json_to_s3(s3_client, S3_BUCKET, s3_paths["processed_s3_key"], processed_doc)
            except Exception as e:
                print("[worker] processed failed")
                send_to_dlq(producer, job, e, "processed")

                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                curated_doc = build_curated_document(job, s3_paths, html)
                upload_json_to_s3(s3_client, S3_BUCKET, s3_paths["curated_s3_key"], curated_doc)
            except Exception as e:
                print("[worker] curated upload failed")

                consumer.commit(message=msg, asynchronous=False)
                send_to_dlq(producer, job, e, "curated")
                continue
            

            print("[worker] job processed successfully")

            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()