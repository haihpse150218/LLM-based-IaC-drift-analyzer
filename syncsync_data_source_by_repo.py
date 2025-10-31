import boto3
import time

bedrock = boto3.client("bedrock-agent")

KNOWLEDGE_BASE_ID = "kb-xxxxxxxxxxxxxxxxxxxx"

def sync_data_source_by_repo(s3_repo_path: str):
   """
   Sync hoặc tạo mới Data Source cho từng repo trong Knowledge Base
   :param s3_repo_path: vd: 's3://drift-iac-kb/repoA/'
   """
   # Tách tên repo từ path
   repo_name = s3_repo_path.rstrip("/").split("/")[-1]

   print(f"🔍 Checking data source for repo: {repo_name}")

   # 1️⃣ Lấy danh sách data source hiện có
   existing_sources = bedrock.list_data_sources(knowledgeBaseId=KNOWLEDGE_BASE_ID)
   ds = next(
       (d for d in existing_sources["dataSourceSummaries"]
        if d["name"] == repo_name),
       None
   )

   # 2️⃣ Nếu chưa có → tạo mới Data Source
   if not ds:
       print(f"🆕 Creating new data source for {repo_name}")
       ds = bedrock.create_data_source(
           name=repo_name,
           knowledgeBaseId=KNOWLEDGE_BASE_ID,
           dataSourceConfiguration={
               "s3Configuration": {
                   "bucketArn": s3_repo_path.replace("s3://", "arn:aws:s3:::"),
                   "inclusionPrefixes": [f"{repo_name}/"]
               }
           },
           description=f"Data source for {repo_name}",
           dataDeletionPolicy="DELETE"  # có thể đổi thành RETAIN nếu muốn giữ khi xóa file
       )["dataSource"]

       data_source_id = ds["dataSourceId"]
       print(f"✅ Created new data source: {data_source_id}")

   else:
       data_source_id = ds["dataSourceId"]
       print(f"♻️ Found existing data source: {data_source_id}")

   # 3️⃣ Bắt đầu sync (ingestion job)
   print(f"🚀 Starting ingestion job for {repo_name}...")
   job = bedrock.start_ingestion_job(
       knowledgeBaseId=KNOWLEDGE_BASE_ID,
       dataSourceId=data_source_id
   )

   job_id = job["ingestionJob"]["ingestionJobId"]
   print(f"⏳ Ingestion Job started: {job_id}")

   # 4️⃣ Theo dõi tiến trình
   while True:
       status = bedrock.get_ingestion_job(
           knowledgeBaseId=KNOWLEDGE_BASE_ID,
           dataSourceId=data_source_id,
           ingestionJobId=job_id
       )["ingestionJob"]["status"]

       if status in ["COMPLETE", "FAILED"]:
           print(f"🏁 Job {job_id} finished with status: {status}")
           break

       print("   ...still running, wait 10s")
       time.sleep(10)
  

 