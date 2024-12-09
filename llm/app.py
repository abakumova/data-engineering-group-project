from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import duckdb
import openai
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
import os

# Correct file paths
BASE_DIR = r""
PARQUET_FILE = os.path.join(BASE_DIR, "unified_health_model_20241208170934.parquet")
INDEX_FILE = os.path.join(BASE_DIR, "index.html")

# DuckDB setup
duckdb_conn = duckdb.connect()
try:
    duckdb_conn.execute(f"CREATE VIEW data_view AS SELECT * FROM read_parquet('{PARQUET_FILE}');")
except duckdb.IOException as e:
    raise RuntimeError(f"Error loading Parquet file: {e}")

# OpenAI setup
openai.api_key = ""
llm = ChatOpenAI(model="gpt-4o-mini", api_key=openai.api_key)

schema = duckdb_conn.execute("DESCRIBE data_view").fetchdf()

prompt_template = """
You are an assistant that helps to query a database and. 
The table schema is as follows:
{schema}
Table name / FROM is: data_view

Generate an SQL query for the following question:
{query}
"""
prompt = PromptTemplate(
    input_variables=["schema", "query"],
    template=prompt_template,
)

llm_chain = LLMChain(llm=llm, prompt=prompt)

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins; restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def extract_sql_query(response: str) -> str:
    """
    Extract the SQL query from the LLM's response.
    Assumes the query is enclosed in backticks (```sql ... ```).
    """
    try:
        # Look for the SQL query enclosed in ```sql ... ```
        start_idx = response.find("```sql")
        end_idx = response.find("```", start_idx + 1)

        if start_idx != -1 and end_idx != -1:
            return response[start_idx + len("```sql"):end_idx].strip()

        # If no backticks are found, return the full response as-is
        return response.strip()
    except Exception as e:
        raise ValueError(f"Unable to extract SQL query from response: {response}")


class QueryRequest(BaseModel):
    query: str


@app.get("/")
def serve_index():
    return FileResponse(INDEX_FILE)


@app.get("/schema")
def get_schema():
    try:
        schema = duckdb_conn.execute("DESCRIBE data_view").fetchdf().to_dict(orient="records")
        return {"schema": schema}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching schema: {e}")

@app.post("/query")
def run_query(request: QueryRequest):
    try:
        # Fetch the schema dynamically for each query
        schema_str = duckdb_conn.execute("DESCRIBE data_view").fetchdf().to_string(index=False)

        # Generate SQL query using LLM
        response = llm_chain.run({"schema": schema_str, "query": request.query})

        # Extract the SQL query (remove explanations if present)
        sql_query = extract_sql_query(response)

        # Log the generated query and the LLM response
        print(f"LLM Response: {response}")
        print(f"Generated SQL Query: {sql_query}")

        # Execute SQL query on DuckDB
        result = duckdb_conn.execute(sql_query).fetchdf().to_dict(orient="records")
        print(f"Query Result: {result}")  # Log the result

        # Return both the LLM response and the query result
        return {"llm_response": response, "query": sql_query, "result": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error executing query: {e}")

